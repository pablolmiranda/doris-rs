//! Running one fragment instance: its operator tree, bottom up, then its sink. Each node yields
//! its whole output as one chunk; every node then applies its conjuncts, its projections (into
//! its output tuple) and its limit, as the C++ operators do after their own work.
use doris_segment::writer::Datum;
use doris_storage::reader::{ColTypeRef, ScanColumn};
use doris_storage::StorageError;
use doris_thrift::gen::data_sinks::TDataSinkType;
use doris_thrift::gen::exprs::TExpr;
use doris_thrift::gen::plan_nodes::TPlanNodeType as P;

use crate::block::Block;
use crate::eval::{coerce, eval_column, filter, SlotMap};
use crate::exchange::{Chunk, ExchangeKey, Exchanges};
use crate::query::{FragmentPlan, InstancePlan, PlanTree, QueryPlan};
use crate::ExecError;

/// Segments of rows, as `doris_storage::reader::scan_tablet` returns them.
pub type ScanRows = Vec<Vec<Vec<Datum>>>;

/// Reads `(tablet id, version)` as these columns.
pub type ScanFn<'a> = dyn Fn(i64, i64, &[ScanColumn]) -> Result<ScanRows, StorageError> + Sync + 'a;
/// Takes the result sink's rows.
pub type ResultFn<'a> = dyn Fn(Block) -> Result<(), ExecError> + Sync + 'a;

/// What an instance reads from and writes to.
pub struct ExecCtx<'a> {
    pub scan: &'a ScanFn<'a>,
    pub exchanges: &'a Exchanges,
    /// Takes the result sink's rows (output expressions evaluated).
    pub result: &'a ResultFn<'a>,
}

struct Instance<'a> {
    plan: &'a QueryPlan,
    fragment: &'a FragmentPlan,
    instance: &'a InstancePlan,
    ctx: &'a ExecCtx<'a>,
}

fn qid(plan: &QueryPlan) -> (i64, i64) {
    (plan.query_id.hi, plan.query_id.lo)
}

pub fn slot_map(slots: &[i32]) -> SlotMap {
    SlotMap(slots.iter().enumerate().map(|(i, s)| (*s, i)).collect())
}

/// Evaluates expressions over a chunk into a block of their values.
pub fn project(exprs: &[TExpr], input: &Chunk) -> Result<Block, ExecError> {
    let slots = slot_map(&input.slots);
    let mut types = Vec::with_capacity(exprs.len());
    let mut cols = Vec::with_capacity(exprs.len());
    for e in exprs {
        let (t, v) = eval_column(e, &input.block, &slots)?;
        types.push(t);
        cols.push(v);
    }
    let rows = (0..input.block.len()).map(|r| cols.iter().map(|c| c[r].clone()).collect()).collect();
    Ok(Block { types, rows })
}

/// Runs one instance of one fragment of the query to its end.
pub fn run_instance(plan: &QueryPlan, fragment: usize, instance: usize, ctx: &ExecCtx) -> Result<(), ExecError> {
    let f = &plan.fragments[fragment];
    let me = Instance {
        plan,
        fragment: f,
        instance: &f.instances[instance],
        ctx,
    };
    let out = me.node(&f.root)?;
    me.sink(out)
}

impl Instance<'_> {
    fn node(&self, t: &PlanTree) -> Result<Chunk, ExecError> {
        let n = &t.node;
        let desc = &self.plan.desc;
        let mut out = match n.node_type {
            P::OLAP_SCAN_NODE => self.scan(t)?,
            P::EMPTY_SET_NODE => {
                let tuple = *n
                    .row_tuples
                    .first()
                    .ok_or_else(|| ExecError::BadRequest("EMPTY_SET_NODE without its tuple".into()))?;
                self.empty(tuple)?
            }
            P::SELECT_NODE => self.child(t, 0)?,
            P::UNION_NODE => self.constants(t)?,
            P::EXCHANGE_NODE => self.exchange(t)?,
            _ => {
                return Err(ExecError::Unsupported {
                    what: crate::query::node_type_name(n.node_type),
                })
            }
        };
        if let Some(c) = n.conjuncts.as_ref().filter(|c| !c.is_empty()) {
            out.block = filter(c, out.block, &slot_map(&out.slots))?;
        }
        if let (Some(p), Some(tuple)) = (n.projections.as_ref(), n.output_tuple_id) {
            let block = project(p, &out)?;
            out = Chunk {
                block: self.typed(block, desc.tuple_slots(tuple)?)?,
                slots: desc.tuple_slots(tuple)?.to_vec(),
            };
        }
        if n.limit >= 0 {
            out.block.rows.truncate(n.limit as usize);
        }
        Ok(out)
    }

    fn child(&self, t: &PlanTree, i: usize) -> Result<Chunk, ExecError> {
        let c = t
            .children
            .get(i)
            .ok_or_else(|| ExecError::BadRequest(format!("node {} has no child {i}", t.node.node_id)))?;
        self.node(c)
    }

    fn empty(&self, tuple: i32) -> Result<Chunk, ExecError> {
        let slots = self.plan.desc.tuple_slots(tuple)?.to_vec();
        let types = slots
            .iter()
            .map(|s| self.plan.desc.slot(*s).map(|s| s.ty.clone()))
            .collect::<Result<_, _>>()?;
        Ok(Chunk {
            block: Block::new(types),
            slots,
        })
    }

    /// Values in the types of a tuple's slots.
    fn typed(&self, block: Block, slots: &[i32]) -> Result<Block, ExecError> {
        let to: Vec<_> = slots
            .iter()
            .map(|s| self.plan.desc.slot(*s).map(|s| s.ty.clone()))
            .collect::<Result<_, _>>()?;
        if to.len() != block.types.len() {
            return Err(ExecError::BadRequest(format!(
                "{} values for a tuple of {} slots",
                block.types.len(),
                to.len()
            )));
        }
        let Block { types, rows } = block;
        let rows = rows
            .into_iter()
            .map(|r| {
                r.into_iter()
                    .zip(types.iter().zip(&to))
                    .map(|(d, (from, to))| coerce(d, from, to))
                    .collect::<Result<Vec<_>, _>>()
            })
            .collect::<Result<_, _>>()?;
        Ok(Block { types: to, rows })
    }

    fn scan(&self, t: &PlanTree) -> Result<Chunk, ExecError> {
        let n = &t.node;
        let s = n
            .olap_scan_node
            .as_ref()
            .ok_or_else(|| ExecError::BadRequest("OLAP_SCAN_NODE without its parameters".into()))?;
        let desc = &self.plan.desc;
        let slots = desc.tuple_slots(s.tuple_id)?.to_vec();
        let mut columns = Vec::with_capacity(slots.len());
        let mut types = Vec::with_capacity(slots.len());
        for id in &slots {
            let slot = desc.slot(*id)?;
            let uid = if slot.unique_id >= 0 {
                slot.unique_id
            } else {
                s.columns_desc
                    .iter()
                    .flatten()
                    .find(|c| c.column_name.eq_ignore_ascii_case(&slot.name))
                    .and_then(|c| c.col_unique_id)
                    .ok_or_else(|| ExecError::BadRequest(format!("slot {} ({}) has no column", slot.id, slot.name)))?
            };
            columns.push(ScanColumn {
                unique_id: u32::try_from(uid).map_err(|_| ExecError::BadRequest(format!("column unique id {uid}")))?,
                ty: ColTypeRef {
                    ptype: slot.ty.ptype,
                    precision: slot.ty.precision,
                    scale: slot.ty.scale,
                },
            });
            types.push(slot.ty.clone());
        }
        let mut rows = Vec::new();
        for (tablet, version) in self.instance.scan_ranges.get(&n.node_id).into_iter().flatten() {
            for segment in (self.ctx.scan)(*tablet, *version, &columns)? {
                rows.extend(segment);
            }
        }
        Ok(Chunk {
            block: Block { types, rows },
            slots,
        })
    }

    fn constants(&self, t: &PlanTree) -> Result<Chunk, ExecError> {
        let u = t
            .node
            .union_node
            .as_ref()
            .ok_or_else(|| ExecError::BadRequest("UNION_NODE without its parameters".into()))?;
        let mut out = self.empty(u.tuple_id)?;
        let one = Chunk {
            block: Block {
                types: vec![],
                rows: vec![vec![]],
            },
            slots: vec![],
        };
        for row in &u.const_expr_lists {
            let b = self.typed(project(row, &one)?, &out.slots)?;
            out.block.rows.extend(b.rows);
        }
        Ok(out)
    }

    fn exchange(&self, t: &PlanTree) -> Result<Chunk, ExecError> {
        let n = &t.node;
        let e = n
            .exchange_node
            .as_ref()
            .ok_or_else(|| ExecError::BadRequest("EXCHANGE_NODE without its parameters".into()))?;
        let senders = self.fragment.per_exch_num_senders.get(&n.node_id).copied().unwrap_or(0).max(0) as usize;
        let key = ExchangeKey {
            query: qid(self.plan),
            instance: (self.instance.id.hi, self.instance.id.lo),
            node: n.node_id,
        };
        let chunks = self.ctx.exchanges.receive(&key, senders)?;
        let tuple = *e
            .input_row_tuples
            .first()
            .ok_or_else(|| ExecError::BadRequest("EXCHANGE_NODE without its tuple".into()))?;
        let mut out = self.empty(tuple)?;
        for c in chunks {
            if c.slots != out.slots {
                out.slots = c.slots;
                out.block.types = c.block.types;
            }
            out.block.rows.extend(c.block.rows);
        }
        let offset = e.offset.unwrap_or(0).max(0) as usize;
        out.block.rows.drain(..offset.min(out.block.rows.len()));
        Ok(out)
    }

    fn sink(&self, out: Chunk) -> Result<(), ExecError> {
        let f = self.fragment;
        match f.sink.type_ {
            TDataSinkType::RESULT_SINK => (self.ctx.result)(project(&f.output_exprs, &out)?),
            TDataSinkType::DATA_STREAM_SINK => {
                let s = f
                    .sink
                    .stream_sink
                    .as_ref()
                    .ok_or_else(|| ExecError::BadRequest("DATA_STREAM_SINK without its parameters".into()))?;
                let mut out = out;
                if let Some(c) = s.conjuncts.as_ref().filter(|c| !c.is_empty()) {
                    out.block = filter(c, out.block, &slot_map(&out.slots))?;
                }
                if let (Some(exprs), Some(tuple)) = (s.output_exprs.as_ref(), s.output_tuple_id) {
                    let slots = self.plan.desc.tuple_slots(tuple)?.to_vec();
                    out = Chunk {
                        block: self.typed(project(exprs, &out)?, &slots)?,
                        slots,
                    };
                }
                for d in &f.destinations {
                    let key = ExchangeKey {
                        query: qid(self.plan),
                        instance: (d.fragment_instance_id.hi, d.fragment_instance_id.lo),
                        node: s.dest_node_id,
                    };
                    self.ctx.exchanges.send(&key, out.clone());
                    self.ctx.exchanges.end(&key);
                }
                Ok(())
            }
            other => Err(ExecError::Unsupported {
                what: crate::query::sink_type_name(other),
            }),
        }
    }
}
