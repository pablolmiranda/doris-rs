//! Running one fragment instance: its operator tree, bottom up, then its sink. Each node yields
//! its whole output as one chunk; every node then applies its conjuncts, its projections (into
//! its output tuple) and its limit, as the C++ operators do after their own work.
use doris_segment::writer::Datum;
use doris_storage::reader::{ColTypeRef, ScanColumn};
use doris_storage::StorageError;
use doris_thrift::gen::data_sinks::{TDataSinkType, TDataStreamSink, TMultiCastDataStreamSink, TPlanFragmentDestination};
use doris_thrift::gen::exprs::TExpr;
use doris_thrift::gen::plan_nodes::TPlanNodeType as P;

use crate::block::Block;
use crate::eval::{coerce, eval_column, filter, SlotMap};
use crate::exchange::{Chunk, ExchangeKey, Exchanges};
use crate::query::{multicast_consumers, projection_tuple, DescTable, FragmentPlan, InstancePlan, PlanTree, QueryPlan};
use crate::sort::{cmp_rows, keys};
use crate::ExecError;

/// Segments of rows, as `doris_storage::reader::scan_tablet` returns them.
pub type ScanRows = Vec<Vec<Vec<Datum>>>;

/// Reads `(tablet id, version)` as these columns.
pub type ScanFn<'a> = dyn Fn(i64, i64, &[ScanColumn]) -> Result<ScanRows, StorageError> + Sync + 'a;
/// Takes the result sink's rows.
pub type ResultFn<'a> = dyn Fn(Block) -> Result<(), ExecError> + Sync + 'a;
/// Takes a table sink's rows (output expressions evaluated): fragment index, instance index.
pub type TableFn<'a> = dyn Fn(usize, usize, Block) -> Result<(), ExecError> + Sync + 'a;
/// Reads the rows row ids name, as these columns, in the ids' order.
pub type FetchFn<'a> = dyn Fn(&[Vec<u8>], &[ScanColumn]) -> Result<Vec<Vec<Datum>>, StorageError> + Sync + 'a;

/// The row id slot of a scan (`__DORIS_GLOBAL_ROWID_COL__<table>`, unique id `i32::MAX`).
pub const ROWID_PREFIX: &str = "__DORIS_GLOBAL_ROWID_COL__";

/// What an instance reads from and writes to.
pub struct ExecCtx<'a> {
    pub scan: &'a ScanFn<'a>,
    pub exchanges: &'a Exchanges,
    /// Takes the result sink's rows (output expressions evaluated).
    pub result: &'a ResultFn<'a>,
    pub fetch: &'a FetchFn<'a>,
    /// The FE's metadata calls, for information_schema scans.
    pub fe: &'a dyn crate::schema_scan::SchemaFe,
    /// Takes an OLAP_TABLE_SINK's rows (`INSERT … SELECT`).
    pub table: &'a TableFn<'a>,
}

struct Instance<'a> {
    plan: &'a QueryPlan,
    /// This instance's fragment and its own index in it.
    at: (usize, usize),
    fragment: &'a FragmentPlan,
    instance: &'a InstancePlan,
    ctx: &'a ExecCtx<'a>,
    /// Exchange node → its chunks, received before the tree runs (shared broadcast builds).
    prefetched: std::cell::RefCell<std::collections::HashMap<i32, Vec<Chunk>>>,
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
    // a merged fragment (`RunMode::Merged`): the first instance scans every instance's ranges
    let merged;
    let me_plan = if f.mode == crate::query::RunMode::Merged && instance == 0 {
        let mut all = f.instances[0].clone();
        all.scan_ranges.clear();
        all.data_gen.clear();
        for i in &f.instances {
            for (node, ranges) in &i.scan_ranges {
                all.scan_ranges.entry(*node).or_default().extend(ranges.iter().cloned());
            }
            for (node, ranges) in &i.data_gen {
                all.data_gen.entry(*node).or_default().extend(ranges.iter().copied());
            }
        }
        merged = all;
        &merged
    } else {
        &f.instances[instance]
    };
    let me = Instance {
        plan,
        at: (fragment, instance),
        fragment: f,
        instance: me_plan,
        ctx,
        prefetched: Default::default(),
    };
    // one instance: its own random generators (random.cpp's per-instance mt19937_64)
    crate::clock::with_clock(&plan.clock, || crate::regexp::with_regex_options(plan.regex, || random_scope(f.mode == crate::query::RunMode::Merged && f.instances.len() > 1, || {
        if f.mode == crate::query::RunMode::Merged && instance > 0 {
            return me.idle();
        }
        me.prefetch_shared_builds()?;
        let out = me.node(&f.root)?;
        me.sink(out)
    })))
}

/// The instance's random generators: shared when one run stands for several C++ tasks.
fn random_scope<R>(merged: bool, f: impl FnOnce() -> R) -> R {
    if merged {
        crate::random::with_shared_scope(f)
    } else {
        crate::random::with_scope(f)
    }
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
            P::INTERSECT_NODE | P::EXCEPT_NODE => self.set_op(t)?,
            P::HASH_JOIN_NODE => self.hash_join(t)?,
            P::CROSS_JOIN_NODE => self.cross_join(t)?,
            P::ASSERT_NUM_ROWS_NODE => self.assert_num_rows(t)?,
            P::EXCHANGE_NODE => self.exchange(t)?,
            P::SORT_NODE => self.sort(t)?,
            P::AGGREGATION_NODE => self.aggregate(t)?,
            P::TABLE_FUNCTION_NODE => self.table_function(t)?,
            P::ANALYTIC_EVAL_NODE => self.analytic(t)?,
            P::PARTITION_SORT_NODE => self.partition_sort(t)?,
            P::SCHEMA_SCAN_NODE => self.schema_scan(t)?,
            P::REPEAT_NODE => self.repeat(t)?,
            P::DATA_GEN_SCAN_NODE => self.data_gen(t)?,
            P::MATERIALIZATION_NODE => self.materialize(t)?,
            _ => {
                return Err(ExecError::Unsupported {
                    what: crate::query::node_type_name(n.node_type),
                })
            }
        };
        if let Some(c) = n.conjuncts.as_ref().filter(|c| !c.is_empty()) {
            out.block = filter(c, out.block, &slot_map(&out.slots))?;
        }
        // intermediate projections first, each into its own tuple (`OperatorXBase::do_projections`)
        let stages = n.intermediate_projections_list.iter().flatten().zip(n.intermediate_output_tuple_id_list.iter().flatten());
        for (p, tuple) in stages {
            let block = project(p, &out)?;
            out = Chunk {
                block: self.typed(block, desc.tuple_slots(*tuple)?)?,
                slots: desc.tuple_slots(*tuple)?.to_vec(),
            };
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
        typed(&self.plan.desc, block, slots)
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
        let mut rowid = None;
        // virtual slots: (position, expression), computed once the real columns are read
        let mut virtuals = Vec::new();
        for (i, id) in slots.iter().enumerate() {
            let slot = desc.slot(*id)?;
            types.push(slot.ty.clone());
            if let Some(e) = &slot.virtual_expr {
                virtuals.push((i, e));
                continue;
            }
            if slot.name.starts_with(ROWID_PREFIX) || slot.unique_id == i32::MAX {
                rowid = Some(i);
                continue;
            }
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
        }
        let mut rows = Vec::new();
        for (tablet, version) in self.instance.scan_ranges.get(&n.node_id).into_iter().flatten() {
            for (si, segment) in (self.ctx.scan)(*tablet, *version, &columns)?.into_iter().enumerate() {
                match rowid {
                    None => rows.extend(segment),
                    Some(at) => rows.extend(segment.into_iter().enumerate().map(|(ordinal, mut r)| {
                        r.insert(at, Datum::Bytes(doris_storage::reader::row_id(*tablet, *version, si as u32, ordinal as u32)));
                        r
                    })),
                }
            }
        }
        if virtuals.is_empty() {
            return Ok(Chunk { block: Block { types, rows }, slots });
        }
        // the real slots' block (the row id among them), then each virtual slot evaluated over it
        let real: Vec<usize> = (0..slots.len()).filter(|i| !virtuals.iter().any(|(v, _)| v == i)).collect();
        let read = Block { types: real.iter().map(|i| types[*i].clone()).collect(), rows };
        let read_slots = slot_map(&real.iter().map(|i| slots[*i]).collect::<Vec<_>>());
        let mut computed = Vec::with_capacity(virtuals.len());
        for (i, e) in &virtuals {
            let (t, v) = eval_column(e, &read, &read_slots)?;
            let v = v.into_iter().map(|d| coerce(d, &t, &types[*i])).collect::<Result<Vec<_>, _>>()?;
            computed.push((*i, v));
        }
        let rows = read
            .rows
            .into_iter()
            .enumerate()
            .map(|(r, row)| {
                let mut out = Vec::with_capacity(slots.len());
                let mut real_values = row.into_iter();
                for i in 0..slots.len() {
                    match computed.iter().find(|(v, _)| *v == i) {
                        Some((_, col)) => out.push(col[r].clone()),
                        None => out.push(real_values.next().unwrap_or(Datum::Null)),
                    }
                }
                out
            })
            .collect();
        Ok(Chunk { block: Block { types, rows }, slots })
    }

    fn constants(&self, t: &PlanTree) -> Result<Chunk, ExecError> {
        let u = t
            .node
            .union_node
            .as_ref()
            .ok_or_else(|| ExecError::BadRequest("UNION_NODE without its parameters".into()))?;
        let mut out = self.empty(u.tuple_id)?;
        // children: the first ones pass through as they are, the rest through their result exprs
        for (i, c) in t.children.iter().enumerate() {
            let child = self.node(c)?;
            let rows = if (i as i64) < u.first_materialized_child_idx || u.result_expr_lists.get(i).is_none_or(|l| l.is_empty()) {
                self.relabel(child, u.tuple_id)?.block.rows
            } else {
                self.typed(project(&u.result_expr_lists[i], &child)?, &out.slots)?.rows
            };
            out.block.rows.extend(rows);
        }
        let one = Chunk {
            block: Block {
                types: vec![],
                rows: vec![vec![]],
            },
            slots: vec![],
        };
        // the constant rows: only the fragment's first instance (union_source_operator.cpp,
        // `per_fragment_instance_idx() == 0`, which is the sender id)
        let consts = if self.instance.sender_id == 0 { &u.const_expr_lists[..] } else { &[] };
        for row in consts {
            let b = self.typed(project(row, &one)?, &out.slots)?;
            out.block.rows.extend(b.rows);
        }
        Ok(out)
    }

    /// INTERSECT_NODE / EXCEPT_NODE: each child projected by its result expr list, then the set
    /// operation, typed onto the node's tuple.
    fn set_op(&self, t: &PlanTree) -> Result<Chunk, ExecError> {
        let (kind, tuple, lists) = crate::query::set_node(&t.node)?;
        let mut out = self.empty(tuple)?;
        let mut children = Vec::with_capacity(t.children.len());
        for (c, exprs) in t.children.iter().zip(lists) {
            // every child in the tuple's types, so equal values compare equal
            let child = self.node(c)?;
            children.push(self.typed(project(exprs, &child)?, &out.slots)?.rows);
        }
        out.block.rows = crate::set_op::set_rows(kind, children);
        Ok(out)
    }

    /// The slots of a join's intermediate tuples, in order.
    fn intermediate(&self, tuples: Option<&Vec<i32>>, name: &str) -> Result<Vec<i32>, ExecError> {
        let tuples = tuples.filter(|t| !t.is_empty()).ok_or_else(|| ExecError::BadRequest(format!("a {name} without its intermediate tuples")))?;
        let mut slots = Vec::new();
        for t in tuples {
            slots.extend_from_slice(self.plan.desc.tuple_slots(*t)?);
        }
        Ok(slots)
    }

    /// HASH_JOIN_NODE: the build child (1) first; for the ops C++ short-circuits on an empty build
    /// side the probe child is never run (`hashjoin_build_sink.cpp:318-339`). Then the join, in
    /// the intermediate tuple's layout.
    fn hash_join(&self, t: &PlanTree) -> Result<Chunk, ExecError> {
        use crate::hash_join::{hash_join, join_op, HashJoin, JoinOp, Side};
        let n = &t.node;
        let h = n.hash_join_node.as_ref().ok_or_else(|| ExecError::BadRequest("HASH_JOIN_NODE without its parameters".into()))?;
        let op = join_op(h.join_op).ok_or_else(|| ExecError::Unsupported { what: format!("a HASH_JOIN_NODE {:?}", h.join_op) })?;
        let mark = h.is_mark == Some(true);
        let slots = self.intermediate(h.vintermediate_tuple_id_list.as_ref(), "HASH_JOIN_NODE")?;
        let build = self.child(t, 1)?;
        let short_circuit = matches!(op, JoinOp::Inner | JoinOp::LeftSemi | JoinOp::RightOuter | JoinOp::RightSemi | JoinOp::RightAnti | JoinOp::AsofLeftInner);
        if build.block.is_empty() && short_circuit && !mark {
            return self.chunk_of(Vec::new(), &slots);
        }
        // an ASOF join's value is the last key
        let asof = match (&h.match_condition, matches!(op, JoinOp::AsofLeftInner | JoinOp::AsofLeftOuter)) {
            (Some(m), true) => Some(crate::hash_join::asof_condition(m)?),
            (None, true) => return Err(ExecError::BadRequest("an ASOF join without its match condition".into())),
            _ => None,
        };
        let keys = |c: &Chunk, left: bool| -> Result<Vec<Vec<Datum>>, ExecError> {
            let map = slot_map(&c.slots);
            let mut cols = Vec::with_capacity(h.eq_join_conjuncts.len() + 1);
            for e in &h.eq_join_conjuncts {
                cols.push(eval_column(if left { &e.left } else { &e.right }, &c.block, &map)?.1);
            }
            if let Some((_, (p, b))) = &asof {
                cols.push(eval_column(if left { p } else { b }, &c.block, &map)?.1);
            }
            Ok((0..c.block.len()).map(|r| cols.iter().map(|col| col[r].clone()).collect()).collect())
        };
        let null_safe: Vec<bool> = h.eq_join_conjuncts.iter().map(|e| e.opcode == Some(doris_thrift::gen::opcodes::TExprOpcode::EQ_FOR_NULL)).collect();
        let build_keys = keys(&build, false)?;
        // NULL_AWARE_LEFT_ANTI with a NULL build key leaves no row, and C++ does not run the probe
        // child (`_short_circuit_for_null_in_build_side`, hashjoin_build_sink.cpp:323-331)
        let null_key = |k: &Vec<Datum>| k.iter().zip(null_safe.iter().chain(std::iter::repeat(&false))).any(|(d, safe)| *d == Datum::Null && !safe);
        if op == JoinOp::NullAwareLeftAnti && !mark && build_keys.iter().any(null_key) {
            return self.chunk_of(Vec::new(), &slots);
        }
        let probe = self.child(t, 0)?;
        let (pw, bw) = (probe.slots.len(), build.slots.len());
        let has_other = h.other_join_conjuncts.as_ref().is_some_and(|c| !c.is_empty());
        let has_mark_conj = h.mark_join_conjuncts.as_ref().is_some_and(|c| !c.is_empty());
        let layout = Layout::new(slots.len(), pw, bw, mark, Layout::sides(op, has_other, mark, has_mark_conj))?;
        let conjuncts = |exprs: &[TExpr], rows: &[Vec<Datum>]| self.joined_conjuncts(exprs, rows, &layout, &slots, mark);
        let other_exprs = h.other_join_conjuncts.clone().unwrap_or_default();
        let mark_exprs = h.mark_join_conjuncts.clone().unwrap_or_default();
        let other = |rows: &[Vec<Datum>]| conjuncts(&other_exprs, rows);
        let mark_conj = |rows: &[Vec<Datum>]| conjuncts(&mark_exprs, rows);
        let j = HashJoin {
            op,
            null_safe,
            mark,
            other: (!other_exprs.is_empty()).then_some(&other as &crate::hash_join::Cond),
            mark_conj: (!mark_exprs.is_empty()).then_some(&mark_conj as &crate::hash_join::Cond),
            asof: asof.as_ref().map(|(how, _)| (*how, h.eq_join_conjuncts.len())),
        };
        let probe_side = Side { keys: keys(&probe, true)?, rows: probe.block.rows, width: pw };
        let build_side = Side { keys: build_keys, rows: build.block.rows, width: bw };
        let rows = hash_join(&j, probe_side, build_side)?;
        self.chunk_of(rows.iter().map(|r| layout.apply(r)).collect(), &slots)
    }

    /// A join's conjuncts over joined rows (probe columns, build columns), three-valued; the rows
    /// are laid out as the intermediate tuple, whose mark slot (if any) reads NULL.
    fn joined_conjuncts(&self, exprs: &[TExpr], rows: &[Vec<Datum>], layout: &Layout, slots: &[i32], mark: bool) -> Result<Vec<Option<bool>>, ExecError> {
        let rows = rows
            .iter()
            .map(|r| {
                let mut r = r.clone();
                if mark {
                    r.push(Datum::Null);
                }
                layout.apply(&r)
            })
            .collect();
        let chunk = self.chunk_of(rows, slots)?;
        let map = slot_map(&chunk.slots);
        let mut out = vec![Some(true); chunk.block.len()];
        for e in exprs {
            let (_, v) = eval_column(e, &chunk.block, &map)?;
            for (o, d) in out.iter_mut().zip(&v) {
                *o = and3(*o, truth(d));
            }
        }
        Ok(out)
    }

    /// CROSS_JOIN_NODE: the build child (1), then the probe child, then the nested-loop join over
    /// `join_conjuncts` (or the legacy `vjoin_conjunct`); `mark_join_conjuncts` and
    /// `is_output_left_side_only` are ignored, as C++ ignores them.
    fn cross_join(&self, t: &PlanTree) -> Result<Chunk, ExecError> {
        use crate::hash_join::{join_op, Side};
        let n = &t.node;
        let j = n.nested_loop_join_node.as_ref().ok_or_else(|| ExecError::BadRequest("CROSS_JOIN_NODE without its parameters".into()))?;
        let op = join_op(j.join_op).ok_or_else(|| ExecError::Unsupported { what: format!("a CROSS_JOIN_NODE {:?}", j.join_op) })?;
        let mark = j.is_mark == Some(true);
        let slots = self.intermediate(j.vintermediate_tuple_id_list.as_ref(), "CROSS_JOIN_NODE")?;
        let build = self.child(t, 1)?;
        let probe = self.child(t, 0)?;
        let (pw, bw) = (probe.slots.len(), build.slots.len());
        // the nested-loop join keeps both sides (`nested_loop_join_probe_operator.cpp:407-408`)
        let layout = Layout::new(slots.len(), pw, bw, mark, Layout::sides(op, true, mark, true))?;
        let exprs: Vec<TExpr> = match (&j.join_conjuncts, &j.vjoin_conjunct) {
            (Some(c), _) if !c.is_empty() => c.clone(),
            (_, Some(v)) => vec![v.clone()],
            _ => Vec::new(),
        };
        let cond = |rows: &[Vec<Datum>]| self.joined_conjuncts(&exprs, rows, &layout, &slots, mark);
        let rows = crate::nested_loop::nested_loop(
            op,
            mark,
            (!exprs.is_empty()).then_some(&cond as &crate::hash_join::Cond),
            Side { rows: probe.block.rows, keys: Vec::new(), width: pw },
            Side { rows: build.block.rows, keys: Vec::new(), width: bw },
        )?;
        self.chunk_of(rows.iter().map(|r| layout.apply(r)).collect(), &slots)
    }

    /// ASSERT_NUM_ROWS_NODE: its child's rows, or one all-NULL row (EQ 1 over none); with
    /// `should_convert_output_to_nullable` every column becomes nullable.
    fn assert_num_rows(&self, t: &PlanTree) -> Result<Chunk, ExecError> {
        let n = &t.node;
        let a = n.assert_num_rows_node.as_ref().ok_or_else(|| ExecError::BadRequest("ASSERT_NUM_ROWS_NODE without its parameters".into()))?;
        let child = self.child(t, 0)?;
        // the child's columns, by position, as the node's own tuple's slots
        let mut slots = Vec::new();
        for tuple in &n.row_tuples {
            slots.extend_from_slice(self.plan.desc.tuple_slots(*tuple)?);
        }
        if slots.len() != child.slots.len() {
            return Err(ExecError::BadRequest(format!(
                "an ASSERT_NUM_ROWS_NODE of {} slots over {} columns",
                slots.len(),
                child.slots.len()
            )));
        }
        let mut rows = child.block.rows;
        if crate::nested_loop::assert_rows(n.node_id, a, rows.len())? {
            rows = vec![vec![Datum::Null; slots.len()]];
        }
        let mut out = self.chunk_of(rows, &slots)?;
        // C++ makes the columns nullable for a block with rows (and for the NULL row), so no rows
        // keep their types (`assert_num_rows_operator.cpp:76-97`)
        if a.should_convert_output_to_nullable == Some(true) && !out.block.is_empty() {
            for ty in &mut out.block.types {
                ty.nullable = true;
            }
        }
        Ok(out)
    }

    /// Rows typed as the given slots.
    fn chunk_of(&self, rows: Vec<Vec<Datum>>, slots: &[i32]) -> Result<Chunk, ExecError> {
        let types = slots.iter().map(|s| self.plan.desc.slot(*s).map(|s| s.ty.clone())).collect::<Result<_, _>>()?;
        Ok(Chunk {
            block: self.typed(Block { types, rows }, slots)?,
            slots: slots.to_vec(),
        })
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
        let chunks = self.receive_exchange(n.node_id, &key, senders)?;
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
        match &e.sort_info {
            // a merging exchange: its senders' sorted rows merged, then offset and limit
            Some(info) => sorted(out, &info.ordering_exprs, info, offset, limit(n.limit)),
            None => {
                out.block.rows.drain(..offset.min(out.block.rows.len()));
                Ok(out)
            }
        }
    }

    /// An exchange's chunks. A shared broadcast build (`query::shared_build`) reaches only the
    /// instances the plan sends it to; each of those forwards a copy to the instances of its
    /// fragment that no sender names, which take it as their one sender.
    fn receive_exchange(&self, node: i32, key: &ExchangeKey, senders: usize) -> Result<Vec<Chunk>, ExecError> {
        use crate::query::{plan_senders, shared_build};
        if let Some(chunks) = self.prefetched.borrow_mut().remove(&node) {
            return Ok(chunks);
        }
        let shared = self.fragment.mode == crate::query::RunMode::Each && shared_build(self.plan, self.at.0, node);
        if shared && plan_senders(self.plan, node, &self.instance.id) == 0 {
            return self.ctx.exchanges.receive(key, 1);
        }
        self.ctx.exchanges.receive(key, senders)
    }

    /// The shared broadcast builds this instance receives, taken before its tree runs and
    /// forwarded to the instances of its fragment that no sender names: they must get them even
    /// when this instance never reaches the join (an empty build side above it skips its probe
    /// child).
    fn prefetch_shared_builds(&self) -> Result<(), ExecError> {
        use crate::query::{plan_senders, shared_build};
        if self.fragment.mode != crate::query::RunMode::Each {
            return Ok(());
        }
        for n in self.fragment.root.nodes().into_iter().filter(|n| n.node_type == P::EXCHANGE_NODE) {
            let node = n.node_id;
            if !shared_build(self.plan, self.at.0, node) || plan_senders(self.plan, node, &self.instance.id) == 0 {
                continue;
            }
            let senders = self.fragment.per_exch_num_senders.get(&node).copied().unwrap_or(0).max(0) as usize;
            let key = ExchangeKey { query: qid(self.plan), instance: (self.instance.id.hi, self.instance.id.lo), node };
            let chunks = self.ctx.exchanges.receive(&key, senders)?;
            if self.at.1 == 0 {
                for other in self.fragment.instances.iter().filter(|i| plan_senders(self.plan, node, &i.id) == 0) {
                    let to = ExchangeKey { query: key.query, instance: (other.id.hi, other.id.lo), node };
                    for c in &chunks {
                        self.ctx.exchanges.send(&to, c.clone());
                    }
                    self.ctx.exchanges.end(&to);
                }
            }
            self.prefetched.borrow_mut().insert(node, chunks);
        }
        Ok(())
    }

    /// A merged fragment's other instances: no rows, but every stream they feed still gets their
    /// end, and a result or table sink their empty output.
    fn idle(&self) -> Result<(), ExecError> {
        let f = self.fragment;
        let q = qid(self.plan);
        let end = |s: &TDataStreamSink, destinations: &[TPlanFragmentDestination]| {
            for key in stream_keys(q, s, destinations) {
                self.ctx.exchanges.end(&key);
            }
        };
        match f.sink.type_ {
            TDataSinkType::DATA_STREAM_SINK => {
                let s = f.sink.stream_sink.as_ref().ok_or_else(|| ExecError::BadRequest("DATA_STREAM_SINK without its parameters".into()))?;
                end(s, &f.destinations);
                Ok(())
            }
            TDataSinkType::MULTI_CAST_DATA_STREAM_SINK => {
                let m = f
                    .sink
                    .multi_cast_stream_sink
                    .as_ref()
                    .ok_or_else(|| ExecError::BadRequest("MULTI_CAST_DATA_STREAM_SINK without its parameters".into()))?;
                let (sinks, destinations) = multicast_consumers(m)?;
                for (s, d) in sinks.iter().zip(destinations) {
                    end(s, d);
                }
                Ok(())
            }
            TDataSinkType::RESULT_SINK => (self.ctx.result)(empty_of(&f.output_exprs)?),
            TDataSinkType::OLAP_TABLE_SINK => (self.ctx.table)(self.at.0, self.at.1, empty_of(&f.output_exprs)?),
            other => Err(ExecError::Unsupported {
                what: crate::query::sink_type_name(other),
            }),
        }
    }

    /// A chunk's columns as the slots of another tuple of as many slots.
    fn relabel(&self, input: Chunk, tuple: i32) -> Result<Chunk, ExecError> {
        let slots = self.plan.desc.tuple_slots(tuple)?.to_vec();
        if slots == input.slots || slots.len() != input.slots.len() {
            return Ok(input);
        }
        Ok(Chunk { block: self.typed(input.block, &slots)?, slots })
    }

    fn schema_scan(&self, t: &PlanTree) -> Result<Chunk, ExecError> {
        let s = t.node.schema_scan_node.as_ref().ok_or_else(|| ExecError::BadRequest("SCHEMA_SCAN_NODE without its parameters".into()))?;
        let slots = self.plan.desc.tuple_slots(s.tuple_id)?.to_vec();
        let schema_slots = slots
            .iter()
            .map(|id| self.plan.desc.slot(*id).map(|s| crate::schema_scan::SchemaSlot { name: s.name.clone(), ty: s.ty.clone() }))
            .collect::<Result<Vec<_>, _>>()?;
        let block = crate::schema_scan::scan_schema(s, &schema_slots, self.ctx.fe)?;
        Ok(Chunk { block, slots })
    }

    /// REPEAT_NODE: the child once per repeat (`crate::repeat`), typed onto the output tuple.
    fn repeat(&self, t: &PlanTree) -> Result<Chunk, ExecError> {
        let r = t.node.repeat_node.as_ref().ok_or_else(|| ExecError::BadRequest("REPEAT_NODE without its parameters".into()))?;
        let child = self.child(t, 0)?;
        let slots = self.plan.desc.tuple_slots(r.output_tuple_id)?.to_vec();
        if slots.len() != r.exprs.len() + r.grouping_list.len() {
            return Err(ExecError::BadRequest("a REPEAT_NODE whose output tuple is not its exprs and grouping columns".into()));
        }
        let map = slot_map(&child.slots);
        let cols = r.exprs.iter().map(|e| crate::eval::eval_column(e, &child.block, &map).map(|(_, v)| v)).collect::<Result<Vec<_>, _>>()?;
        let rows = child.block.len();
        let values: Vec<Vec<Datum>> = (0..rows).map(|i| cols.iter().map(|c| c[i].clone()).collect()).collect();
        let out = crate::repeat::repeat_rows(r, &slots[..r.exprs.len()], &values, rows)?;
        let types = slots.iter().map(|s| self.plan.desc.slot(*s).map(|s| s.ty.clone())).collect::<Result<Vec<_>, _>>()?;
        Ok(Chunk { block: self.typed(Block { types, rows: out }, &slots)?, slots })
    }

    /// DATA_GEN_SCAN_NODE: this instance's `numbers` range, one non-null BIGINT column.
    fn data_gen(&self, t: &PlanTree) -> Result<Chunk, ExecError> {
        let d = t.node.data_gen_scan_node.as_ref().ok_or_else(|| ExecError::BadRequest("DATA_GEN_SCAN_NODE without its parameters".into()))?;
        let tuple = d.tuple_id.ok_or_else(|| ExecError::BadRequest("DATA_GEN_SCAN_NODE without its tuple".into()))?;
        let mut out = self.empty(tuple)?;
        crate::datagen::check_numbers_tuple(&out.block.types)?;
        for n in self.instance.data_gen.get(&t.node.node_id).into_iter().flatten() {
            out.block.rows.extend(crate::datagen::numbers(n)?.into_iter().map(|v| vec![v]));
        }
        Ok(out)
    }

    fn aggregate(&self, t: &PlanTree) -> Result<Chunk, ExecError> {
        let a = t.node.agg_node.as_ref().ok_or_else(|| ExecError::BadRequest("AGGREGATION_NODE without its parameters".into()))?;
        let agg = crate::aggregate::Aggregation::from_thrift(a)?;
        let mut block = match self.tablet_scans(t, &agg)? {
            Some(parts) => parts,
            None => agg.run(&self.child(t, 0)?)?,
        };
        // the FE pushed the limit of an ORDER BY on the group keys into the aggregation
        if let (Some(info), Some(l)) = (&a.agg_sort_info_by_group_key, limit(t.node.limit)) {
            block = crate::aggregate::top_groups(block, info, l);
        }
        let tuple = if a.need_finalize { a.output_tuple_id } else { a.intermediate_tuple_id };
        let slots = self.plan.desc.tuple_slots(tuple)?.to_vec();
        Ok(Chunk { block: self.typed(block, &slots)?, slots })
    }

    /// TABLE_FUNCTION_NODE: the child's slots (a slot not in `outputSlotIds` at its default),
    /// then one column per function, each child row once per combination of the functions'
    /// values (`table_function_operator.cpp`).
    fn table_function(&self, t: &PlanTree) -> Result<Chunk, ExecError> {
        use crate::table_function::{expand, function_name, values};
        let n = &t.node;
        let tf = n.table_function_node.as_ref().ok_or_else(|| ExecError::BadRequest("TABLE_FUNCTION_NODE without its parameters".into()))?;
        let child = self.child(t, 0)?;
        let map = slot_map(&child.slots);
        let fns = tf.fn_call_expr_list.as_deref().unwrap_or_default();
        let mut lists = Vec::with_capacity(fns.len());
        for e in fns {
            let name = function_name(e).unwrap_or_default();
            // in place: the plan's own nodes, whose addresses key their random generators
            let (_, arg) = crate::eval::eval_nodes(&e.nodes[1..], &child.block, &map)?;
            lists.push(arg.iter().map(|a| values(&name, a)).collect::<Vec<_>>());
        }
        let fn_tuple = *n.row_tuples.last().ok_or_else(|| ExecError::BadRequest("TABLE_FUNCTION_NODE without its tuples".into()))?;
        let fn_slots = self.plan.desc.tuple_slots(fn_tuple)?.to_vec();
        if fn_slots.len() != fns.len() {
            return Err(ExecError::BadRequest(format!("a TABLE_FUNCTION_NODE with {} functions for {} slots", fns.len(), fn_slots.len())));
        }
        let keep: Vec<bool> = child.slots.iter().map(|s| tf.output_slot_ids.as_ref().is_none_or(|o| o.contains(s))).collect();
        let defaults: Vec<Datum> = child.block.types.iter().map(default_of).collect();
        let rows = expand(&child.block.rows, &lists, &keep, &defaults);
        let mut types = child.block.types.clone();
        for s in &fn_slots {
            types.push(self.plan.desc.slots.get(s).map(|s| s.ty.clone()).ok_or_else(|| ExecError::BadRequest(format!("slot {s} is not described")))?);
        }
        let mut slots = child.slots;
        slots.extend(fn_slots);
        Ok(Chunk { block: Block { types, rows }, slots })
    }

    /// A scalar first-phase aggregation straight over a scan of several tablets: one partial
    /// state per tablet, as C++'s parallel scan tasks each keep their own (the merge combines
    /// them). None for any other aggregation.
    fn tablet_scans(&self, t: &PlanTree, agg: &crate::aggregate::Aggregation) -> Result<Option<Block>, ExecError> {
        let Some(scan) = t.children.first().filter(|c| c.node.node_type == P::OLAP_SCAN_NODE) else { return Ok(None) };
        let ranges = self.instance.scan_ranges.get(&scan.node.node_id).cloned().unwrap_or_default();
        // a scan limit counts across the tablets: such a scan runs once
        if agg.finalize || !agg.group_by.is_empty() || agg.functions.iter().any(|f| f.merge) || ranges.len() < 2 || scan.node.limit >= 0 {
            return Ok(None);
        }
        let mut out: Option<Block> = None;
        for range in ranges {
            let mut one = self.instance.clone();
            one.scan_ranges.insert(scan.node.node_id, vec![range]);
            let tablet = Instance { instance: &one, prefetched: Default::default(), ..*self };
            let part = agg.run(&tablet.node(scan)?)?;
            match &mut out {
                None => out = Some(part),
                Some(b) => b.rows.extend(part.rows),
            }
        }
        Ok(out)
    }

    /// ANALYTIC_EVAL_NODE: each input row, in input order, then one column per function, typed
    /// onto the output tuple. The input is sorted by partition keys, then order keys.
    fn analytic(&self, t: &PlanTree) -> Result<Chunk, ExecError> {
        use crate::analytic::{frame, window_columns, WindowFn};
        let a = t.node.analytic_node.as_ref().ok_or_else(|| ExecError::BadRequest("ANALYTIC_EVAL_NODE without its parameters".into()))?;
        let child = self.child(t, 0)?;
        let slots = slot_map(&child.slots);
        let n = child.block.len();
        let frame = frame(a.window.as_ref())?;
        // each row's key values over the given exprs, and their types
        let keys = |exprs: &[TExpr]| -> Result<(Vec<crate::value::ColType>, Vec<Vec<Datum>>), ExecError> {
            let mut types = Vec::new();
            let mut rows = vec![Vec::new(); n];
            for e in exprs {
                let (ty, values) = eval_column(e, &child.block, &slots)?;
                types.push(ty);
                for (row, v) in rows.iter_mut().zip(values) {
                    row.push(v);
                }
            }
            Ok((types, rows))
        };
        let (part_types, part) = keys(&a.partition_exprs)?;
        let (order_types, order) = keys(&a.order_by_exprs)?;
        let instances = self.fragment.instances.len();
        if instances > 1 && crate::analytic::split_float_keys(&part, &part_types) {
            return Err(ExecError::Unsupported {
                what: format!("an ANALYTIC_EVAL_NODE over float partition keys equal in value but not in bits (-0.0 and 0.0) in {instances} instances"),
            });
        }
        let out_slots = self.plan.desc.tuple_slots(a.output_tuple_id)?.to_vec();
        if out_slots.len() != a.analytic_functions.len() {
            return Err(ExecError::BadRequest(format!(
                "an ANALYTIC_EVAL_NODE with {} functions for a tuple of {} slots",
                a.analytic_functions.len(),
                out_slots.len()
            )));
        }
        let mut results = Block { types: Vec::new(), rows: vec![Vec::new(); n] };
        for e in &a.analytic_functions {
            let f = WindowFn::from_expr(e)?;
            let args = f.args.iter().map(|x| eval_column(x, &child.block, &slots)).collect::<Result<Vec<_>, _>>()?;
            let values = window_columns(&part, &part_types, &order, &order_types, &crate::analytic::Window { frame: &frame, f: &f, args: &args, batch_size: self.plan.batch_size })?;
            results.types.push(f.ret.clone());
            for (row, v) in results.rows.iter_mut().zip(values) {
                row.push(v);
            }
        }
        let results = self.typed(results, &out_slots)?;
        let Chunk { block: Block { mut types, rows }, slots: mut all } = child;
        types.extend(results.types);
        let rows = rows.into_iter().zip(results.rows).map(|(mut r, f)| {
            r.extend(f);
            r
        });
        all.extend(out_slots);
        Ok(Chunk { block: Block { types, rows: rows.collect() }, slots: all })
    }

    fn sort(&self, t: &PlanTree) -> Result<Chunk, ExecError> {
        let n = &t.node;
        let s = n.sort_node.as_ref().ok_or_else(|| ExecError::BadRequest("SORT_NODE without its parameters".into()))?;
        let child = self.child(t, 0)?;
        let input = self.sort_input(n, &s.sort_info, child)?;
        sorted(input, &s.sort_info.ordering_exprs, &s.sort_info, s.offset.unwrap_or(0).max(0) as usize, limit(n.limit))
    }

    /// A sort's input on its own tuple: its slot exprs over the child, or the child's columns by
    /// position when there are none.
    fn sort_input(&self, n: &doris_thrift::gen::plan_nodes::TPlanNode, info: &doris_thrift::gen::plan_nodes::TSortInfo, child: Chunk) -> Result<Chunk, ExecError> {
        Ok(match (&info.sort_tuple_slot_exprs, n.row_tuples.first()) {
            (Some(exprs), Some(tuple)) => {
                let slots = self.plan.desc.tuple_slots(*tuple)?.to_vec();
                Chunk { block: self.typed(project(exprs, &child)?, &slots)?, slots }
            }
            (None, Some(tuple)) => self.relabel(child, *tuple)?,
            _ => child,
        })
    }

    /// PARTITION_SORT_NODE: each partition's top rows (`partition_sort`). The partition exprs
    /// read the child; the ordering exprs read the node's own tuple.
    fn partition_sort(&self, t: &PlanTree) -> Result<Chunk, ExecError> {
        let n = &t.node;
        let (p, info, algo, limit) = crate::query::partition_sort_node(n)?;
        let child = self.child(t, 0)?;
        let part = project(p, &child)?.rows;
        let input = self.sort_input(n, info, child)?;
        let key_block = project(&info.ordering_exprs, &input)?;
        let ks = keys(info, &(0..info.ordering_exprs.len()).collect::<Vec<_>>());
        let cmp = |a: usize, b: usize| cmp_rows(&key_block.rows[a], &key_block.rows[b], &key_block.types, &ks);
        let Chunk { block: Block { types, rows }, slots } = input;
        let rows = crate::partition_sort::partition_top_n(rows, &part, cmp, algo, limit);
        Ok(Chunk { block: Block { types, rows }, slots })
    }

    fn materialize(&self, t: &PlanTree) -> Result<Chunk, ExecError> {
        let m = t.node.materialization_node.as_ref().ok_or_else(|| ExecError::BadRequest("MATERIALIZATION_NODE without its parameters".into()))?;
        let child = self.child(t, 0)?;
        let desc = &self.plan.desc;
        let inter = desc.tuple_slots(m.intermediate_tuple_id.ok_or_else(|| ExecError::BadRequest("no intermediate tuple".into()))?)?.to_vec();
        let fetch_exprs = m.fetch_expr_lists.clone().unwrap_or_default();
        let column_lists = m.column_descs_lists.clone().unwrap_or_default();
        let slot_locs = m.slot_locs_lists.clone().unwrap_or_default();
        // the row id column of each relation, and the columns fetched for it
        let mut fetched: Vec<(usize, Vec<crate::value::ColType>, Vec<Vec<Datum>>)> = Vec::new();
        let map = slot_map(&child.slots);
        for (j, e) in fetch_exprs.iter().enumerate() {
            let slot = e.nodes.first().and_then(|n| n.slot_ref.as_ref()).ok_or_else(|| ExecError::Unsupported { what: "a row id that is not a slot".into() })?.slot_id;
            let at = *map.0.get(&slot).ok_or_else(|| ExecError::BadRequest(format!("row id slot {slot} is not in the input")))?;
            let cols = column_lists.get(j).cloned().unwrap_or_default();
            let locs = slot_locs.get(j).cloned().unwrap_or_default();
            let types: Vec<_> = locs
                .iter()
                .map(|l| inter.get(*l as usize).ok_or_else(|| ExecError::BadRequest(format!("slot location {l}"))).and_then(|s| desc.slot(*s).map(|s| s.ty.clone())))
                .collect::<Result<_, _>>()?;
            let scan_cols: Vec<ScanColumn> = cols
                .iter()
                .zip(&types)
                .map(|(c, ty)| {
                    let uid = c.col_unique_id.unwrap_or(-1);
                    Ok(ScanColumn {
                        unique_id: u32::try_from(uid).map_err(|_| ExecError::BadRequest(format!("column {} has no unique id", c.column_name)))?,
                        ty: ColTypeRef { ptype: ty.ptype, precision: ty.precision, scale: ty.scale },
                    })
                })
                .collect::<Result<_, ExecError>>()?;
            let ids: Vec<Vec<u8>> = child
                .block
                .rows
                .iter()
                .map(|r| match &r[at] {
                    Datum::Bytes(b) => Ok(b.clone()),
                    other => Err(ExecError::BadRequest(format!("row id {other:?}"))),
                })
                .collect::<Result<_, _>>()?;
            fetched.push((at, types, (self.ctx.fetch)(&ids, &scan_cols)?));
        }
        // MaterializationSharedState::get_block: each row id column becomes its fetched columns
        let mut types = Vec::new();
        for (i, t) in child.block.types.iter().enumerate() {
            match fetched.iter().find(|f| f.0 == i) {
                Some((_, ft, _)) => types.extend(ft.iter().cloned()),
                None => types.push(t.clone()),
            }
        }
        let rows = child
            .block
            .rows
            .into_iter()
            .enumerate()
            .map(|(r, row)| {
                let mut out = Vec::with_capacity(types.len());
                for (i, d) in row.into_iter().enumerate() {
                    match fetched.iter().find(|f| f.0 == i) {
                        Some((_, _, values)) => out.extend(values[r].iter().cloned()),
                        None => out.push(d),
                    }
                }
                out
            })
            .collect();
        if types.len() != inter.len() {
            return Err(ExecError::BadRequest(format!("materialized {} columns for a tuple of {}", types.len(), inter.len())));
        }
        Ok(Chunk { block: Block { types, rows }, slots: inter })
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
                let out = consumer_copy(out, s, &self.plan.desc)?;
                self.send(s, &f.destinations, out)
            }
            TDataSinkType::MULTI_CAST_DATA_STREAM_SINK => {
                let m = f
                    .sink
                    .multi_cast_stream_sink
                    .as_ref()
                    .ok_or_else(|| ExecError::BadRequest("MULTI_CAST_DATA_STREAM_SINK without its parameters".into()))?;
                for (s, destinations, copy) in multicast(&out, m, &self.plan.desc)? {
                    self.send(&s, &destinations, copy)?;
                }
                Ok(())
            }
            TDataSinkType::OLAP_TABLE_SINK => (self.ctx.table)(self.at.0, self.at.1, project(&f.output_exprs, &out)?),
            other => Err(ExecError::Unsupported {
                what: crate::query::sink_type_name(other),
            }),
        }
    }

    /// A stream sink's rows, partitioned over its destinations: each gets its part and this
    /// sender's end.
    fn send(&self, s: &TDataStreamSink, destinations: &[TPlanFragmentDestination], out: Chunk) -> Result<(), ExecError> {
        send_stream(self.ctx.exchanges, qid(self.plan), s, destinations, out, self.instance.sender_id)
    }
}

/// A stream sink's rows, partitioned over its destinations: each gets its part and one end from
/// this sender, keyed by the sink's exchange node.
pub fn send_stream(
    exchanges: &Exchanges,
    query: (i64, i64),
    s: &TDataStreamSink,
    destinations: &[TPlanFragmentDestination],
    out: Chunk,
    sender: i32,
) -> Result<(), ExecError> {
    let keys: Vec<ExchangeKey> = destinations
        .iter()
        .map(|d| ExchangeKey { query, instance: (d.fragment_instance_id.hi, d.fragment_instance_id.lo), node: s.dest_node_id })
        .collect();
    let parts = partition(out, &s.output_partition, keys.len(), sender)?;
    // a bucket-shuffle list names an instance once per bucket it scans: each instance gets its
    // buckets' rows in one send and one end; an unscanned bucket's (-1, -1) rows are dropped
    let mut merged: Vec<(ExchangeKey, Chunk)> = Vec::new();
    for (key, part) in keys.into_iter().zip(parts) {
        if key.instance == (-1, -1) {
            continue;
        }
        match merged.iter_mut().find(|(k, _)| *k == key) {
            Some((_, c)) => c.block.rows.extend(part.block.rows),
            None => merged.push((key, part)),
        }
    }
    for (key, part) in merged {
        exchanges.send(&key, part);
        exchanges.end(&key);
    }
    Ok(())
}

/// The receivers a stream sink sends to, each once, without an unscanned bucket's (-1, -1).
fn stream_keys(query: (i64, i64), s: &TDataStreamSink, destinations: &[TPlanFragmentDestination]) -> Vec<ExchangeKey> {
    let mut keys: Vec<ExchangeKey> = Vec::new();
    for d in destinations {
        let key = ExchangeKey { query, instance: (d.fragment_instance_id.hi, d.fragment_instance_id.lo), node: s.dest_node_id };
        if key.instance != (-1, -1) && !keys.contains(&key) {
            keys.push(key);
        }
    }
    keys
}

/// No rows, in the types of a sink's output exprs.
fn empty_of(exprs: &[TExpr]) -> Result<Block, ExecError> {
    let types = exprs
        .iter()
        .map(|e| {
            e.nodes
                .first()
                .and_then(|n| crate::value::ColType::from_type_desc("", &n.type_, true))
                .ok_or_else(|| ExecError::BadRequest("an output expr of a nested type".into()))
        })
        .collect::<Result<_, _>>()?;
    Ok(Block::new(types))
}

/// Values in the types of a tuple's slots.
pub fn typed(desc: &DescTable, block: Block, slots: &[i32]) -> Result<Block, ExecError> {
    let to: Vec<_> = slots
        .iter()
        .map(|s| desc.slot(*s).map(|s| s.ty.clone()))
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

/// A stream sink's own copy of the rows: its conjuncts first, then its output exprs onto its
/// output tuple (MultiCastDataStreamerSourceOperatorX::get_block). C++'s ExchangeSinkOperatorX
/// applies neither; the FE sets them only on multicast consumers, so a plain DATA_STREAM_SINK
/// passing through here sends its rows unchanged.
fn consumer_copy(mut out: Chunk, s: &TDataStreamSink, desc: &DescTable) -> Result<Chunk, ExecError> {
    if let Some(c) = s.conjuncts.as_ref().filter(|c| !c.is_empty()) {
        out.block = filter(c, out.block, &slot_map(&out.slots))?;
    }
    if let Some(tuple) = projection_tuple(s)? {
        let exprs = s.output_exprs.as_deref().unwrap_or_default();
        let slots = desc.tuple_slots(tuple)?.to_vec();
        out = Chunk { block: typed(desc, project(exprs, &out)?, &slots)?, slots };
    }
    Ok(out)
}

/// A multicast sink's consumers (a CTE used more than once): each with its stream sink, its
/// destinations and its own copy of the fragment's output.
pub fn multicast(out: &Chunk, m: &TMultiCastDataStreamSink, desc: &DescTable) -> Result<Vec<(TDataStreamSink, Vec<TPlanFragmentDestination>, Chunk)>, ExecError> {
    let (sinks, destinations) = multicast_consumers(m)?;
    sinks
        .iter()
        .zip(destinations)
        .map(|(s, d)| consumer_copy(out.clone(), s, desc).map(|c| (s.clone(), d.clone(), c)))
        .collect()
}

fn limit(l: i64) -> Option<usize> {
    (l >= 0).then_some(l as usize)
}

/// A chunk sorted by expressions over it, from `offset`, at most `limit` rows.
fn sorted(input: Chunk, ordering: &[TExpr], info: &doris_thrift::gen::plan_nodes::TSortInfo, offset: usize, limit: Option<usize>) -> Result<Chunk, ExecError> {
    let key_block = project(ordering, &input)?;
    let ks = keys(info, &(0..ordering.len()).collect::<Vec<_>>());
    let mut order: Vec<usize> = (0..input.block.len()).collect();
    order.sort_by(|a, b| cmp_rows(&key_block.rows[*a], &key_block.rows[*b], &key_block.types, &ks));
    let end = limit.map_or(order.len(), |l| offset.saturating_add(l).min(order.len()));
    let keep: Vec<usize> = order[offset.min(end)..end].to_vec();
    let Chunk { block: Block { types, rows }, slots } = input;
    let mut rows: Vec<Option<Vec<Datum>>> = rows.into_iter().map(Some).collect();
    let rows = keep.into_iter().map(|i| rows[i].take().unwrap_or_default()).collect();
    Ok(Chunk { block: Block { types, rows }, slots })
}

/// A stable hash of a row's partition values: equal values (as compared) hash alike in every
/// sender of the query.
fn hash_values(values: &[Datum]) -> u64 {
    // FNV-1a over a canonical form of each value
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    let mut eat = |bytes: &[u8]| {
        for b in bytes {
            h ^= u64::from(*b);
            h = h.wrapping_mul(0x0100_0000_01b3);
        }
    };
    for v in values {
        match v {
            Datum::Null => eat(&[0]),
            Datum::Bool(b) => eat(&[1, u8::from(*b)]),
            Datum::Int(i) => {
                eat(&[2]);
                eat(&i.to_le_bytes());
            }
            Datum::Float(f) => {
                let f = f64::from(*f);
                eat(&[3]);
                eat(&(if f == 0.0 { 0.0f64 } else if f.is_nan() { f64::NAN } else { f }).to_bits().to_le_bytes());
            }
            Datum::Double(f) => {
                eat(&[3]);
                eat(&(if *f == 0.0 { 0.0f64 } else if f.is_nan() { f64::NAN } else { *f }).to_bits().to_le_bytes());
            }
            Datum::Bytes(b) => {
                eat(&[4]);
                eat(&(b.len() as u64).to_le_bytes());
                eat(b);
            }
            Datum::Array(_) | Datum::Map(_) | Datum::Struct(_) => {
                let k = crate::set_op::row_key(std::slice::from_ref(v));
                eat(&[5]);
                eat(&(k.len() as u64).to_le_bytes());
                eat(&k);
            }
        }
    }
    h
}

/// A sink's rows split over its destinations: all to each (UNPARTITIONED), by a hash of the
/// partition expressions (HASH_PARTITIONED), or round robin (RANDOM).
fn partition(out: Chunk, p: &doris_thrift::gen::partitions::TDataPartition, n: usize, sender: i32) -> Result<Vec<Chunk>, ExecError> {
    use doris_thrift::gen::partitions::TPartitionType as T;
    if n == 0 {
        return Ok(Vec::new());
    }
    let empty = || Chunk { block: Block::new(out.block.types.clone()), slots: out.slots.clone() };
    match p.type_ {
        T::UNPARTITIONED => Ok(vec![out; n]),
        T::HASH_PARTITIONED => {
            let exprs = p.partition_exprs.clone().unwrap_or_default();
            let values = project(&exprs, &out)?;
            let mut parts: Vec<Chunk> = (0..n).map(|_| empty()).collect();
            for (row, key) in out.block.rows.into_iter().zip(values.rows) {
                parts[(hash_values(&key) % n as u64) as usize].block.rows.push(row);
            }
            Ok(parts)
        }
        T::BUCKET_SHFFULE_HASH_PARTITIONED => {
            // destination i is the instance scanning bucket i; a constant column is not hashed
            // (`partitioner.cpp:31-66`)
            let exprs: Vec<TExpr> = p
                .partition_exprs
                .iter()
                .flatten()
                .filter(|e| e.nodes.iter().any(|n| n.node_type == doris_thrift::gen::exprs::TExprNodeType::SLOT_REF))
                .cloned()
                .collect();
            let values = project(&exprs, &out)?;
            let mut parts: Vec<Chunk> = (0..n).map(|_| empty()).collect();
            for (row, key) in out.block.rows.into_iter().zip(values.rows) {
                let typed: Vec<(crate::value::ColType, Datum)> = values.types.iter().cloned().zip(key).collect();
                parts[(crate::route::bucket_hash(&typed) % n as u32) as usize].block.rows.push(row);
            }
            Ok(parts)
        }
        // the receiving table sink routes each row itself, so any split is right
        T::RANDOM | T::OLAP_TABLE_SINK_HASH_PARTITIONED => {
            let mut parts: Vec<Chunk> = (0..n).map(|_| empty()).collect();
            for (i, row) in out.block.rows.into_iter().enumerate() {
                parts[(i + sender.max(0) as usize) % n].block.rows.push(row);
            }
            Ok(parts)
        }
        other => Err(ExecError::Unsupported { what: format!("a DATA_STREAM_SINK partitioned as {other:?}") }),
    }
}

/// Which sides of a joined row a join's intermediate tuple holds, by its op
/// (`hashjoin_probe_operator.cpp:618-621`, FE `PhysicalPlanTranslator.java:1895-1915`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Sides {
    /// Right semi and anti joins without other (or mark) conjuncts: the build side only.
    Build,
    /// Left semi, anti and null-aware joins: the probe side, or both sides when they have
    /// other conjuncts.
    ProbeOrBoth,
    Both,
}

/// Where a joined row (probe columns, build columns, then the mark) goes in the join's
/// intermediate tuple: the sides its op holds, each with the mark if any
/// (`join_probe_operator.cpp:54-62`).
#[derive(Debug, Clone, Copy)]
pub struct Layout {
    probe: usize,
    build: usize,
    keep_probe: bool,
    keep_build: bool,
}

impl Layout {
    /// The sides a join of `op` holds.
    pub fn sides(op: crate::hash_join::JoinOp, has_other: bool, mark: bool, has_mark_conj: bool) -> Sides {
        use crate::hash_join::JoinOp::*;
        match op {
            RightSemi | RightAnti if !has_other && (!mark || !has_mark_conj) => Sides::Build,
            LeftSemi | LeftAnti | NullAwareLeftAnti | NullAwareLeftSemi => Sides::ProbeOrBoth,
            _ => Sides::Both,
        }
    }

    pub fn new(slots: usize, probe: usize, build: usize, mark: bool, sides: Sides) -> Result<Layout, ExecError> {
        let m = usize::from(mark);
        let (keep_probe, keep_build) = match sides {
            Sides::Build if slots == build + m => (false, true),
            Sides::ProbeOrBoth if slots == probe + m => (true, false),
            Sides::ProbeOrBoth | Sides::Both if slots == probe + build + m => (true, true),
            _ => {
                return Err(ExecError::BadRequest(format!(
                    "a join's intermediate tuple has {slots} slots for {probe} probe and {build} build columns ({sides:?})"
                )))
            }
        };
        Ok(Layout { probe, build, keep_probe, keep_build })
    }

    pub fn apply(&self, row: &[Datum]) -> Vec<Datum> {
        let mut out = Vec::with_capacity(row.len());
        if self.keep_probe {
            out.extend_from_slice(&row[..self.probe]);
        }
        if self.keep_build {
            out.extend_from_slice(&row[self.probe..self.probe + self.build]);
        }
        out.extend_from_slice(&row[self.probe + self.build..]);
        out
    }
}

/// `insert_many_defaults`: NULL in a nullable column, else the type's zero.
fn default_of(t: &crate::value::ColType) -> Datum {
    use doris_thrift::gen::types::TPrimitiveType as T;
    match t.ptype {
        _ if t.nullable => Datum::Null,
        T::BOOLEAN => Datum::Bool(false),
        T::FLOAT => Datum::Float(0.0),
        T::DOUBLE => Datum::Double(0.0),
        T::CHAR | T::VARCHAR | T::STRING => Datum::Bytes(vec![]),
        T::ARRAY => Datum::Array(vec![]),
        T::MAP => Datum::Map(vec![]),
        T::STRUCT => Datum::Struct(t.fields().unwrap_or_default().iter().map(|(_, f)| default_of(f)).collect()),
        _ => Datum::Int(0),
    }
}

fn truth(d: &Datum) -> Option<bool> {
    match d {
        Datum::Null => None,
        Datum::Bool(b) => Some(*b),
        Datum::Int(i) => Some(*i != 0),
        Datum::Float(f) => Some(*f != 0.0),
        Datum::Double(f) => Some(*f != 0.0),
        Datum::Bytes(_) => Some(true),
        Datum::Array(_) | Datum::Map(_) | Datum::Struct(_) => None,
    }
}

/// Three-valued AND.
fn and3(a: Option<bool>, b: Option<bool>) -> Option<bool> {
    match (a, b) {
        (Some(false), _) | (_, Some(false)) => Some(false),
        (Some(true), Some(true)) => Some(true),
        _ => None,
    }
}
