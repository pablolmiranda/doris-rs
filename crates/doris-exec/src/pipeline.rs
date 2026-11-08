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
            P::INTERSECT_NODE | P::EXCEPT_NODE => self.set_op(t)?,
            P::EXCHANGE_NODE => self.exchange(t)?,
            P::SORT_NODE => self.sort(t)?,
            P::AGGREGATION_NODE => self.aggregate(t)?,
            P::SCHEMA_SCAN_NODE => self.schema_scan(t)?,
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
        for (i, id) in slots.iter().enumerate() {
            let slot = desc.slot(*id)?;
            types.push(slot.ty.clone());
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
        for row in &u.const_expr_lists {
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
        match &e.sort_info {
            // a merging exchange: its senders' sorted rows merged, then offset and limit
            Some(info) => sorted(out, &info.ordering_exprs, info, offset, limit(n.limit)),
            None => {
                out.block.rows.drain(..offset.min(out.block.rows.len()));
                Ok(out)
            }
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

    fn aggregate(&self, t: &PlanTree) -> Result<Chunk, ExecError> {
        let a = t.node.agg_node.as_ref().ok_or_else(|| ExecError::BadRequest("AGGREGATION_NODE without its parameters".into()))?;
        let child = self.child(t, 0)?;
        let mut block = crate::aggregate::Aggregation::from_thrift(a)?.run(&child)?;
        // the FE pushed the limit of an ORDER BY on the group keys into the aggregation
        if let (Some(info), Some(l)) = (&a.agg_sort_info_by_group_key, limit(t.node.limit)) {
            block = crate::aggregate::top_groups(block, info, l);
        }
        let tuple = if a.need_finalize { a.output_tuple_id } else { a.intermediate_tuple_id };
        let slots = self.plan.desc.tuple_slots(tuple)?.to_vec();
        Ok(Chunk { block: self.typed(block, &slots)?, slots })
    }

    fn sort(&self, t: &PlanTree) -> Result<Chunk, ExecError> {
        let n = &t.node;
        let s = n.sort_node.as_ref().ok_or_else(|| ExecError::BadRequest("SORT_NODE without its parameters".into()))?;
        let child = self.child(t, 0)?;
        let input = match (&s.sort_info.sort_tuple_slot_exprs, n.row_tuples.first()) {
            (Some(exprs), Some(tuple)) => {
                let slots = self.plan.desc.tuple_slots(*tuple)?.to_vec();
                Chunk { block: self.typed(project(exprs, &child)?, &slots)?, slots }
            }
            // no slot expressions: the sort tuple takes the child's columns by position
            (None, Some(tuple)) => self.relabel(child, *tuple)?,
            _ => child,
        };
        sorted(input, &s.sort_info.ordering_exprs, &s.sort_info, s.offset.unwrap_or(0).max(0) as usize, limit(n.limit))
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
    for (key, part) in keys.iter().zip(parts) {
        exchanges.send(key, part);
        exchanges.end(key);
    }
    Ok(())
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
        T::RANDOM => {
            let mut parts: Vec<Chunk> = (0..n).map(|_| empty()).collect();
            for (i, row) in out.block.rows.into_iter().enumerate() {
                parts[(i + sender.max(0) as usize) % n].block.rows.push(row);
            }
            Ok(parts)
        }
        other => Err(ExecError::Unsupported { what: format!("a DATA_STREAM_SINK partitioned as {other:?}") }),
    }
}
