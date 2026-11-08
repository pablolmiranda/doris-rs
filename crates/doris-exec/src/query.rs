//! Query fragments as the FE sends them for a SELECT ([wire] §1): every fragment of the query in
//! one `TPipelineFragmentParamsList`, the receiving fragment first. Fragments marked
//! `is_simplified_param` share the first full fragment's descriptor table, globals and
//! coordinator. `check_plan` refuses, naming it, the first node, sink, expression or type
//! doris-rs does not run, so that nothing of a refused query runs.
use std::collections::{BTreeMap, HashMap};

use doris_thrift::gen::data_sinks::{TDataSink, TDataSinkType, TDataStreamSink, TMultiCastDataStreamSink, TPlanFragmentDestination};
use doris_thrift::gen::descriptors::TDescriptorTable;
use doris_thrift::gen::exprs::TExpr;
use doris_thrift::gen::palo_internal_service::TPipelineFragmentParamsList;
use doris_thrift::gen::partitions::TPartitionType;
use doris_thrift::gen::plan_nodes::{TPlanNode, TPlanNodeType as P};
use doris_thrift::gen::types::{TNetworkAddress, TUniqueId};

use crate::eval::check_nodes;
use crate::value::ColType;
use crate::ExecError;

#[derive(Debug, Clone, PartialEq)]
pub struct SlotInfo {
    pub id: i32,
    pub tuple: i32,
    pub ty: ColType,
    pub unique_id: i32,
    pub name: String,
}

/// Slots by id, and each tuple's slots in descriptor order.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct DescTable {
    pub slots: HashMap<i32, SlotInfo>,
    pub tuples: HashMap<i32, Vec<i32>>,
}

impl DescTable {
    pub fn from_thrift(d: &TDescriptorTable) -> Result<DescTable, ExecError> {
        let mut out = DescTable::default();
        for t in &d.tuple_descriptors {
            out.tuples.entry(t.id).or_default();
        }
        for s in d.slot_descriptors.iter().flatten() {
            let nullable = s.null_indicator_bit >= 0 || s.slot_type.is_nullable == Some(true);
            let ty = ColType::from_type_desc(&s.col_name, &s.slot_type, nullable).ok_or_else(|| ExecError::Unsupported {
                what: format!("slot {} ({}) of a nested type", s.id, s.col_name),
            })?;
            out.tuples.entry(s.parent).or_default().push(s.id);
            out.slots.insert(
                s.id,
                SlotInfo {
                    id: s.id,
                    tuple: s.parent,
                    ty,
                    unique_id: s.col_unique_id.unwrap_or(-1),
                    name: s.col_name.clone(),
                },
            );
        }
        Ok(out)
    }

    pub fn tuple_slots(&self, tuple: i32) -> Result<&[i32], ExecError> {
        self.tuples
            .get(&tuple)
            .map(Vec::as_slice)
            .ok_or_else(|| ExecError::BadRequest(format!("no tuple {tuple} in the descriptor table")))
    }

    pub fn slot(&self, id: i32) -> Result<&SlotInfo, ExecError> {
        self.slots
            .get(&id)
            .ok_or_else(|| ExecError::BadRequest(format!("no slot {id} in the descriptor table")))
    }
}

/// A plan node with its children (the FE sends them flattened in pre-order).
#[derive(Debug, Clone, PartialEq)]
pub struct PlanTree {
    pub node: TPlanNode,
    pub children: Vec<PlanTree>,
}

impl PlanTree {
    fn build(nodes: &[TPlanNode], at: &mut usize) -> Result<PlanTree, ExecError> {
        let node = nodes.get(*at).cloned().ok_or_else(|| ExecError::BadRequest("plan ends early".into()))?;
        *at += 1;
        let children = (0..node.num_children.max(0))
            .map(|_| PlanTree::build(nodes, at))
            .collect::<Result<_, _>>()?;
        Ok(PlanTree { node, children })
    }

    /// Every node, pre-order.
    pub fn nodes(&self) -> Vec<&TPlanNode> {
        let mut out = vec![&self.node];
        for c in &self.children {
            out.extend(c.nodes());
        }
        out
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct InstancePlan {
    pub id: TUniqueId,
    pub sender_id: i32,
    /// Scan node → `(tablet id, version)` ranges.
    pub scan_ranges: BTreeMap<i32, Vec<(i64, i64)>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FragmentPlan {
    pub fragment_id: i32,
    pub root: PlanTree,
    pub sink: TDataSink,
    pub output_exprs: Vec<TExpr>,
    pub per_exch_num_senders: BTreeMap<i32, i32>,
    pub destinations: Vec<TPlanFragmentDestination>,
    pub instances: Vec<InstancePlan>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct QueryPlan {
    pub query_id: TUniqueId,
    pub fragments: Vec<FragmentPlan>,
    pub desc: DescTable,
    pub batch_size: usize,
    /// Seconds the FE waits for the query.
    pub timeout_s: i64,
    pub coord: Option<TNetworkAddress>,
    pub backend_id: Option<i64>,
    /// Whether execution waits for `exec_plan_fragment_start`.
    pub need_wait: bool,
    /// Results are fetched by query id; when false, by the result instance id.
    pub parallel_result_sink: bool,
    pub binary_rows: bool,
    /// `dry_run_query`: the result sink counts rows and sends none.
    pub dry_run: bool,
}

impl QueryPlan {
    /// The fragment holding the result sink.
    pub fn result_fragment(&self) -> Option<&FragmentPlan> {
        self.fragments.iter().find(|f| f.sink.type_ == TDataSinkType::RESULT_SINK)
    }
}

pub fn plan_query(list: &TPipelineFragmentParamsList) -> Result<QueryPlan, ExecError> {
    let params = list.params_list.as_deref().unwrap_or_default();
    let full = params
        .iter()
        .find(|p| p.desc_tbl.is_some())
        .ok_or_else(|| ExecError::BadRequest("no fragment carries the descriptor table".into()))?;
    let desc = DescTable::from_thrift(full.desc_tbl.as_ref().expect("checked"))?;
    let options = full.query_options.clone();
    let mut fragments = Vec::new();
    for p in params {
        let f = p
            .fragment
            .as_ref()
            .ok_or_else(|| ExecError::BadRequest("a fragment without its plan".into()))?;
        let nodes = f.plan.as_ref().map(|p| p.nodes.as_slice()).unwrap_or_default();
        let root = PlanTree::build(nodes, &mut 0)?;
        let sink = f
            .output_sink
            .clone()
            .ok_or_else(|| ExecError::BadRequest("a fragment without its sink".into()))?;
        let instances = p
            .local_params
            .iter()
            .flatten()
            .map(|l| InstancePlan {
                id: l.fragment_instance_id.clone(),
                sender_id: l.sender_id.unwrap_or(0),
                scan_ranges: l
                    .per_node_scan_ranges
                    .iter()
                    .map(|(node, ranges)| {
                        let r = ranges
                            .iter()
                            .filter_map(|r| r.scan_range.palo_scan_range.as_ref())
                            .map(|pr| (pr.tablet_id, pr.version.parse().unwrap_or(0)))
                            .collect();
                        (*node, r)
                    })
                    .collect(),
            })
            .collect();
        fragments.push(FragmentPlan {
            fragment_id: p.fragment_id.unwrap_or(0),
            root,
            sink,
            output_exprs: f.output_exprs.clone().unwrap_or_default(),
            per_exch_num_senders: p.per_exch_num_senders.clone(),
            destinations: p.destinations.clone().unwrap_or_default(),
            instances,
        });
    }
    let o = options.as_ref();
    Ok(QueryPlan {
        query_id: full.query_id.clone(),
        fragments,
        desc,
        batch_size: o.and_then(|o| o.batch_size).filter(|b| *b > 0).unwrap_or(4064) as usize,
        timeout_s: o.and_then(|o| o.query_timeout).filter(|t| *t > 0).unwrap_or(900).into(),
        coord: full.coord.clone(),
        backend_id: full.backend_id,
        need_wait: params.iter().any(|p| p.need_wait_execution_trigger == Some(true)),
        parallel_result_sink: o.and_then(|o| o.enable_parallel_result_sink).unwrap_or(true),
        binary_rows: o.and_then(|o| o.mysql_row_binary_format).unwrap_or(false),
        dry_run: o.and_then(|o| o.dry_run_query).unwrap_or(false),
    })
}

/// A plan node type's Thrift name.
pub fn node_type_name(t: P) -> String {
    const NAMES: &[&str] = &[
        "OLAP_SCAN_NODE",
        "MYSQL_SCAN_NODE",
        "CSV_SCAN_NODE",
        "SCHEMA_SCAN_NODE",
        "HASH_JOIN_NODE",
        "MERGE_JOIN_NODE",
        "AGGREGATION_NODE",
        "PRE_AGGREGATION_NODE",
        "SORT_NODE",
        "EXCHANGE_NODE",
        "MERGE_NODE",
        "SELECT_NODE",
        "CROSS_JOIN_NODE",
        "META_SCAN_NODE",
        "ANALYTIC_EVAL_NODE",
        "OLAP_REWRITE_NODE",
        "KUDU_SCAN_NODE",
        "BROKER_SCAN_NODE",
        "EMPTY_SET_NODE",
        "UNION_NODE",
        "ES_SCAN_NODE",
        "ES_HTTP_SCAN_NODE",
        "REPEAT_NODE",
        "ASSERT_NUM_ROWS_NODE",
        "INTERSECT_NODE",
        "EXCEPT_NODE",
        "ODBC_SCAN_NODE",
        "TABLE_FUNCTION_NODE",
        "DATA_GEN_SCAN_NODE",
        "FILE_SCAN_NODE",
        "JDBC_SCAN_NODE",
        "TEST_EXTERNAL_SCAN_NODE",
        "PARTITION_SORT_NODE",
        "GROUP_COMMIT_SCAN_NODE",
        "MATERIALIZATION_NODE",
        "REC_CTE_NODE",
        "REC_CTE_SCAN_NODE",
    ];
    usize::try_from(t.0)
        .ok()
        .and_then(|i| NAMES.get(i))
        .map_or_else(|| format!("{t:?}"), |s| s.to_string())
}

/// A data sink type's Thrift name.
pub fn sink_type_name(t: TDataSinkType) -> String {
    const NAMES: &[&str] = &[
        "DATA_STREAM_SINK",
        "RESULT_SINK",
        "DATA_SPLIT_SINK",
        "MYSQL_TABLE_SINK",
        "EXPORT_SINK",
        "OLAP_TABLE_SINK",
        "MEMORY_SCRATCH_SINK",
        "ODBC_TABLE_SINK",
        "RESULT_FILE_SINK",
        "JDBC_TABLE_SINK",
        "MULTI_CAST_DATA_STREAM_SINK",
        "GROUP_COMMIT_OLAP_TABLE_SINK",
        "GROUP_COMMIT_BLOCK_SINK",
        "HIVE_TABLE_SINK",
        "ICEBERG_TABLE_SINK",
        "DICTIONARY_SINK",
        "BLACKHOLE_SINK",
        "TVF_TABLE_SINK",
        "MAXCOMPUTE_TABLE_SINK",
        "ICEBERG_DELETE_SINK",
        "ICEBERG_MERGE_SINK",
        "PAIMON_TABLE_SINK",
    ];
    usize::try_from(t.0)
        .ok()
        .and_then(|i| NAMES.get(i))
        .map_or_else(|| format!("{t:?}"), |s| s.to_string())
}

fn unsupported(what: impl Into<String>) -> ExecError {
    ExecError::Unsupported { what: what.into() }
}

fn check_exprs<'a>(exprs: impl IntoIterator<Item = &'a TExpr>, aggregates: bool) -> Result<(), ExecError> {
    exprs.into_iter().try_for_each(|e| check_nodes(e, aggregates))
}

fn check_partition(t: TPartitionType, what: &str) -> Result<(), ExecError> {
    if matches!(t, TPartitionType::UNPARTITIONED | TPartitionType::HASH_PARTITIONED | TPartitionType::RANDOM) {
        Ok(())
    } else {
        Err(unsupported(format!("{what} partitioned as {t:?}")))
    }
}

fn check_node(n: &TPlanNode) -> Result<(), ExecError> {
    let name = node_type_name(n.node_type);
    match n.node_type {
        P::OLAP_SCAN_NODE | P::EMPTY_SET_NODE | P::SELECT_NODE => {}
        P::UNION_NODE => {
            let u = n
                .union_node
                .as_ref()
                .ok_or_else(|| ExecError::BadRequest("UNION_NODE without its parameters".into()))?;
            check_exprs(u.result_expr_lists.iter().flatten(), false)?;
            check_exprs(u.const_expr_lists.iter().flatten(), false)?;
        }
        P::EXCHANGE_NODE => {
            let e = n
                .exchange_node
                .as_ref()
                .ok_or_else(|| ExecError::BadRequest("EXCHANGE_NODE without its parameters".into()))?;
            if let Some(info) = &e.sort_info {
                check_exprs(&info.ordering_exprs, false)?;
            }
            check_partition(e.partition_type.unwrap_or(TPartitionType::UNPARTITIONED), "an EXCHANGE_NODE")?;
        }
        P::SORT_NODE => {
            let s = n.sort_node.as_ref().ok_or_else(|| ExecError::BadRequest("SORT_NODE without its parameters".into()))?;
            if s.sort_info.use_two_phase_read == Some(true) {
                return Err(unsupported("a SORT_NODE with the two-phase read"));
            }
            check_exprs(&s.sort_info.ordering_exprs, false)?;
            check_exprs(s.sort_info.sort_tuple_slot_exprs.iter().flatten(), false)?;
        }
        P::SCHEMA_SCAN_NODE => {
            let s = n.schema_scan_node.as_ref().ok_or_else(|| ExecError::BadRequest("SCHEMA_SCAN_NODE without its parameters".into()))?;
            crate::schema_scan::check_schema_table(&s.table_name)?;
        }
        P::AGGREGATION_NODE => {
            let a = n.agg_node.as_ref().ok_or_else(|| ExecError::BadRequest("AGGREGATION_NODE without its parameters".into()))?;
            crate::aggregate::Aggregation::from_thrift(a)?;
        }
        P::MATERIALIZATION_NODE => {
            let m = n.materialization_node.as_ref().ok_or_else(|| ExecError::BadRequest("MATERIALIZATION_NODE without its parameters".into()))?;
            check_exprs(m.fetch_expr_lists.iter().flatten(), false)?;
        }
        // const_expr_lists and first_materialized_child_idx are ignored, as C++ ignores them; the
        // tuple, types and distribution are checked in check_plan (check_sets)
        P::INTERSECT_NODE | P::EXCEPT_NODE => check_exprs(set_node(n)?.2.iter().flatten(), false)?,
        _ => return Err(unsupported(name)),
    }
    check_exprs(n.conjuncts.iter().flatten(), false)?;
    check_exprs(n.projections.iter().flatten(), false)?;
    Ok(())
}

/// An INTERSECT_NODE's or EXCEPT_NODE's kind, tuple and result expr lists (one per child, all of
/// one length).
pub fn set_node(n: &TPlanNode) -> Result<(crate::set_op::SetKind, i32, &[Vec<TExpr>]), ExecError> {
    use crate::set_op::SetKind;
    let name = node_type_name(n.node_type);
    let missing = || ExecError::BadRequest(format!("{name} without its parameters"));
    let (kind, tuple, lists) = match n.node_type {
        P::INTERSECT_NODE => n.intersect_node.as_ref().map(|s| (SetKind::Intersect, s.tuple_id, &s.result_expr_lists)).ok_or_else(missing)?,
        P::EXCEPT_NODE => n.except_node.as_ref().map(|s| (SetKind::Except, s.tuple_id, &s.result_expr_lists)).ok_or_else(missing)?,
        _ => return Err(ExecError::BadRequest(format!("{name} is not a set node"))),
    };
    if n.num_children < 2 {
        let what = if n.num_children == 1 { "child" } else { "children" };
        return Err(ExecError::BadRequest(format!("a set node with {} {what}; it needs at least 2", n.num_children)));
    }
    if lists.len() != n.num_children as usize {
        return Err(ExecError::BadRequest(format!("an {name} with {} children and {} result expr lists", n.num_children, lists.len())));
    }
    if lists.iter().any(|l| l.len() != lists[0].len()) {
        return Err(ExecError::BadRequest(format!("an {name} with result expr lists of different lengths")));
    }
    Ok((kind, tuple, lists))
}

/// The set nodes of a fragment's tree, against the plan:
/// - their result exprs fill the node's tuple, each in its slot's type. C++ probes every child
///   with child 0's hash method, so the types must match; only nullability may differ;
/// - in a fragment of several instances, equal rows must meet in one instance. Every child is
///   an exchange; child 0 is fed by hash-partitioned streams, and the others by hash-partitioned
///   or unpartitioned ones. doris-rs has no local shuffle, so a colocated node is refused.
fn check_sets(t: &PlanTree, f: &FragmentPlan, plan: &QueryPlan) -> Result<(), ExecError> {
    for c in &t.children {
        check_sets(c, f, plan)?;
    }
    if !matches!(t.node.node_type, P::INTERSECT_NODE | P::EXCEPT_NODE) {
        return Ok(());
    }
    let name = node_type_name(t.node.node_type);
    let (_, tuple, lists) = set_node(&t.node)?;
    let slots = plan.desc.tuple_slots(tuple)?;
    if lists[0].is_empty() {
        return Err(ExecError::BadRequest(format!("an {name} with no result exprs")));
    }
    if slots.len() != lists[0].len() {
        return Err(ExecError::BadRequest(format!(
            "an {name} with result expr lists of {} exprs for a tuple of {} slots",
            lists[0].len(),
            slots.len()
        )));
    }
    for list in lists {
        for (e, id) in list.iter().zip(slots) {
            let slot = &plan.desc.slot(*id)?.ty;
            let Some(ty) = e.nodes.first().and_then(|n| ColType::from_type_desc("", &n.type_, slot.nullable)) else {
                return Err(unsupported(format!("an {name} result expr of a type doris-rs does not run")));
            };
            if (ty.ptype, ty.precision, ty.scale) != (slot.ptype, slot.precision, slot.scale) {
                return Err(unsupported(format!(
                    "an {name} result expr of type {} for a slot of type {}",
                    ty.type_name(),
                    slot.type_name()
                )));
            }
        }
    }
    let n = f.instances.len();
    if n <= 1 {
        return Ok(());
    }
    let colocate = t.node.intersect_node.as_ref().and_then(|s| s.is_colocate).or(t.node.except_node.as_ref().and_then(|s| s.is_colocate));
    if colocate == Some(true) {
        return Err(unsupported(format!("an {name} colocated over {n} instances")));
    }
    for (i, (c, keys)) in t.children.iter().zip(lists).enumerate() {
        let keys = slot_ids(keys);
        match c.node.node_type {
            // a nested set node's rows are partitioned by its own keys: the parent's must be them
            P::INTERSECT_NODE | P::EXCEPT_NODE => {
                let (_, inner, _) = set_node(&c.node)?;
                if keys.as_deref() != Some(plan.desc.tuple_slots(inner)?) {
                    return Err(unsupported(format!("an {name} over a set node on other keys in {n} instances")));
                }
            }
            P::EXCHANGE_NODE => {
                for (p, exprs) in feeding_sinks(plan, c.node.node_id) {
                    let hashed = p == TPartitionType::HASH_PARTITIONED;
                    if !(hashed || (i > 0 && p == TPartitionType::UNPARTITIONED)) {
                        return Err(unsupported(format!("an {name} child fed by a {} stream in {n} instances", partition_name(p))));
                    }
                    if hashed && (keys.is_none() || slot_ids(exprs) != keys) {
                        return Err(unsupported(format!("an {name} child hashed on other exprs than its keys in {n} instances")));
                    }
                }
            }
            other => {
                let child = node_type_name(other);
                let a = if child.starts_with(['A', 'E', 'I', 'O', 'U']) { "an" } else { "a" };
                return Err(unsupported(format!("an {name} over {a} {child} in {n} instances")));
            }
        }
    }
    Ok(())
}

/// The slot ids of exprs that are all plain slot refs.
fn slot_ids(exprs: &[TExpr]) -> Option<Vec<i32>> {
    exprs
        .iter()
        .map(|e| match e.nodes.as_slice() {
            [n] if n.node_type == doris_thrift::gen::exprs::TExprNodeType::SLOT_REF => n.slot_ref.as_ref().map(|r| r.slot_id),
            _ => None,
        })
        .collect()
}

/// The partition type and exprs of every stream sink (or multicast consumer) that sends to
/// exchange `node`.
fn feeding_sinks(plan: &QueryPlan, node: i32) -> Vec<(TPartitionType, &[TExpr])> {
    let mut out = Vec::new();
    for g in &plan.fragments {
        let sinks: Vec<&TDataStreamSink> = match g.sink.type_ {
            TDataSinkType::DATA_STREAM_SINK => g.sink.stream_sink.iter().collect(),
            TDataSinkType::MULTI_CAST_DATA_STREAM_SINK => g.sink.multi_cast_stream_sink.iter().flat_map(|m| m.sinks.iter().flatten()).collect(),
            _ => Vec::new(),
        };
        for s in sinks.into_iter().filter(|s| s.dest_node_id == node) {
            out.push((s.output_partition.type_, s.output_partition.partition_exprs.as_deref().unwrap_or_default()));
        }
    }
    out
}

fn partition_name(p: TPartitionType) -> String {
    const NAMES: &[&str] = &["UNPARTITIONED", "RANDOM", "HASH_PARTITIONED", "RANGE_PARTITIONED", "LIST_PARTITIONED", "BUCKET_SHFFULE_HASH_PARTITIONED"];
    usize::try_from(p.0).ok().and_then(|i| NAMES.get(i)).map_or_else(|| format!("{p:?}"), |s| s.to_string())
}

/// How many of a fragment's stream sinks send to exchange `node` in instance `to`: its stream
/// sink, or each consumer of its multicast sink.
fn feeds(g: &FragmentPlan, node: i32, to: &TUniqueId) -> usize {
    let hits = |s: &TDataStreamSink, d: &[TPlanFragmentDestination]| s.dest_node_id == node && d.iter().any(|d| d.fragment_instance_id == *to);
    match g.sink.type_ {
        TDataSinkType::DATA_STREAM_SINK => g.sink.stream_sink.as_ref().is_some_and(|s| hits(s, &g.destinations)) as usize,
        TDataSinkType::MULTI_CAST_DATA_STREAM_SINK => g
            .sink
            .multi_cast_stream_sink
            .as_ref()
            .map(|m| m.sinks.iter().flatten().zip(m.destinations.iter().flatten()).filter(|(s, d)| hits(s, d)).count())
            .unwrap_or(0),
        _ => 0,
    }
}

/// Every exchange waits for as many senders as the plan has: each instance of a fragment whose
/// stream sink names the exchange and one of its instances among its destinations. A wrong count
/// would drop rows (too few) or wait forever (too many).
fn check_senders(plan: &QueryPlan) -> Result<(), ExecError> {
    for f in &plan.fragments {
        for n in f.root.nodes().into_iter().filter(|n| n.node_type == P::EXCHANGE_NODE) {
            let declared = f.per_exch_num_senders.get(&n.node_id).copied();
            for r in &f.instances {
                let senders: usize = plan.fragments.iter().map(|g| g.instances.len() * feeds(g, n.node_id, &r.id)).sum();
                if declared != Some(senders as i32) {
                    return Err(ExecError::BadRequest(format!(
                        "exchange node {} expects {declared:?} senders, the plan has {senders}",
                        n.node_id
                    )));
                }
            }
        }
    }
    Ok(())
}

/// A multicast sink's consumers and their destination lists, one each.
pub fn multicast_consumers(m: &TMultiCastDataStreamSink) -> Result<(&[TDataStreamSink], &[Vec<TPlanFragmentDestination>]), ExecError> {
    let (sinks, destinations) = (m.sinks.as_deref().unwrap_or_default(), m.destinations.as_deref().unwrap_or_default());
    if sinks.is_empty() || sinks.len() != destinations.len() {
        return Err(ExecError::BadRequest(format!(
            "a multicast sink with {} consumers and {} destination lists",
            sinks.len(),
            destinations.len()
        )));
    }
    Ok((sinks, destinations))
}

/// The tuple a stream sink projects onto, if it projects: only when it has output exprs, as
/// C++ (pipeline_fragment_context.cpp, the multicast row descriptor).
pub fn projection_tuple(s: &TDataStreamSink) -> Result<Option<i32>, ExecError> {
    match (s.output_exprs.as_ref().filter(|e| !e.is_empty()), s.output_tuple_id) {
        (None, _) => Ok(None),
        (Some(_), Some(t)) => Ok(Some(t)),
        (Some(_), None) => Err(ExecError::BadRequest("a stream sink with output exprs and no output tuple".into())),
    }
}

fn check_stream_sink(s: &TDataStreamSink, what: &str) -> Result<(), ExecError> {
    projection_tuple(s)?;
    check_partition(s.output_partition.type_, what)?;
    check_exprs(s.output_partition.partition_exprs.iter().flatten(), false)?;
    check_exprs(s.output_exprs.iter().flatten(), false)?;
    check_exprs(s.conjuncts.iter().flatten(), false)
}

/// Checks every fragment holds only what doris-rs runs; names the first thing it does not.
pub fn check_plan(plan: &QueryPlan) -> Result<(), ExecError> {
    if plan.binary_rows {
        return Err(unsupported("results in the MySQL binary row format"));
    }
    for f in &plan.fragments {
        for n in f.root.nodes() {
            check_node(n)?;
        }
        match f.sink.type_ {
            TDataSinkType::RESULT_SINK => check_exprs(&f.output_exprs, false)?,
            TDataSinkType::DATA_STREAM_SINK => {
                let s = f
                    .sink
                    .stream_sink
                    .as_ref()
                    .ok_or_else(|| ExecError::BadRequest("DATA_STREAM_SINK without its parameters".into()))?;
                check_stream_sink(s, "a DATA_STREAM_SINK")?;
            }
            TDataSinkType::MULTI_CAST_DATA_STREAM_SINK => {
                let m = f
                    .sink
                    .multi_cast_stream_sink
                    .as_ref()
                    .ok_or_else(|| ExecError::BadRequest("MULTI_CAST_DATA_STREAM_SINK without its parameters".into()))?;
                let (sinks, _) = multicast_consumers(m)?;
                for s in sinks {
                    check_stream_sink(s, "a multicast consumer")?;
                }
            }
            other => return Err(unsupported(sink_type_name(other))),
        }
    }
    for f in &plan.fragments {
        check_sets(&f.root, f, plan)?;
    }
    check_senders(plan)?;
    for s in plan.desc.slots.values() {
        if !s.ty.supported() && s.ty.ptype != doris_thrift::gen::types::TPrimitiveType::NULL_TYPE {
            return Err(unsupported(format!("type {} (slot {})", s.ty.type_name(), s.name)));
        }
    }
    Ok(())
}
