//! Query fragments as the FE sends them for a SELECT ([wire] §1): every fragment of the query in
//! one `TPipelineFragmentParamsList`, the receiving fragment first. Fragments marked
//! `is_simplified_param` share the first full fragment's descriptor table, globals and
//! coordinator. `check_plan` refuses, naming it, the first node, sink, expression or type
//! doris-rs does not run, so that nothing of a refused query runs.
use std::collections::{BTreeMap, HashMap};

use doris_thrift::gen::data_sinks::{TDataSink, TDataSinkType, TPlanFragmentDestination};
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
        _ => return Err(unsupported(name)),
    }
    check_exprs(n.conjuncts.iter().flatten(), false)?;
    check_exprs(n.projections.iter().flatten(), false)?;
    Ok(())
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
                check_partition(s.output_partition.type_, "a DATA_STREAM_SINK")?;
                check_exprs(s.output_partition.partition_exprs.iter().flatten(), false)?;
                check_exprs(s.output_exprs.iter().flatten(), false)?;
                check_exprs(s.conjuncts.iter().flatten(), false)?;
            }
            other => return Err(unsupported(sink_type_name(other))),
        }
    }
    for s in plan.desc.slots.values() {
        if !s.ty.supported() && s.ty.ptype != doris_thrift::gen::types::TPrimitiveType::NULL_TYPE {
            return Err(unsupported(format!("type {} (slot {})", s.ty.type_name(), s.name)));
        }
    }
    Ok(())
}
