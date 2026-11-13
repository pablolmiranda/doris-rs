//! Query fragments as the FE sends them for a SELECT ([wire] §1): every fragment of the query in
//! one `TPipelineFragmentParamsList`, the receiving fragment first. Fragments marked
//! `is_simplified_param` share the first full fragment's descriptor table, globals and
//! coordinator. `check_plan` refuses, naming it, the first node, sink, expression or type
//! doris-rs does not run, so that nothing of a refused query runs.
use std::collections::{BTreeMap, HashMap};

use doris_thrift::gen::data_sinks::{TDataSink, TDataSinkType, TDataStreamSink, TMultiCastDataStreamSink, TPlanFragmentDestination};
use doris_thrift::gen::descriptors::TDescriptorTable;
use doris_thrift::gen::exprs::{TExpr, TExprNodeType};
use doris_thrift::gen::palo_internal_service::TPipelineFragmentParamsList;
use doris_thrift::gen::partitions::TPartitionType;
use doris_thrift::gen::plan_nodes::{TPlanNode, TPlanNodeType as P, TSortInfo, TopNAlgorithm};
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
    /// A scan's virtual column (`TSlotDescriptor.virtual_column_expr`): computed from the row's
    /// other slots, not read (`SegmentIterator::_materialization_of_virtual_column`).
    pub virtual_expr: Option<TExpr>,
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
                    virtual_expr: s.virtual_column_expr.clone(),
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
    /// Data-gen scan node → its `numbers` ranges.
    pub data_gen: BTreeMap<i32, Vec<crate::datagen::Numbers>>,
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
    /// The fragment's OLAP_TABLE_SINK (`INSERT … SELECT`), checked.
    pub write: Option<crate::fragment::WriteFragment>,
    pub mode: RunMode,
}

/// How a fragment's instances run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunMode {
    /// Each instance over its own scan ranges and exchange input.
    Each,
    /// The FE planned a local shuffle (`parallel_instances` set, over several instances): it
    /// sends every exchange's rows to the first instance and spreads the scans, and C++ shuffles
    /// them between its tasks. doris-rs runs the first instance over every instance's scan ranges;
    /// the others only end their streams.
    Merged,
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
    /// Arithmetic under `check_overflow_for_decimal` false, whose decimal multiply rounds
    /// differently (`F/multiply.cpp:401-422`); the FE always sends true.
    pub unchecked_arithmetic: bool,
    /// The query's instant and time zone (`TQueryGlobals`), the same in every instance.
    pub clock: crate::clock::QueryClock,
    /// `enable_share_hash_table_for_broadcast_join`: a broadcast hash join's build input goes to
    /// the first instance of its fragment, and the others share it.
    pub share_broadcast_build: bool,
    /// `enable_extended_regex` and `enable_hyperscan_fallback`, read by `regexp`.
    pub regex: crate::regexp::RegexOptions,
}

impl QueryPlan {
    /// The fragment holding the result sink.
    pub fn result_fragment(&self) -> Option<&FragmentPlan> {
        self.fragments.iter().find(|f| f.sink.type_ == TDataSinkType::RESULT_SINK)
    }
}

pub fn plan_query(list: &TPipelineFragmentParamsList) -> Result<QueryPlan, ExecError> {
    // the load service's (exec_plan_fragment); its constants are not the query evaluator's
    if crate::fragment::is_load_plan(list) {
        return Err(unsupported("an INSERT … VALUES or a file scan sent as a query"));
    }
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
        let instances: Vec<InstancePlan> = p
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
                data_gen: l
                    .per_node_scan_ranges
                    .iter()
                    .map(|(node, ranges)| {
                        let r = ranges
                            .iter()
                            .filter_map(|r| r.scan_range.data_gen_scan_range.as_ref()?.numbers_params.as_ref())
                            .map(|n| crate::datagen::Numbers {
                                total: n.total_numbers.unwrap_or(0),
                                use_const: n.use_const == Some(true),
                                const_value: n.const_value.unwrap_or(0),
                            })
                            .collect::<Vec<_>>();
                        (*node, r)
                    })
                    .filter(|(_, r)| !r.is_empty())
                    .collect(),
            })
            .collect();
        // vnumbers_tvf.cpp reads only an instance's first range
        if instances.iter().any(|i| i.data_gen.values().any(|r| r.len() > 1)) {
            return Err(unsupported("a data-gen scan with several ranges in one instance"));
        }
        let write = if sink.type_ == TDataSinkType::OLAP_TABLE_SINK {
            // a later fragment of a prepare is simplified (is_simplified_param): it takes the
            // query's options and globals from the fragment that carries them
            let mut p = p.clone();
            p.query_options = p.query_options.or_else(|| full.query_options.clone());
            p.query_globals = p.query_globals.or_else(|| full.query_globals.clone());
            p.coord = p.coord.or_else(|| full.coord.clone());
            p.backend_id = p.backend_id.or(full.backend_id);
            Some(crate::fragment::table_sink(&p, crate::fragment::WritePlan::Select)?)
        } else {
            None
        };
        fragments.push(FragmentPlan {
            fragment_id: p.fragment_id.unwrap_or(0),
            root,
            sink,
            output_exprs: f.output_exprs.clone().unwrap_or_default(),
            per_exch_num_senders: p.per_exch_num_senders.clone(),
            destinations: p.destinations.clone().unwrap_or_default(),
            mode: if p.parallel_instances.is_some() && instances.len() > 1 { RunMode::Merged } else { RunMode::Each },
            instances,
            write,
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
        share_broadcast_build: o.and_then(|o| o.enable_share_hash_table_for_broadcast_join).unwrap_or(true),
        regex: crate::regexp::RegexOptions::from_query_options(o),
        clock: crate::clock::QueryClock::from_optional(params.iter().find_map(|p| p.query_globals.as_ref())),
        unchecked_arithmetic: o.and_then(|o| o.check_overflow_for_decimal) == Some(false)
            && format!("{list:?}").contains(&format!("node_type: {:?}", TExprNodeType::ARITHMETIC_EXPR)),
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

/// Keys of a sort, grouping, window, join, set operation or partition: no order or hash of an
/// ARRAY is ported, so one is refused.
/// Refuses an ARRAY, MAP or STRUCT among `exprs`, which are used as `what` (a sort, grouping,
/// join, window or partition key): doris-rs neither compares nor hashes nested values as C++ does.
pub fn check_keys<'a>(exprs: impl IntoIterator<Item = &'a TExpr>, what: &str) -> Result<(), ExecError> {
    for e in exprs {
        let t = e.nodes.first().and_then(|n| ColType::from_type_desc("", &n.type_, true));
        if let Some(t) = t.filter(ColType::is_nested) {
            return Err(unsupported(format!("{} as {what}", t.nested_name())));
        }
    }
    Ok(())
}

fn check_partition(t: TPartitionType, what: &str) -> Result<(), ExecError> {
    use TPartitionType as T;
    if matches!(t, T::UNPARTITIONED | T::HASH_PARTITIONED | T::RANDOM | T::BUCKET_SHFFULE_HASH_PARTITIONED | T::OLAP_TABLE_SINK_HASH_PARTITIONED) {
        Ok(())
    } else {
        Err(unsupported(format!("{what} partitioned as {t:?}")))
    }
}

fn check_node(n: &TPlanNode) -> Result<(), ExecError> {
    let name = node_type_name(n.node_type);
    match n.node_type {
        P::OLAP_SCAN_NODE | P::EMPTY_SET_NODE | P::SELECT_NODE => {}
        P::TABLE_FUNCTION_NODE => {
            let t = n.table_function_node.as_ref().ok_or_else(|| ExecError::BadRequest("TABLE_FUNCTION_NODE without its parameters".into()))?;
            crate::table_function::check(t)?;
        }
        P::REPEAT_NODE => {
            let r = n.repeat_node.as_ref().ok_or_else(|| ExecError::BadRequest("REPEAT_NODE without its parameters".into()))?;
            crate::repeat::check_repeat(r)?;
            check_exprs(&r.exprs, false)?;
        }
        P::DATA_GEN_SCAN_NODE => {
            use doris_thrift::gen::plan_nodes::TDataGenFunctionName;
            let d = n.data_gen_scan_node.as_ref().ok_or_else(|| ExecError::BadRequest("DATA_GEN_SCAN_NODE without its parameters".into()))?;
            if d.func_name != Some(TDataGenFunctionName::NUMBERS) {
                return Err(unsupported("a data-gen scan other than numbers"));
            }
        }
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
                check_keys(&info.ordering_exprs, "a sort key")?;
            }
            check_partition(e.partition_type.unwrap_or(TPartitionType::UNPARTITIONED), "an EXCHANGE_NODE")?;
        }
        P::SORT_NODE => {
            let s = n.sort_node.as_ref().ok_or_else(|| ExecError::BadRequest("SORT_NODE without its parameters".into()))?;
            if s.sort_info.use_two_phase_read == Some(true) {
                return Err(unsupported("a SORT_NODE with the two-phase read"));
            }
            check_exprs(&s.sort_info.ordering_exprs, false)?;
            check_keys(&s.sort_info.ordering_exprs, "a sort key")?;
            check_exprs(s.sort_info.sort_tuple_slot_exprs.iter().flatten(), false)?;
        }
        P::SCHEMA_SCAN_NODE => {
            let s = n.schema_scan_node.as_ref().ok_or_else(|| ExecError::BadRequest("SCHEMA_SCAN_NODE without its parameters".into()))?;
            crate::schema_scan::check_schema_table(&s.table_name)?;
        }
        P::AGGREGATION_NODE => {
            let a = n.agg_node.as_ref().ok_or_else(|| ExecError::BadRequest("AGGREGATION_NODE without its parameters".into()))?;
            crate::aggregate::Aggregation::from_thrift(a)?;
            check_keys(a.grouping_exprs.iter().flatten(), "a grouping key")?;
        }
        P::ANALYTIC_EVAL_NODE => {
            let a = n.analytic_node.as_ref().ok_or_else(|| ExecError::BadRequest("ANALYTIC_EVAL_NODE without its parameters".into()))?;
            let frame = crate::analytic::frame(a.window.as_ref())?;
            check_exprs(a.partition_exprs.iter().chain(&a.order_by_exprs), false)?;
            check_keys(a.partition_exprs.iter().chain(&a.order_by_exprs), "a window key")?;
            for f in &a.analytic_functions {
                crate::analytic::check_window(&crate::analytic::WindowFn::from_expr(f)?, &frame, !a.order_by_exprs.is_empty())?;
            }
        }
        P::PARTITION_SORT_NODE => {
            let (p, info, _, _) = partition_sort_node(n)?;
            check_exprs(p.iter().chain(&info.ordering_exprs).chain(info.sort_tuple_slot_exprs.iter().flatten()), false)?;
            check_keys(p.iter().chain(&info.ordering_exprs), "a sort key")?;
        }
        P::MATERIALIZATION_NODE => {
            let m = n.materialization_node.as_ref().ok_or_else(|| ExecError::BadRequest("MATERIALIZATION_NODE without its parameters".into()))?;
            check_exprs(m.fetch_expr_lists.iter().flatten(), false)?;
        }
        // const_expr_lists and first_materialized_child_idx are ignored, as C++ ignores them; the
        // tuple, types and distribution are checked in check_plan (check_sets)
        P::INTERSECT_NODE | P::EXCEPT_NODE => {
            check_exprs(set_node(n)?.2.iter().flatten(), false)?;
            check_keys(set_node(n)?.2.iter().flatten(), "a set operation's column")?;
        }
        P::HASH_JOIN_NODE => check_hash_join(n)?,
        P::CROSS_JOIN_NODE => {
            use crate::hash_join::{join_op, JoinOp};
            let j = n.nested_loop_join_node.as_ref().ok_or_else(|| ExecError::BadRequest("CROSS_JOIN_NODE without its parameters".into()))?;
            let op = join_op(j.join_op);
            let mark = j.is_mark == Some(true);
            let runs = match op {
                Some(JoinOp::LeftSemi | JoinOp::LeftAnti | JoinOp::NullAwareLeftAnti) => true,
                Some(JoinOp::Inner | JoinOp::Cross | JoinOp::LeftOuter | JoinOp::RightOuter | JoinOp::FullOuter) => !mark,
                _ => false,
            };
            if !runs {
                return Err(unsupported(format!("a CROSS_JOIN_NODE with join op {}{}", join_op_name(j.join_op), if mark { " (mark)" } else { "" })));
            }
            if j.vintermediate_tuple_id_list.as_ref().is_none_or(Vec::is_empty) {
                return Err(ExecError::BadRequest("a CROSS_JOIN_NODE without its intermediate tuples".into()));
            }
            check_exprs(j.join_conjuncts.iter().flatten().chain(j.vjoin_conjunct.iter()), false)?;
        }
        P::ASSERT_NUM_ROWS_NODE => {
            use doris_thrift::gen::plan_nodes::TAssertion;
            let a = n.assert_num_rows_node.as_ref().ok_or_else(|| ExecError::BadRequest("ASSERT_NUM_ROWS_NODE without its parameters".into()))?;
            if a.assertion.is_some_and(|x| !(0..=5).contains(&x.0)) {
                return Err(unsupported(format!("an ASSERT_NUM_ROWS_NODE assertion {:?}", a.assertion.unwrap_or(TAssertion::LE))));
            }
        }
        _ => return Err(unsupported(name)),
    }
    check_exprs(n.conjuncts.iter().flatten(), false)?;
    // `OperatorXBase::init`: one tuple per intermediate projection list, and final projections
    let stages = n.intermediate_projections_list.as_ref().map_or(0, Vec::len);
    if stages > 0 && (n.intermediate_output_tuple_id_list.as_ref().map_or(0, Vec::len) != stages || n.projections.is_none() || n.output_tuple_id.is_none()) {
        return Err(ExecError::BadRequest("intermediate projections without their tuples or final projections".into()));
    }
    check_exprs(n.intermediate_projections_list.iter().flatten().flatten(), false)?;
    check_exprs(n.projections.iter().flatten(), false)?;
    Ok(())
}

/// Whether two key types hash and compare alike: equal up to nullability, every string type
/// being one (`DataTypeString`), as `hashjoin_build_sink.cpp:734-746` requires.
fn same_key_type(a: &ColType, b: &ColType) -> bool {
    use doris_thrift::gen::types::TPrimitiveType as T;
    let string = |t: &ColType| matches!(t.ptype, T::CHAR | T::VARCHAR | T::STRING);
    (string(a) && string(b)) || (a.ptype, a.precision, a.scale) == (b.ptype, b.precision, b.scale)
}

/// HASH_JOIN_NODE: an op doris-rs runs, eq keys of one type on both sides, and exprs it evaluates.
fn check_hash_join(n: &TPlanNode) -> Result<(), ExecError> {
    use crate::hash_join::{join_op, JoinOp};
    let h = n.hash_join_node.as_ref().ok_or_else(|| ExecError::BadRequest("HASH_JOIN_NODE without its parameters".into()))?;
    let op = join_op(h.join_op).ok_or_else(|| unsupported(format!("a HASH_JOIN_NODE with join op {}", join_op_name(h.join_op))))?;
    let mark = h.is_mark == Some(true);
    match op {
        JoinOp::Cross => return Err(unsupported("a HASH_JOIN_NODE with join op CROSS_JOIN")),
        JoinOp::NullAwareLeftSemi if !mark => return Err(unsupported("a NULL_AWARE_LEFT_SEMI_JOIN that is not a mark join")),
        JoinOp::LeftSemi | JoinOp::LeftAnti | JoinOp::NullAwareLeftAnti | JoinOp::NullAwareLeftSemi | JoinOp::RightSemi => {}
        _ if mark => return Err(unsupported(format!("a mark join with join op {}", join_op_name(h.join_op)))),
        _ => {}
    }
    if matches!(op, JoinOp::AsofLeftInner | JoinOp::AsofLeftOuter) {
        let m = h.match_condition.as_ref().ok_or_else(|| ExecError::BadRequest("an ASOF join without its match condition".into()))?;
        crate::hash_join::check_asof_condition(m)?;
        let (_, (p, b)) = crate::hash_join::asof_condition(m)?;
        check_exprs([&p, &b], false)?;
    }
    if h.vintermediate_tuple_id_list.as_ref().is_none_or(Vec::is_empty) {
        return Err(ExecError::BadRequest("a HASH_JOIN_NODE without its intermediate tuples".into()));
    }
    for e in &h.eq_join_conjuncts {
        use doris_thrift::gen::opcodes::TExprOpcode;
        check_keys([&e.left, &e.right], "a join key")?;
        if !matches!(e.opcode, None | Some(TExprOpcode::EQ) | Some(TExprOpcode::EQ_FOR_NULL)) {
            return Err(unsupported(format!("a hash join key compared by {:?}", e.opcode)));
        }
        let ty = |x: &TExpr| x.nodes.first().and_then(|n| ColType::from_type_desc("", &n.type_, true));
        match (ty(&e.left), ty(&e.right)) {
            (Some(a), Some(b)) if same_key_type(&a, &b) => {}
            (Some(a), Some(b)) => return Err(unsupported(format!("hash join keys of types {} and {}", a.type_name(), b.type_name()))),
            _ => return Err(unsupported("a hash join key of a nested type")),
        }
        check_exprs([&e.left, &e.right], false)?;
    }
    check_exprs(h.other_join_conjuncts.iter().flatten().chain(h.mark_join_conjuncts.iter().flatten()), false)
}

/// A join op's Thrift name.
pub fn join_op_name(op: doris_thrift::gen::plan_nodes::TJoinOp) -> String {
    const NAMES: &[&str] = &[
        "INNER_JOIN",
        "LEFT_OUTER_JOIN",
        "LEFT_SEMI_JOIN",
        "RIGHT_OUTER_JOIN",
        "FULL_OUTER_JOIN",
        "CROSS_JOIN",
        "MERGE_JOIN",
        "RIGHT_SEMI_JOIN",
        "LEFT_ANTI_JOIN",
        "RIGHT_ANTI_JOIN",
        "NULL_AWARE_LEFT_ANTI_JOIN",
        "NULL_AWARE_LEFT_SEMI_JOIN",
        "ASOF_LEFT_INNER_JOIN",
        "ASOF_RIGHT_INNER_JOIN",
        "ASOF_LEFT_OUTER_JOIN",
        "ASOF_RIGHT_OUTER_JOIN",
    ];
    usize::try_from(op.0).ok().and_then(|i| NAMES.get(i)).map_or_else(|| format!("{op:?}"), |s| s.to_string())
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

/// Which instance a subtree's rows are in, within a fragment of several instances.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Dist {
    /// Hashed by doris-rs's own shuffle (HASH_PARTITIONED exchanges).
    Hashed,
    /// In the instance holding the row's storage bucket: the instance's own scans, or a
    /// bucket shuffle to them.
    Bucketed,
    /// The same rows in every instance (UNPARTITIONED exchanges).
    Whole,
    /// No rows from elsewhere (an empty set, constants).
    Neutral,
    /// Anything else: round robin, or sides that disagree.
    Other,
}

/// The distribution of a subtree's rows: an exchange's is its sinks', a scan's its buckets'; a
/// broadcast or nested-loop join keeps its probe side's, a shuffle join is hashed, and a
/// bucket-shuffle or colocate join bucketed.
fn dist(t: &PlanTree, plan: &QueryPlan) -> Dist {
    use doris_thrift::gen::plan_nodes::TJoinDistributionType as D;
    match t.node.node_type {
        P::EXCHANGE_NODE => {
            let kinds: Vec<TPartitionType> = feeding_sinks(plan, t.node.node_id).into_iter().map(|(p, _)| p).collect();
            match kinds.first().copied().filter(|k| kinds.iter().all(|x| x == k)) {
                Some(TPartitionType::HASH_PARTITIONED) => Dist::Hashed,
                Some(TPartitionType::BUCKET_SHFFULE_HASH_PARTITIONED) => Dist::Bucketed,
                Some(TPartitionType::UNPARTITIONED) => Dist::Whole,
                _ => Dist::Other,
            }
        }
        P::OLAP_SCAN_NODE => Dist::Bucketed,
        P::HASH_JOIN_NODE => match t.node.hash_join_node.as_ref().and_then(|h| h.dist_type) {
            Some(D::PARTITIONED) => Dist::Hashed,
            Some(D::BUCKET_SHUFFLE | D::COLOCATE) => Dist::Bucketed,
            _ => t.children.first().map_or(Dist::Other, |c| dist(c, plan)),
        },
        P::CROSS_JOIN_NODE => t.children.first().map_or(Dist::Other, |c| dist(c, plan)),
        _ if t.children.is_empty() => {
            if matches!(t.node.node_type, P::EMPTY_SET_NODE | P::UNION_NODE) {
                Dist::Neutral
            } else {
                Dist::Other
            }
        }
        _ => {
            let kids: Vec<Dist> = t.children.iter().map(|c| dist(c, plan)).filter(|d| *d != Dist::Neutral).collect();
            match kids.first() {
                None => Dist::Neutral,
                Some(d) if kids.iter().all(|x| x == d) => *d,
                _ => Dist::Other,
            }
        }
    }
}

/// The joins of a fragment of several instances that each run their own rows, against the plan:
/// rows with equal keys must meet in one instance, or a match is lost.
/// - BROADCAST: the build side is whole in each instance;
/// - PARTITIONED: both sides are hashed by doris-rs's shuffle, the same hash for both (the FE
///   casts the keys to one type); a side that is an exchange is hashed on its own keys;
/// - BUCKET_SHUFFLE and COLOCATE: both sides are in the instance of their storage bucket.
///
/// A nested-loop join takes its build side whole in each instance, and cannot emit the build
/// rows no instance matched (RIGHT and FULL OUTER), so those run in one instance only.
fn check_joins(t: &PlanTree, f: &FragmentPlan, plan: &QueryPlan) -> Result<(), ExecError> {
    use doris_thrift::gen::plan_nodes::TJoinDistributionType as D;
    for c in &t.children {
        check_joins(c, f, plan)?;
    }
    let n = f.instances.len();
    if n <= 1 || f.mode == RunMode::Merged {
        return Ok(());
    }
    let child = |i: usize| t.children.get(i).ok_or_else(|| ExecError::BadRequest(format!("join node {} without child {i}", t.node.node_id)));
    // whole: every exchange of the side reaches every instance (no shared build for these)
    let whole_everywhere = |c: &PlanTree| {
        fn exchange_ids(t: &PlanTree, out: &mut Vec<i32>) {
            if t.node.node_type == P::EXCHANGE_NODE {
                out.push(t.node.node_id);
            }
            t.children.iter().for_each(|c| exchange_ids(c, out));
        }
        let mut ids = Vec::new();
        exchange_ids(c, &mut ids);
        dist(c, plan) == Dist::Whole && ids.iter().all(|e| f.instances.iter().all(|i| plan_senders(plan, *e, &i.id) > 0))
    };
    match t.node.node_type {
        P::HASH_JOIN_NODE => {
            let h = t.node.hash_join_node.as_ref().ok_or_else(|| ExecError::BadRequest("HASH_JOIN_NODE without its parameters".into()))?;
            let (probe, build) = (child(0)?, child(1)?);
            // an exchange child is hashed on some of its side's keys
            let on_keys = |c: &PlanTree, left: bool| {
                if c.node.node_type != P::EXCHANGE_NODE {
                    return true;
                }
                let keys: Vec<TExpr> = h.eq_join_conjuncts.iter().map(|e| if left { e.left.clone() } else { e.right.clone() }).collect();
                let Some(keys) = slot_ids(&keys) else { return false };
                feeding_sinks(plan, c.node.node_id)
                    .iter()
                    .all(|(_, exprs)| slot_ids(exprs).is_some_and(|s| !s.is_empty() && s.iter().all(|x| keys.contains(x))))
            };
            let dist_type = h.dist_type.unwrap_or(D::NONE);
            let ok = match dist_type {
                // a shared build reaches the first instance, which forwards it (`shared_build`)
                D::BROADCAST => dist(build, plan) == Dist::Whole,
                D::PARTITIONED => dist(probe, plan) == Dist::Hashed && dist(build, plan) == Dist::Hashed && on_keys(probe, true) && on_keys(build, false),
                D::BUCKET_SHUFFLE | D::COLOCATE => dist(probe, plan) == Dist::Bucketed && dist(build, plan) == Dist::Bucketed,
                _ => false,
            };
            if !ok {
                const NAMES: &[&str] = &["NONE", "BROADCAST", "PARTITIONED", "BUCKET_SHUFFLE", "COLOCATE"];
                let name = usize::try_from(dist_type.0).ok().and_then(|i| NAMES.get(i)).map_or_else(|| format!("{dist_type:?}"), |s| s.to_string());
                return Err(unsupported(format!("a {name} join whose inputs are not distributed on its keys in {n} instances")));
            }
        }
        P::CROSS_JOIN_NODE => {
            use crate::hash_join::{join_op, JoinOp};
            let j = t.node.nested_loop_join_node.as_ref().ok_or_else(|| ExecError::BadRequest("CROSS_JOIN_NODE without its parameters".into()))?;
            if matches!(join_op(j.join_op), Some(JoinOp::RightOuter | JoinOp::FullOuter)) || !whole_everywhere(child(1)?) {
                return Err(unsupported(format!(
                    "a CROSS_JOIN_NODE {} whose build side is not whole in each of {n} instances",
                    join_op_name(j.join_op)
                )));
            }
        }
        _ => {}
    }
    Ok(())
}

/// A PARTITION_SORT_NODE's partition exprs, sort, algorithm and per-partition limit. With
/// `has_global_limit` the algorithm is ROW_NUMBER, as in C++.
pub fn partition_sort_node(n: &TPlanNode) -> Result<(&[TExpr], &TSortInfo, TopNAlgorithm, usize), ExecError> {
    let p = n.partition_sort_node.as_ref().ok_or_else(|| ExecError::BadRequest("PARTITION_SORT_NODE without its parameters".into()))?;
    let info = p.sort_info.as_ref().ok_or_else(|| ExecError::BadRequest("a PARTITION_SORT_NODE without its sort".into()))?;
    let limit = p
        .partition_inner_limit
        .filter(|l| *l >= 0)
        .ok_or_else(|| ExecError::BadRequest("a PARTITION_SORT_NODE without a limit of 0 or more".into()))?;
    let algo = if p.has_global_limit == Some(true) {
        TopNAlgorithm::ROW_NUMBER
    } else {
        p.top_n_algorithm.ok_or_else(|| ExecError::BadRequest("a PARTITION_SORT_NODE without its algorithm".into()))?
    };
    Ok((p.partition_exprs.as_deref().unwrap_or_default(), info, algo, limit as usize))
}

/// ANALYTIC_EVAL_NODE: one function per slot of its output tuple. In several instances, the
/// rows of a partition must meet in one: the node is colocated (a bucket's rows are in one
/// instance), or its input is hashed on a subset of its partition keys. A node with no
/// partition keys runs as one instance (the FE makes it serial).
fn check_windows(t: &PlanTree, f: &FragmentPlan, plan: &QueryPlan) -> Result<(), ExecError> {
    for c in &t.children {
        check_windows(c, f, plan)?;
    }
    // a global phase's output feeds the window with no sort between, so its partitions' order
    // must be C++'s (each block's rows taken last to first), which doris-rs does not reproduce;
    // no eligible suite has one
    if let Some(p) = t.node.partition_sort_node.as_ref().filter(|_| t.node.node_type == P::PARTITION_SORT_NODE) {
        if p.ptopn_phase == Some(doris_thrift::gen::plan_nodes::TPartTopNPhase::TWO_PHASE_GLOBAL) {
            return Err(unsupported("a global PARTITION_SORT_NODE".to_string()));
        }
    }
    let Some(a) = t.node.analytic_node.as_ref().filter(|_| t.node.node_type == P::ANALYTIC_EVAL_NODE) else {
        return Ok(());
    };
    let name = "ANALYTIC_EVAL_NODE";
    let slots = plan.desc.tuple_slots(a.output_tuple_id)?;
    if slots.len() != a.analytic_functions.len() {
        return Err(ExecError::BadRequest(format!(
            "an {name} with {} functions for a tuple of {} slots",
            a.analytic_functions.len(),
            slots.len()
        )));
    }
    let n = f.instances.len();
    if n <= 1 {
        return Ok(());
    }
    // with no partition keys the node is serial in C++, colocated or not
    if a.partition_exprs.is_empty() {
        return Err(unsupported(format!("an {name} without partition keys in {n} instances")));
    }
    if a.is_colocate == Some(true) {
        return Ok(());
    }
    let keys = slot_ids(&a.partition_exprs).ok_or_else(|| unsupported(format!("an {name} with partition keys that are not slots in {n} instances")))?;
    let child = t.children.first().ok_or_else(|| ExecError::BadRequest(format!("an {name} without its child")))?;
    window_input(child, keys, plan, n)
}

/// Walks a window's input down to the exchange that feeds it, carrying the partition keys'
/// slots: a sort maps its tuple's slots to its input's, and a window passes its input's through.
fn window_input(t: &PlanTree, keys: Vec<i32>, plan: &QueryPlan, n: usize) -> Result<(), ExecError> {
    let name = "ANALYTIC_EVAL_NODE";
    let next = || t.children.first().ok_or_else(|| ExecError::BadRequest(format!("node {} without its child", t.node.node_id)));
    match t.node.node_type {
        P::ANALYTIC_EVAL_NODE => window_input(next()?, keys, plan, n),
        P::SORT_NODE => {
            let s = t.node.sort_node.as_ref().ok_or_else(|| ExecError::BadRequest("SORT_NODE without its parameters".into()))?;
            let tuple = plan.desc.tuple_slots(*t.node.row_tuples.first().ok_or_else(|| ExecError::BadRequest("a SORT_NODE without its tuple".into()))?)?;
            let child = next()?;
            // without slot exprs, the sort tuple takes its input's columns by position
            let from: Vec<Option<i32>> = match &s.sort_info.sort_tuple_slot_exprs {
                Some(exprs) => exprs.iter().map(|e| slot_ids(std::slice::from_ref(e)).map(|v| v[0])).collect(),
                None => output_slots(child, plan)?.into_iter().map(Some).collect(),
            };
            let mapped = keys
                .iter()
                .map(|k| tuple.iter().position(|s| s == k).and_then(|i| from.get(i).copied().flatten()))
                .collect::<Option<Vec<i32>>>()
                .ok_or_else(|| unsupported(format!("an {name} over a sort it can't see its partition keys through in {n} instances")))?;
            window_input(child, mapped, plan, n)
        }
        // a grouping aggregation: its output's grouping slots are its grouping exprs' input
        // slots, and each group is in the instance its input's hash sends it to
        P::AGGREGATION_NODE => {
            let a = t.node.agg_node.as_ref().ok_or_else(|| ExecError::BadRequest("AGGREGATION_NODE without its parameters".into()))?;
            // a projection on the node first: a key must be a projected slot of the aggregation's own
            let keys = match (&t.node.projections, t.node.output_tuple_id) {
                (Some(p), Some(out)) => {
                    let projected = plan.desc.tuple_slots(out)?;
                    keys.iter()
                        .map(|k| projected.iter().position(|s| s == k).and_then(|i| p.get(i)).and_then(|e| slot_ids(std::slice::from_ref(e)).map(|v| v[0])))
                        .collect::<Option<Vec<i32>>>()
                        .ok_or_else(|| unsupported(format!("an {name} over an aggregation it can't see its partition keys through in {n} instances")))?
                }
                _ => keys,
            };
            let tuple = plan.desc.tuple_slots(a.output_tuple_id)?;
            let group = a.grouping_exprs.as_deref().unwrap_or_default();
            let mapped = keys
                .iter()
                .map(|k| tuple.iter().position(|s| s == k).filter(|i| *i < group.len()).and_then(|i| slot_ids(std::slice::from_ref(&group[i])).map(|v| v[0])))
                .collect::<Option<Vec<i32>>>()
                .ok_or_else(|| unsupported(format!("an {name} over an aggregation it can't see its partition keys through in {n} instances")))?;
            window_input(next()?, mapped, plan, n)
        }
        P::EXCHANGE_NODE => {
            for (p, exprs) in feeding_sinks(plan, t.node.node_id) {
                if p != TPartitionType::HASH_PARTITIONED {
                    return Err(unsupported(format!("an {name} fed by a {} stream in {n} instances", partition_name(p))));
                }
                match slot_ids(exprs) {
                    Some(h) if !h.is_empty() && h.iter().all(|s| keys.contains(s)) => {}
                    _ => return Err(unsupported(format!("an {name} hashed on other exprs than its partition keys in {n} instances"))),
                }
            }
            Ok(())
        }
        other => {
            let child = node_type_name(other);
            let a = if child.starts_with(['A', 'E', 'I', 'O', 'U']) { "an" } else { "a" };
            Err(unsupported(format!("an {name} over {a} {child} in {n} instances")))
        }
    }
}

/// A node's output slots: its projection's tuple, or the slots of its row tuples, in order.
fn output_slots(t: &PlanTree, plan: &QueryPlan) -> Result<Vec<i32>, ExecError> {
    // a projected node outputs its projection's tuple
    if let (Some(_), Some(out)) = (&t.node.projections, t.node.output_tuple_id) {
        return Ok(plan.desc.tuple_slots(out)?.to_vec());
    }
    let mut out = Vec::new();
    for tuple in &t.node.row_tuples {
        out.extend_from_slice(plan.desc.tuple_slots(*tuple)?);
    }
    Ok(out)
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

/// How many sender instances name instance `to` for exchange `node`.
pub fn plan_senders(plan: &QueryPlan, node: i32, to: &TUniqueId) -> usize {
    plan.fragments.iter().map(|g| g.instances.len() * feeds(g, node, to)).sum()
}

/// Whether exchange `node` of fragment `fragment` is the build input of a broadcast hash join
/// whose table the fragment's instances share (`hashjoin_build_sink.cpp:58-76`): the FE sends it
/// to the first instance only (`DistributePlanner.java:253-254`).
pub fn shared_build(plan: &QueryPlan, fragment: usize, node: i32) -> bool {
    fn find(t: &PlanTree, node: i32) -> bool {
        let here = t.node.node_type == P::HASH_JOIN_NODE
            && t.node.hash_join_node.as_ref().is_some_and(|h| h.is_broadcast_join == Some(true))
            && t.children.get(1).is_some_and(|c| c.node.node_id == node);
        here || t.children.iter().any(|c| find(c, node))
    }
    plan.share_broadcast_build && plan.fragments.get(fragment).is_some_and(|f| find(&f.root, node))
}

/// Every exchange waits for as many senders as the plan has: each instance of a fragment whose
/// stream sink names the exchange and one of its instances among its destinations. A wrong count
/// would drop rows (too few) or wait forever (too many). A merged fragment's other instances
/// receive nothing, nor does a shared broadcast build's (the first instance forwards it).
fn check_senders(plan: &QueryPlan) -> Result<(), ExecError> {
    for (fi, f) in plan.fragments.iter().enumerate() {
        for n in f.root.nodes().into_iter().filter(|n| n.node_type == P::EXCHANGE_NODE) {
            let declared = f.per_exch_num_senders.get(&n.node_id).copied();
            let shared = shared_build(plan, fi, n.node_id);
            for (ii, r) in f.instances.iter().enumerate() {
                let senders = plan_senders(plan, n.node_id, &r.id);
                // a merged fragment's other instances run nothing: rows sent to them would be lost
                if ii > 0 && f.mode == RunMode::Merged {
                    if senders > 0 {
                        return Err(unsupported(format!(
                            "exchange node {} sending to instance {ii} of a merged fragment (parallel_instances)",
                            n.node_id
                        )));
                    }
                    continue;
                }
                if ii > 0 && shared && senders == 0 {
                    continue;
                }
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
    check_keys(s.output_partition.partition_exprs.iter().flatten(), "a partition key")?;
    check_exprs(s.output_exprs.iter().flatten(), false)?;
    check_exprs(s.conjuncts.iter().flatten(), false)
}

/// Checks every fragment holds only what doris-rs runs; names the first thing it does not.
pub fn check_plan(plan: &QueryPlan) -> Result<(), ExecError> {
    // constants are evaluated at prepare under the query's options and clock
    crate::clock::with_clock(&plan.clock, || crate::regexp::with_regex_options(plan.regex, || check_plan_inner(plan)))
}

fn check_plan_inner(plan: &QueryPlan) -> Result<(), ExecError> {
    if plan.binary_rows {
        return Err(unsupported("results in the MySQL binary row format"));
    }
    if plan.unchecked_arithmetic {
        return Err(unsupported("arithmetic with check_overflow_for_decimal off"));
    }
    check_exprs(plan.desc.slots.values().filter_map(|s| s.virtual_expr.as_ref()), false)?;
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
            // its sink checked when planned (`fragment::table_sink`)
            TDataSinkType::OLAP_TABLE_SINK if f.write.is_some() => check_exprs(&f.output_exprs, false)?,
            other => return Err(unsupported(sink_type_name(other))),
        }
    }
    for f in &plan.fragments {
        check_sets(&f.root, f, plan)?;
        check_windows(&f.root, f, plan)?;
        check_joins(&f.root, f, plan)?;
    }
    check_senders(plan)?;
    for s in plan.desc.slots.values() {
        // an ARRAY, MAP or STRUCT is carried (literals, functions, exchanges), never read from storage
        if !s.ty.supported() && !s.ty.supported_nested() && s.ty.ptype != doris_thrift::gen::types::TPrimitiveType::NULL_TYPE {
            return Err(unsupported(format!("type {} (slot {})", s.ty.type_name(), s.name)));
        }
    }
    for f in &plan.fragments {
        for n in f.root.nodes() {
            let Some(scan) = n.olap_scan_node.as_ref().filter(|_| n.node_type == P::OLAP_SCAN_NODE) else { continue };
            if let Some(s) = plan.desc.slots.values().find(|s| s.tuple == scan.tuple_id && s.ty.is_nested()) {
                return Err(unsupported(format!("reading {} column ({})", s.ty.nested_name(), s.name)));
            }
        }
    }
    Ok(())
}
