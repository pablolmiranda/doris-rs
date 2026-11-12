//! The query replays sampled from the traffic corpus (`crates/doris-fixtures/replays`).
use std::collections::BTreeSet;

use doris_fixtures::replays::{self, Step};

const SYS_TABLES: &str = "nereids_p0.system.test_query_sys_tables.test_query_sys_tables";

#[test]
fn every_replay_loads() {
    let all = replays::all();
    assert!(all.len() >= 40, "only {} replays", all.len());
    for r in &all {
        let eos = r
            .steps
            .iter()
            .any(|s| matches!(s, Step::Fetch { reply, .. } if reply.eos == Some(true)));
        assert!(eos, "{}: no fetch ends with eos", r.suite);
    }
}

#[test]
fn shapes_are_covered() {
    let all = replays::all();
    let mut shapes = BTreeSet::new();
    let mut functions = BTreeSet::new();
    for r in &all {
        for q in r.queries() {
            if let Some(c) = replays::shape_letter(&replays::shape(&q)) {
                shapes.insert(c);
            }
            functions.extend(replays::functions(&q));
        }
    }
    // J (SORT(AGG(OLAP_SCAN))) occurs in no replayable suite; Task 7's unit tests and the full run cover it
    for c in ['A', 'B', 'C', 'D', 'E', 'F', 'H', 'I', 'K'] {
        assert!(
            shapes.contains(&c),
            "shape {c} not covered; have {shapes:?}"
        );
    }
    for f in [
        "eq",
        "ne",
        "lt",
        "gt",
        "le",
        "ge",
        "is_null_pred",
        "is_not_null_pred",
        "substring",
        "count",
        "sum",
        "min",
        "max",
        "avg",
        "multi_distinct_count",
    ] {
        assert!(
            functions.contains(f),
            "function {f} not covered; have {functions:?}"
        );
    }
}

#[test]
fn sys_tables_stops_before_the_set() {
    let r = replays::load(SYS_TABLES);
    let queries = r.result_queries();
    assert_eq!(queries.len(), 6, "result queries: {queries:?}");
    // the last step is the eos fetch of the sixth (session_variables) query
    match r.steps.last() {
        Some(Step::Fetch { reply, .. }) => assert_eq!(reply.eos, Some(true)),
        other => panic!("last step is not a fetch: {other:?}"),
    }
}

const CTE: &str = "correctness_p0.test_subquery_grouping.test_subquery_grouping";

#[test]
fn a_cte_replay_has_a_multicast_sink() {
    use doris_thrift::gen::data_sinks::TDataSinkType;
    let r = replays::load(CTE);
    let multicast = r.queries().iter().flat_map(|q| q.params_list.clone().unwrap_or_default()).any(|p| {
        p.fragment.as_ref().and_then(|f| f.output_sink.as_ref()).is_some_and(|s| s.type_ == TDataSinkType::MULTI_CAST_DATA_STREAM_SINK)
    });
    assert!(multicast, "{CTE} has no multicast sink");
}

const SET_OPS: [&str; 4] = [
    "correctness_p0.test_set_operation.test_set_operation",
    "nereids_p0.test_filter_pushdown_set.test_filter_pushdown_set",
    "query_p0.set_operations.set_with_null.set_with_null.set_with_null",
    "query_p0.set_operations.test_pullup_predicate_null.test_pullup_predicate_null",
];

#[test]
fn set_op_replays_have_intersect_and_except() {
    use doris_thrift::gen::plan_nodes::TPlanNodeType as P;
    let fragments: Vec<_> = SET_OPS
        .iter()
        .flat_map(|s| replays::load(s).queries())
        .flat_map(|q| q.params_list.unwrap_or_default())
        .collect();
    let has = |t: P| fragments.iter().any(|p| p.fragment.iter().flat_map(|f| f.plan.iter()).flat_map(|x| &x.nodes).any(|n| n.node_type == t));
    assert!(has(P::INTERSECT_NODE), "no INTERSECT_NODE");
    assert!(has(P::EXCEPT_NODE), "no EXCEPT_NODE");
    let parallel = fragments.iter().any(|p| {
        p.local_params.as_ref().is_some_and(|l| l.len() > 1)
            && p.fragment.iter().flat_map(|f| f.plan.iter()).flat_map(|x| &x.nodes).any(|n| matches!(n.node_type, P::INTERSECT_NODE | P::EXCEPT_NODE))
    });
    assert!(parallel, "no set node runs in more than one instance");
}

const WINDOWS: [&str; 7] = [
    "correctness_p0.test_min_max_window.test_min_max_window",
    "nereids_p0.partition_topn.check_partitionkey.check_partitionkey",
    "nereids_p0.select_no_from.sql.withGroupOrderHaving.withGroupOrderHaving",
    "query_p0.dual.dual.dual",
    "query_p0.select_no_from.sql.withGroupOrderHaving.withGroupOrderHaving",
    "query_p0.sql_functions.window_functions.test_partition_topn.test_partition_topn",
    "correctness_p0.test_always_nullable_window_function.test_always_nullable_window_function",
];

#[test]
fn window_replays_have_analytic_and_partition_sort() {
    use doris_thrift::gen::plan_nodes::TPlanNodeType as P;
    let has = |suite: &str, t: P| {
        replays::load(suite)
            .queries()
            .into_iter()
            .flat_map(|q| q.params_list.unwrap_or_default())
            .any(|p| p.fragment.iter().flat_map(|f| f.plan.iter()).flat_map(|x| &x.nodes).any(|n| n.node_type == t))
    };
    for s in WINDOWS {
        assert!(has(s, P::ANALYTIC_EVAL_NODE), "{s}: no ANALYTIC_EVAL_NODE");
    }
    for s in [WINDOWS[1], WINDOWS[5]] {
        assert!(has(s, P::PARTITION_SORT_NODE), "{s}: no PARTITION_SORT_NODE");
    }
}

/// The suites that run their own INSERT … SELECT: a table sink fed by a scan.
const INSERT_SELECTS: [&str; 4] = [
    "insert_p0.test_insert_docs_demo.test_insert_docs_demo",
    "query_p0.keyword.test_role_mapping_keyword.test_role_mapping_keyword",
    "insert_p0.test_insert_partition_fail_url.test_insert_partition_fail_url",
    "nereids_p0.ddl.clean.test_clean_label_nereids.test_clean_label_nereids",
];

#[test]
fn insert_select_replays_write_from_a_scan() {
    use doris_fixtures::replays::{plan_list, Step};
    use doris_thrift::gen::data_sinks::TDataSinkType;
    use doris_thrift::gen::plan_nodes::TPlanNodeType as P;
    for s in INSERT_SELECTS {
        let writes = replays::load(s)
            .steps
            .iter()
            .filter_map(|step| match step {
                Step::ExecPlanFragment { request, .. } => plan_list(request),
                _ => None,
            })
            .filter(|l| {
                let ps = || l.params_list.iter().flatten();
                ps().any(|p| p.fragment.as_ref().and_then(|f| f.output_sink.as_ref()).is_some_and(|k| k.type_ == TDataSinkType::OLAP_TABLE_SINK))
                    && ps().flat_map(|p| p.fragment.iter().flat_map(|f| f.plan.iter()).flat_map(|x| &x.nodes)).any(|n| n.node_type == P::OLAP_SCAN_NODE)
            })
            .count();
        assert!(writes >= 1, "{s}: no INSERT … SELECT");
    }
}

/// The suites of story 5 that replay: between them, every new scalar function.
const SCALARS: [&str; 32] = [
    "correctness_p0.test_char_default_value.test_char_default_value",
    "correctness_p0.test_implict_cast.test_implicit_cast",
    "correctness_p0.test_like_predicate_with_nullable.test_like_predicate_with_nullable",
    "correctness_p0.test_no_grouping_window.test_no_grouping_window",
    "correctness_p0.test_orderby_mod.test_order_by_mod",
    "datatype_p0.decimalv3.test_if_functions.test_if_functions",
    "nereids_p0.aggregate.agg_window_project.agg_window_project",
    "nereids_p0.aggregate.aggregate_groupby_constant.aggregate_groupby_constant",
    "nereids_p0.authorization.view_authorization.view_authorization",
    "nereids_p0.ddl.show_variables.show_variables_command.show_variables_command",
    "nereids_p0.expression.case_when_to_if.test_case_when_to_if",
    "nereids_p0.expression.fold_constant.fold_constant_cast.fold_constant_cast",
    "nereids_p0.select_no_from.sql.projectAggFuncs.projectAggFuncs",
    "nereids_p0.show.test_show_columns_command.test_show_columns_command",
    "nereids_p0.sql_functions.math_functions.test_multiply.test_multiply",
    "nereids_p0.sql_functions.math_functions.test_round_overflow.test_round_overflow",
    "nereids_p0.sql_functions.string_functions.test_string_function_like.test_string_function_like",
    "nereids_p0.sql_functions.string_functions.test_string_function_like_pushdown.test_string_function_like_pushdown",
    "query_p0.aggregate.support_type.count.count.count",
    "query_p0.select_no_from.sql.projectAggFuncs.projectAggFuncs",
    "query_p0.sql_functions.conditional_functions.test_ifnull.test_ifnull",
    "query_p0.sql_functions.math_functions.test_multiply.test_multiply",
    "query_p0.sql_functions.string_functions.test_like_escape.test_like_escapes",
    "query_p0.sql_functions.string_functions.test_like_literal.test_like_literal",
    "query_p0.sql_functions.string_functions.test_like_no_backslash_escapes_mode.test_like_no_backslash_escapes_mode",
    "query_p0.sql_functions.string_functions.test_like_table.test_like_table",
    "query_p0.sql_functions.string_functions.test_string_function_like_pushdown.test_string_function_like_pushdown",
    "query_p0.sql_functions.window_functions.test_window_float_sliding_frame.test_window_float_sliding_frame",
    "query_p0.union.test_union_decimal_agg.test_union_decimal_agg",
    "query_p0.view.test_view2.test_view2",
    "query_p0.virtual_slot_ref.char_type_shrink_before_project.char_type_shrink_before_projec",
    "query_p0.virtual_slot_ref.virtual_slot_ref_basic.virtual_slot_ref_basic",
];

#[test]
fn scalar_replays_hold_their_functions() {
    let mut seen = BTreeSet::new();
    for s in SCALARS {
        for q in replays::load(s).queries() {
            seen.extend(replays::functions(&q));
        }
    }
    for f in ["add", "subtract", "multiply", "divide", "mod", "abs", "if", "ifnull", "coalesce", "like", "concat", "length", "round"] {
        assert!(seen.contains(f), "no replay calls {f}");
    }
}

/// The M6 story 6 replays: suites the joins (with ASSERT_NUM_ROWS, CASE and `nullable`) make eligible.
/// No replayable suite has a RIGHT_OUTER, RIGHT_SEMI or RIGHT_ANTI hash join: those are in
/// suites that do not extract, which the full run checks.
const JOINS: [&str; 55] = [
    "correctness_p0.test_bucket_join_with_colocate_table.test_bucket_join_with_colocate_table",
    "correctness_p0.test_bugfix_block_reuse.test_bugfix_block_reuse",
    "correctness_p0.test_colocate_join_with_different_tablets.test_colocate_join_with_different_tablets",
    "correctness_p0.test_column_prune_in_hash_join.test_column_prune_in_hash_join",
    "correctness_p0.test_distinct_streaming_agg_local_shuffle.test_distinct_streaming_agg_local_shuffle",
    "correctness_p0.test_having_subquery_with_nullliteral.test_having_subquery_with_nullliteral",
    "correctness_p0.test_join_with_projection.test_join_with_projection",
    "correctness_p0.test_join_without_condition.test_join_without_condition",
    "correctness_p0.test_null_aware_left_anti_join.test_null_aware_left_anti_join",
    "correctness_p0.test_outer_join_with_cross_join.test_outer_join_with_cross_join",
    "correctness_p0.test_outer_join_with_empty_node.test_outer_join_with_empty_node",
    "correctness_p0.test_outer_join_with_order_by.test_outer_join_with_order_by",
    "correctness_p0.test_outer_join_with_subquery.test_outer_join_with_subquery",
    "correctness_p0.test_rf_in_list_not_erased_by_scope_range.test_rf_in_list_not_erased_by_scope_range",
    "correctness_p0.test_runtime_filter.test_runtime_filter",
    "correctness_p0.test_runtime_filter_outer_join_nullable_side.test_runtime_filter_outer_join_nullable_side",
    "datatype_p0.decimalv3.test_decimalv3_cast_to_string.test_decimalv3_cast_to_string",
    "datatype_p0.decimalv3.test_predicate_with_implicit_cast.test_predicate_with_implicit_cast",
    "nereids_p0.colocate.test_multi_cast_colocate.test_multi_cast_colocate",
    "nereids_p0.colocate.test_set_operation_colocate.test_set_operation_colocate",
    "nereids_p0.cte.costbasedrewrite_producer.costbasedrewrite_producer.costbasedrewrite_producer",
    "nereids_p0.cte.subquery_in_cte.subquery_in_cte.subquery_in_cte",
    "nereids_p0.eager_agg.scalar_agg_pushdown.scalar_agg_pushdown",
    "nereids_p0.join.asof.test_asof_join_large_bucket.test_asof_join_large_bucket",
    "nereids_p0.join.outer_join_with_empty_node.nereids_outer_join_with_empty_node",
    "nereids_p0.join.test_join2.test_join2",
    "nereids_p0.join.test_join3.test_join3",
    "nereids_p0.join.test_join4.test_join4",
    "nereids_p0.join.test_join_local_shuffle.test_join_local_shuffle",
    "nereids_p0.join.test_limit_join.test_limit_join",
    "nereids_p0.join.test_mark_join.test_mark_join",
    "nereids_p0.join.test_nestedloop_outer_join.test_nestedloop_outer_join",
    "nereids_p0.join.test_nestedloop_semi_anti_join.test_nestedloop_semi_anti_join",
    "nereids_p0.join.test_other_conjuncts_nullable.test_other_conjuncts_nullable",
    "nereids_p0.join.test_runtimefilter_on_datev2.test_runtimefilter_on_datev2",
    "nereids_p0.join.test_runtimefilter_on_decimal.test_runtimefilter_on_decimal",
    "nereids_p0.limit.OffsetInSubqueryWithJoin.test_offset_in_subquery_with_join",
    "nereids_p0.runtime_filter.expr-target.expr-target",
    "nereids_p0.runtime_filter.set_operator_in_filter.set_operator_in_filter",
    "nereids_p0.sql_functions.test_in_expr.test_in_expr",
    "nereids_p0.subquery.test_subquery_in_simple_case.test_subquery_in_simple_case",
    "query_p0.join.eliminate_const_join_condition.eliminate_const_join_condition",
    "query_p0.join.rqg.doris8317.doris8317.doris8317",
    "query_p0.join.rqg.doris9100.doris9100.doris9100",
    "query_p0.join.rqg.rqg4265.rqg4265.rqg4265",
    "query_p0.join.test_full_join_batch_size.test_full_join_batch_size",
    "query_p0.join.test_hash_join_probe_early_eos.test_hash_join_probe_early_eos",
    "query_p0.join.test_join3.test_join3",
    "query_p0.join.test_join4.test_join4",
    "query_p0.join.test_join6.test_join6",
    "query_p0.join.test_join_result_count.test_join_result_count",
    "query_p0.join.test_left_join_batch_size.test_left_join_batch_size",
    "query_p0.join.test_nestedloop_semi_anti_join.test_nestedloop_semi_anti_join",
    "query_p0.join.test_runtimefilter_2.test_runtimefilter_2",
    "query_p0.sql_functions.window_functions.test_qualify_query.test_qualify_query",
];

#[test]
fn join_replays_hold_their_nodes() {
    use doris_thrift::gen::plan_nodes::{TJoinOp, TPlanNodeType as P};
    let mut ops = BTreeSet::new();
    let (mut cross, mut assert_rows, mut bucket_shuffle) = (false, false, false);
    for s in JOINS {
        for q in replays::load(s).queries() {
            for f in q.params_list.iter().flatten().filter_map(|p| p.fragment.as_ref()) {
                let sink = f.output_sink.as_ref().and_then(|s| s.stream_sink.as_ref());
                bucket_shuffle |= sink.is_some_and(|s| s.output_partition.type_ == doris_thrift::gen::partitions::TPartitionType::BUCKET_SHFFULE_HASH_PARTITIONED);
                for n in f.plan.iter().flat_map(|p| &p.nodes) {
                    match n.node_type {
                        P::HASH_JOIN_NODE => {
                            ops.insert(n.hash_join_node.as_ref().expect("hash_join_node").join_op.0);
                        }
                        P::CROSS_JOIN_NODE => cross = true,
                        P::ASSERT_NUM_ROWS_NODE => assert_rows = true,
                        _ => {}
                    }
                }
            }
        }
    }
    for op in [
        TJoinOp::INNER_JOIN,
        TJoinOp::LEFT_OUTER_JOIN,
        TJoinOp::FULL_OUTER_JOIN,
        TJoinOp::LEFT_SEMI_JOIN,
        TJoinOp::LEFT_ANTI_JOIN,
        TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN,
        TJoinOp::NULL_AWARE_LEFT_SEMI_JOIN,
        TJoinOp::ASOF_LEFT_INNER_JOIN,
        TJoinOp::ASOF_LEFT_OUTER_JOIN,
    ] {
        assert!(ops.contains(&op.0), "no replay has a hash join {op:?} (ops seen: {ops:?})");
    }
    assert!(cross, "no replay has a CROSS_JOIN_NODE");
    assert!(assert_rows, "no replay has an ASSERT_NUM_ROWS_NODE");
    assert!(bucket_shuffle, "no replay has a bucket-shuffle sink");
}
