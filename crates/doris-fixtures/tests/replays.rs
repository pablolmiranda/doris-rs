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

/// The M6 story 7 batch 1 replays (regexp, to_date, eq_for_null, concat_ws, stddev,
/// window_funnel_v2, REPEAT_NODE, DATA_GEN_SCAN_NODE).
const BATCH1: [&str; 16] = [
    "correctness_p0.test_assert_row_num.test_assert_num_rows",
    "correctness_p0.test_grouping_sets_empty.test_grouping_sets_empty",
    "correctness_p0.test_like_predicate_with_concat.test_like_predicate_with_concat",
    "correctness_p0.test_null_equal.test_null_equal",
    "correctness_p0.test_outer_join_with_window_function.test_outer_join_with_with_window_function",
    "correctness_p0.test_outer_join_with_window_function_datev2.test_outer_join_with_window_function_datev2",
    "nereids_p0.aggregate.agg_distinct_case_when.agg_distinct_case_when",
    "nereids_p0.aggregate.agg_with_distinct_project.agg_with_distinct_project",
    "nereids_p0.aggregate.aggregate_groupby_simplify.aggregate_groupby_simplify",
    "nereids_p0.aggregate.aggregate_stddev_over_range.aggregate_stddev_over_range",
    "nereids_p0.grouping_sets.test_grouping_sets1.test_grouping_sets1",
    "query_p0.having.having_between.test_having_between",
    "query_p0.join.rqg.rqg500079780.rqg500079780.rqg1333347798",
    "query_p0.set.test_user_var.test_user_var",
    "query_p0.sql_functions.string_functions.test_regexp_chinese.test_regexp_chinese",
    "query_p0.sql_functions.string_functions.test_regexp_extended_nonconstant_pattern.test_regexp_extended_nonconstant_pattern",
];

#[test]
fn batch1_replays_hold_their_features() {
    use doris_thrift::gen::plan_nodes::TPlanNodeType as P;
    let wanted = ["regexp", "to_date", "concat_ws", "stddev", "window_funnel_v2", "eq_for_null"];
    let mut seen = BTreeSet::new();
    for s in BATCH1 {
        let mut mine = BTreeSet::new();
        for q in replays::load(s).queries() {
            mine.extend(replays::functions(&q).into_iter().filter(|f| wanted.contains(&f.as_str())));
            if format!("{q:?}").contains("opcode: TExprOpcode(65)") {
                mine.insert("eq_for_null".to_string());
            }
            for f in q.params_list.iter().flatten().filter_map(|p| p.fragment.as_ref()) {
                for n in f.plan.iter().flat_map(|p| &p.nodes) {
                    match n.node_type {
                        P::REPEAT_NODE => mine.insert("REPEAT_NODE".to_string()),
                        P::DATA_GEN_SCAN_NODE => mine.insert("DATA_GEN_SCAN_NODE".to_string()),
                        _ => false,
                    };
                }
            }
        }
        assert!(!mine.is_empty(), "{s} holds no batch 1 feature");
        seen.extend(mine);
    }
    for f in wanted.iter().copied().chain(["REPEAT_NODE", "DATA_GEN_SCAN_NODE"]) {
        assert!(seen.contains(f), "no batch 1 replay holds {f} (seen: {seen:?})");
    }
}

/// The M6 story 7 batch 2 replays (geo, table functions, moments, dates, bits).
const BATCH2: [&str; 39] = [
    "correctness_p0.test_agg_materialize.test_agg_materialize",
    "correctness_p0.test_bit_function.test_bit_functions",
    "correctness_p0.test_bit_shift_lagency.test_bit_shift",
    "correctness_p0.test_bit_shift_nereids.test_bit_shift_nereids",
    "correctness_p0.test_bit_test_function.test_bit_test_function",
    "correctness_p0.test_case_when_date.test_case_when_date",
    "correctness_p0.test_date_diff.test_date_diff",
    "correctness_p0.test_inlineview_with_lateralview.test_inlineview_with_lateralview",
    "correctness_p0.test_nested_lateral_view.test_nested_lateral_view",
    "correctness_p0.test_outer_join_sort.test_outer_join_sort",
    "correctness_p0.test_probe_clean.test_probe_clean",
    "correctness_p0.test_select_constant.test_select_constant",
    "correctness_p0.test_table_function_with_subquery.test_table_function_with_subquery",
    "correctness_p0.test_window_function_with_project.test_window_function_with_project",
    "correctness_p0.test_xor_pred.test_xor_pred",
    "datatype_p0.date.test_weekofyear.test_weekofyear",
    "nereids_p0.expression.operator_precedence.operator_precedence",
    "nereids_p0.legacy.test_clone_legacy_nullable_from_nereids.test_clone_legacy_nullable_from_nereids",
    "nereids_p0.sql_functions.datetime_functions.test_date_or_datetime_computation_negative.test_date_or_datetime_computation_negative",
    "nereids_p0.sql_functions.spatial_functions.test_st_astext.test_st_astext",
    "nereids_p0.sql_functions.string_functions.test_split_by_string.test_split_by_string",
    "nereids_p0.stats.col_stats.estimate_string_as_date.estimate_string_as_date",
    "nereids_p0.test_generate_subquery_output.test_generate_subquery_output",
    "nereids_p0.test_select_constant.test_select_constant",
    "query_p0.aggregate.aggregate_function_kurt.aggregate_function_kurt",
    "query_p0.aggregate.aggregate_function_skew.aggregate_function_skew",
    "query_p0.aggregate.support_type.avg_weighted.avg_weighted.avg_weighted",
    "query_p0.aggregate.support_type.corr.corr.corr",
    "query_p0.aggregate.support_type.corr_welford.corr_welford.corr_welford",
    "query_p0.aggregate.support_type.covar.covar.covar",
    "query_p0.aggregate.support_type.covar_samp.covar_samp.covar_samp",
    "query_p0.aggregate.support_type.regr_intercept.regr_intercept.regr_intercept",
    "query_p0.aggregate.support_type.regr_slope.regr_slope.regr_slope",
    "query_p0.join.left_anti_with_other.left_anti_with_other.left_anti_with_other",
    "query_p0.join.test_ignore.test_ignore.test_ignore",
    "query_p0.lateral_view.test_with_view.lateral_view_with_inline_view",
    "query_p0.sort_spill.sort_spill.sort_spill",
    "query_p0.sql_functions.spatial_functions.test_st_astext.test_st_astext",
    "query_p0.sql_functions.test_years_add.test_years_add",
];

#[test]
fn batch2_replays_hold_their_features() {
    use doris_thrift::gen::plan_nodes::TPlanNodeType as P;
    let wanted = ["st_point", "st_astext", "explode_numbers", "explode", "split_by_string", "non_nullable", "regr_slope", "regr_intercept", "corr", "corr_welford", "covar", "covar_samp", "kurt", "skew", "avg_weighted", "year", "month", "dayofmonth", "weekofyear", "yearweek", "str_to_date", "days_diff", "weeks_diff", "years_add", "years_sub", "months_add", "months_sub", "weeks_add", "weeks_sub", "days_add", "days_sub", "hours_add", "hours_sub", "minutes_add", "minutes_sub", "seconds_add", "seconds_sub", "bitand", "bitor", "bitxor", "bitnot", "bit_count", "bit_shift_left", "bit_shift_right", "bit_test", "xor"];
    let mut seen = BTreeSet::new();
    for s in BATCH2 {
        let mut mine = BTreeSet::new();
        for q in replays::load(s).queries() {
            mine.extend(replays::functions(&q).into_iter().filter(|f| wanted.contains(&f.as_str())));
            for f in q.params_list.iter().flatten().filter_map(|p| p.fragment.as_ref()) {
                for n in f.plan.iter().flat_map(|p| &p.nodes) {
                    if n.node_type == P::TABLE_FUNCTION_NODE {
                        mine.insert("TABLE_FUNCTION_NODE".to_string());
                    }
                }
            }
        }
        assert!(!mine.is_empty(), "{s} holds no batch 2 feature");
        seen.extend(mine);
    }
    assert!(seen.contains("TABLE_FUNCTION_NODE"), "no batch 2 replay holds a TABLE_FUNCTION_NODE");
    assert!(seen.len() >= 30, "batch 2 replays hold only {seen:?}");
}

/// M6 story 7 batch 3 (interval calls, math, strings, aggregates).
const BATCH3: [&str; 26] = [
    "correctness_p0.test_grouping_with_alias.test_grouping_with_alias",
    "correctness_p0.test_keywords.test_keywords",
    "correctness_p0.test_outer_join_with_null_value.test_outer_join_with_null_value",
    "correctness_p0.test_partial_const_args_for_function.test_partial_const_args_for_function",
    "correctness_p0.test_string_pad_function.test_string_pad_function",
    "datatype_p0.decimalv3.fix-overflow.fix-overflow",
    "datatype_p0.string.test_string_embedded_nul_zonemap.test_string_embedded_nul_zonemap",
    "nereids_p0.sql_functions.aggregate_functions.test_aggregate_percentile_no_cast.test_aggregate_percentile_no_cast",
    "nereids_p0.sql_functions.conditional_functions.test_conditional_function.test_conditional_function",
    "nereids_p0.sql_functions.width_bucket_fuctions.test_width_bucket_function.test_width_bucket_function",
    "query_p0.aggregate.exponential_moving_average.exponential_moving_average.exponential_moving_average",
    "query_p0.aggregate.support_type.percentile.percentile.percentile",
    "query_p0.aggregate.support_type.percentile_ar.percentile_array.percentile_array",
    "query_p0.set_operations.set_operation_exprid_reuse.set_operation_exprid_reuse",
    "query_p0.sql_functions.aggregate_functions.test_aggregate_retention_param_limit.test_aggregate_retention_param_limit",
    "query_p0.sql_functions.aggregate_functions.test_linear_histogram_boolean.test_linear_histogram_boolean",
    "query_p0.sql_functions.math_functions.test_function_truncate.test_function_truncate",
    "query_p0.sql_functions.math_functions.test_math_unary_always_nullable.test_math_unary_alway_nullable",
    "query_p0.sql_functions.math_functions.test_mod.test_mod",
    "query_p0.sql_functions.math_functions.test_triangle.test_triangle",
    "query_p0.sql_functions.string_functions.test_find_in_set.test_find_in_set",
    "query_p0.sql_functions.string_functions.test_like_whole_string_match.test_like_whole_string_match",
    "query_p0.sql_functions.string_functions.test_parse_data_size_nullable.test_parse_data_size_nullable",
    "query_p0.sql_functions.string_functions.test_url_decode.test_url_decode",
    "query_p0.sql_functions.string_functions.test_url_encode.test_url_encode",
    "query_p0.sql_functions.string_functions.test_url_functions.test_url_functions",
];

#[test]
fn batch3_replays_hold_their_features() {
    let wanted = ["acos", "acosh", "asin", "atan2", "atanh", "bin", "char", "cot", "crc32", "csc", "cut_to_first_significant_subdomain", "dsqrt", "element_at", "exponential_moving_average", "factorial", "find_in_set", "first_significant_subdomain", "floor", "group_bit_and", "group_bit_or", "group_bit_xor", "hex", "least", "linear_histogram", "ln", "lpad", "ltrim_in", "nullif", "parse_data_size", "percentile", "percentile_array", "pmod", "pow", "replace", "retention", "rpad", "rtrim_in", "sec", "sqrt", "top_level_domain", "trim_in", "truncate", "unhex", "upper", "url_decode", "url_encode", "width_bucket"];
    let mut seen = BTreeSet::new();
    for s in BATCH3 {
        let mut mine = BTreeSet::new();
        for q in replays::load(s).queries() {
            mine.extend(replays::functions(&q).into_iter().filter(|f| wanted.contains(&f.as_str())));
            let cfc = format!("node_type: {:?},", doris_thrift::gen::exprs::TExprNodeType::COMPUTE_FUNCTION_CALL);
            if format!("{q:?}").contains(&cfc) {
                mine.insert("COMPUTE_FUNCTION_CALL".to_string());
            }
        }
        assert!(!mine.is_empty(), "{s} holds no batch 3 feature");
        seen.extend(mine);
    }
    assert!(seen.contains("COMPUTE_FUNCTION_CALL"), "no batch 3 replay holds a COMPUTE_FUNCTION_CALL");
    assert!(seen.len() >= 30, "batch 3 replays hold only {seen:?}");
}

#[test]
fn exec_replies_are_loaded() {
    // C++ evaluates constants at open: years_add('9999-12-31', 1) fails exec_plan_fragment itself
    let r = replays::load("query_p0.sql_functions.test_years_add.test_years_add");
    let refused: Vec<String> = r
        .steps
        .iter()
        .filter_map(|s| match s {
            replays::Step::ExecPlanFragment { reply: Some(reply), .. } if reply.status.status_code != 0 => Some(reply.status.error_msgs.join("")),
            _ => None,
        })
        .collect();
    assert!(refused.iter().any(|m| m == "(127.0.0.1)[E-218]Operation year_add of 9999-12-31, 1 out of range"), "{refused:?}");
}

const BATCH5: [&str; 7] = [
    "nereids_p0.compress_materialize.compress_materialize.compress_materialize",
    "nereids_p0.compress_materialize.pushdown_encode.pushdown_encode",
    "nereids_p0.sql_functions.hash_functions.test_hash_function.test_hash_function",
    "nereids_p0.subquery.test_inlineview_with_project.inlineview_with_project",
    "query_p0.sql_functions.datetime_functions.test_convert_tz.test_convert_tz",
    "query_p0.sql_functions.datetime_functions.test_week_floor_ceil_microsecond.test_week_floor_ceil_microsecond",
    "query_p0.sql_functions.string_functions.test_code_varchar.test_code_varchar",
];

#[test]
fn batch5_replays_hold_their_features() {
    let wanted = ["convert_tz", "date_format", "day_ceil", "day_floor", "day_hour_add", "day_second_add", "decode_as_varchar", "elt", "encode_as_bigint", "encode_as_int", "encode_as_largeint", "encode_as_smallint", "format_round", "from_microsecond", "from_millisecond", "from_second", "from_unixtime", "hour_ceil", "hour_floor", "locate", "milliseconds_add", "minute_ceil", "minute_floor", "minute_second_add", "money_format", "month_ceil", "month_floor", "months_between", "murmur_hash3_32", "murmur_hash3_64", "next_day", "not_null_or_empty", "null_or_empty", "quarter_ceil", "quarter_floor", "second_ceil", "second_floor", "second_microsecond_add", "starts_with", "sub_replace", "unix_timestamp", "week_ceil", "week_floor", "xxhash_32", "xxhash_64", "year_ceil", "year_floor"];
    let mut seen = BTreeSet::new();
    for s in BATCH5 {
        let mut mine = BTreeSet::new();
        for q in replays::load(s).queries() {
            mine.extend(replays::functions(&q).into_iter().filter(|f| wanted.contains(&f.as_str())));
        }
        assert!(!mine.is_empty(), "{s} holds no batch 5 feature");
        seen.extend(mine);
    }
    assert!(seen.len() >= 13, "batch 5 replays hold only {seen:?}");
}

const BATCH6: [&str; 5] = [
    "nereids_p0.multi_distinct.multi_distinct_window.multi_distinct_window",
    "nereids_p0.physical_translator.test_physical_translator.test_physical_translator",
    "nereids_p0.sql_functions.encryption_digest.test_encryption_function.test_encryption_function",
    "nereids_p0.system.test_query_sys.test_query_sys",
    "query_p0.sql_functions.encryption_digest.key_case_sensitive.key_case_sensitive",
];

#[test]
fn batch6_replays_hold_their_features() {
    let wanted = ["aes_decrypt", "aes_encrypt", "from_base64", "multi_distinct_group_concat", "multi_distinct_sum", "random", "sleep", "sm3", "sm3sum", "sm4_decrypt", "sm4_encrypt", "to_base64"];
    let mut seen = BTreeSet::new();
    for s in BATCH6 {
        let mut mine = BTreeSet::new();
        for q in replays::load(s).queries() {
            mine.extend(replays::functions(&q).into_iter().filter(|f| wanted.contains(&f.as_str())));
        }
        assert!(!mine.is_empty(), "{s} holds no batch 6 feature");
        seen.extend(mine);
    }
    assert!(seen.len() >= 12, "batch 6 replays hold only {seen:?}");
}
