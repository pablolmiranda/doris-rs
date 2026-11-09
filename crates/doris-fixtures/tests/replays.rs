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
