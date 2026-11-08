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
