//! The needs index: what the reference BE handled for one suite, by feature kind.
use std::path::Path;

use doris_thrift::gen::palo_internal_service::TPipelineFragmentParamsList;
use doris_wire::needs::needs_of;
use doris_wire::roundtrip::fragment_plan;
use doris_wire::suites::{read_suite, SuiteRecord};
use thrift::protocol::{TCompactInputProtocol, TSerializable};

#[test]
fn needs_of_the_fixture_suite() {
    let records =
        read_suite(&Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/suite.bin.gz"))
            .unwrap();
    let needs = needs_of(&records).unwrap();
    assert!(
        needs["rpc"].contains("HeartbeatService.heartbeat"),
        "{needs:?}"
    );
    assert!(
        needs["rpc"].contains("PBackendService.exec_plan_fragment"),
        "{needs:?}"
    );
    assert!(needs["rpc"].contains("http.stream_load"), "{needs:?}");
    assert!(needs["plan_node"].contains("EMPTY_SET_NODE"), "{needs:?}");
    assert!(needs["task"].contains("CREATE"), "{needs:?}");
    assert!(needs["sink"].contains("RESULT_SINK"), "{needs:?}");
    assert!(!needs["type"].is_empty(), "{needs:?}");
    assert!(!needs["task"].is_empty(), "{needs:?}");
    for (kind, values) in &needs {
        for v in values {
            assert!(
                !v.chars().all(|c| c.is_ascii_digit() || c == '-'),
                "{kind}:{v} is a number, not a name"
            );
        }
    }
}

/// A `reportExecStatus` call (binary protocol) for query `(hi, lo)`.
fn report(hi: i64, lo: i64) -> SuiteRecord {
    let s = |b: &[u8]| [&(b.len() as u32).to_be_bytes()[..], b].concat();
    let data = [
        &[0x80u8, 0x01, 0x00, 0x01][..],
        &s(b"reportExecStatus"),
        &[0, 0, 0, 1],          // seqid
        &[12, 0, 1],            // args.params
        &[8, 0, 1, 0, 0, 0, 0], // protocol_version
        &[12, 0, 2],            // query_id
        &[10, 0, 1],
        &hi.to_be_bytes(), // hi
        &[10, 0, 2],
        &lo.to_be_bytes(), // lo
        &[0, 0, 0],
    ]
    .concat();
    let header = serde_json::json!({"protocol": "thrift", "service": "FrontendService",
                                    "method": "reportExecStatus", "kind": "call"});
    SuiteRecord { header, data }
}

/// The query id of the fixture suite's first plan fragment.
fn fixture_query() -> (Vec<SuiteRecord>, (i64, i64)) {
    let records =
        read_suite(&Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/suite.bin.gz"))
            .unwrap();
    let r = records
        .iter()
        .find(|r| r.header["method"] == "exec_plan_fragment" && r.header["kind"] == "request")
        .expect("a fragment request in the fixture");
    let plan = fragment_plan(&r.data).unwrap();
    let list = TPipelineFragmentParamsList::read_from_in_protocol(&mut TCompactInputProtocol::new(
        &plan[..],
    ))
    .unwrap();
    let id = &list.params_list.unwrap()[0].query_id;
    let query = (id.hi, id.lo);
    (
        records
            .into_iter()
            .filter(|r| r.header["method"] != "reportExecStatus")
            .collect(),
        query,
    )
}

#[test]
fn a_report_for_a_query_the_suite_started_is_a_need() {
    let (mut records, (hi, lo)) = fixture_query();
    records.push(report(hi, lo));
    assert!(needs_of(&records).unwrap()["rpc"].contains("FrontendService.reportExecStatus"));
}

#[test]
fn a_late_report_for_an_earlier_suites_query_is_not_a_need() {
    let (mut records, (hi, lo)) = fixture_query();
    records.push(report(hi ^ 1, lo));
    assert!(!needs_of(&records).unwrap()["rpc"].contains("FrontendService.reportExecStatus"));
}
