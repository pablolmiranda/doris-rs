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

/// Plan features the node and sink types do not show: what a scan reads, what feeds a write,
/// how results are encoded, which schema tables are scanned.
#[test]
fn plan_features_the_types_do_not_show() {
    use doris_fixtures::build::{col, scan_query, ty};
    use doris_thrift::gen::palo_internal_service::TPipelineFragmentParamsList;
    use doris_thrift::gen::types::{TKeysType, TPrimitiveType, TUniqueId};
    use doris_wire::needs::plan_needs;
    let list = |p| TPipelineFragmentParamsList::new(vec![p], None, None, None, None, None, None, None, None, None, None, None, None, None);
    let q = || scan_query(TUniqueId { hi: 1, lo: 2 }, 10, 2, &[col("k", ty(TPrimitiveType::INT), false)], 4064);
    let has = |p, kind: &str, v: &str| plan_needs(&list(p)).get(kind).is_some_and(|s| s.contains(v));

    // a duplicate-key scan needs nothing more; a unique-key one needs a merge-on-read
    assert!(!has(q(), "read", "UNIQUE_KEYS"));
    let mut p = q();
    let node = &mut p.fragment.as_mut().unwrap().plan.as_mut().unwrap().nodes[0];
    let scan = node.olap_scan_node.as_mut().unwrap();
    scan.key_type = Some(TKeysType::UNIQUE_KEYS);
    scan.table_name = Some("t(t)".into());
    assert!(has(p.clone(), "read", "UNIQUE_KEYS"));
    // the FE's statistics table: every corpus read of it is empty
    p.fragment.as_mut().unwrap().plan.as_mut().unwrap().nodes[0].olap_scan_node.as_mut().unwrap().table_name =
        Some("column_statistics(column_statistics)".into());
    assert!(!has(p, "read", "UNIQUE_KEYS"));

    // a write fed by a scan
    let mut p = q();
    p.fragment.as_mut().unwrap().output_sink.as_mut().unwrap().type_ = doris_thrift::gen::data_sinks::TDataSinkType::OLAP_TABLE_SINK;
    assert!(has(p, "write", "INSERT_SELECT"));

    // binary-protocol rows
    let mut p = q();
    p.query_options.as_mut().unwrap().mysql_row_binary_format = Some(true);
    assert!(has(p, "result", "binary_rows"));
}
