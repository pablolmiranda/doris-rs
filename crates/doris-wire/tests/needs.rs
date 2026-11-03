//! The needs index: what the reference BE handled for one suite, by feature kind.
use std::path::Path;

use doris_wire::needs::needs_of;
use doris_wire::suites::read_suite;

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
