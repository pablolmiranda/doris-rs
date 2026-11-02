//! The corpus holds both directions of every RPC the spec requires the scenario to produce.
use doris_wire::{Kind, Manifest};

#[test]
fn corpus_covers_the_required_methods() {
    let required = [
        ("HeartbeatService", "heartbeat"),
        ("FrontendService", "report"),
        ("BackendService", "submit_tasks"),
        ("FrontendService", "finishTask"),
        ("FrontendService", "reportExecStatus"),
        ("PBackendService", "exec_plan_fragment"),
        ("PBackendService", "fetch_data"),
        ("FrontendService", "loadTxnBegin"),
        ("FrontendService", "streamLoadPut"),
        ("FrontendService", "loadTxnCommit"),
    ];
    let m = Manifest::load();
    let has = |service: &str, method: &str, kinds: [Kind; 2]| {
        m.messages()
            .iter()
            .any(|e| e.service == service && e.method == method && kinds.contains(&e.kind))
    };
    let missing: Vec<_> = required
        .iter()
        .flat_map(|&(service, method)| {
            let call = has(service, method, [Kind::Call, Kind::Request]);
            let reply = has(service, method, [Kind::Reply, Kind::Response]);
            [(!call).then_some("call"), (!reply).then_some("reply")]
                .into_iter()
                .flatten()
                .map(move |dir| format!("{service}.{method} {dir}"))
        })
        .collect();
    assert!(missing.is_empty(), "corpus lacks: {missing:?}");
}
