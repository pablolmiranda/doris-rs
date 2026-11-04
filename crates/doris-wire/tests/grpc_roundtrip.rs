//! Every captured gRPC payload decodes with the prost types and re-encodes to the same bytes, and
//! so does the compact-protocol Thrift plan nested in fragment execution requests.
use doris_proto::doris::{PExecPlanFragmentRequest, PFragmentRequestVersion};
use doris_wire::roundtrip::{carries_plan, first_difference, grpc_codec, plan, Outcome};
use doris_wire::{Kind, Manifest, MessageEntry, Protocol};
use prost::Message;

fn describe(e: &MessageEntry) -> String {
    format!(
        "{} {}.{} {:?}",
        e.file.display(),
        e.service,
        e.method,
        e.kind
    )
}

#[test]
fn every_grpc_message_reencodes_identically() {
    let m = Manifest::load();
    let grpc: Vec<_> = m
        .messages()
        .iter()
        .filter(|e| e.protocol == Protocol::Grpc)
        .collect();
    assert!(!grpc.is_empty(), "no gRPC messages in the corpus");
    let mut failures = Vec::new();
    for e in &grpc {
        let what = describe(e);
        if e.compressed != Some(false) {
            failures.push(format!("{what}: compressed flag {:?}", e.compressed));
            continue;
        }
        let Some(reencode) = grpc_codec(&e.method, e.kind == Kind::Request) else {
            failures.push(format!("{what}: no prost type for this method"));
            continue;
        };
        let bytes = m.read(e);
        match reencode(&bytes) {
            Err(err) => failures.push(format!("{what}: {err}")),
            Ok(out) => {
                if let Some(at) = first_difference(&bytes, &out) {
                    failures.push(format!(
                        "{what}: re-encoding differs at byte {at} ({} bytes captured, {} re-encoded)",
                        bytes.len(),
                        out.len()
                    ));
                }
            }
        }
    }
    assert!(
        failures.is_empty(),
        "{} of {} gRPC messages did not re-encode identically:\n{}",
        failures.len(),
        grpc.len(),
        failures.join("\n")
    );
}

#[test]
fn exec_plan_fragment_plans_reencode_identically() {
    // both methods carry the plan: a TPipelineFragmentParamsList in TCompactProtocol
    let m = Manifest::load();
    let mut checked = 0;
    let mut failures = Vec::new();
    for e in m.messages().iter().filter(|e| {
        e.protocol == Protocol::Grpc && e.kind == Kind::Request && carries_plan(&e.method)
    }) {
        let what = describe(e);
        let request = PExecPlanFragmentRequest::decode(&m.read(e)[..]).expect("checked above");
        if request.compact != Some(true)
            || request.version != Some(PFragmentRequestVersion::Version3 as i32)
        {
            failures.push(format!(
                "{what}: compact {:?}, version {:?}; expected a compact VERSION_3 plan",
                request.compact, request.version
            ));
            continue;
        }
        match plan(&m.read(e)) {
            Ok(Outcome::Identical) => {}
            Ok(other) => failures.push(format!("{what}: plan re-encodes as {other:?}")),
            Err(err) => failures.push(format!("{what}: {err}")),
        }
        checked += 1;
    }
    assert!(checked > 0, "no exec_plan_fragment requests in the corpus");
    assert!(
        failures.is_empty(),
        "{} plans did not re-encode identically:\n{}",
        failures.len(),
        failures.join("\n")
    );
}
