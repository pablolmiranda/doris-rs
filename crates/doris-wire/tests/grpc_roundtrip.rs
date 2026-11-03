//! Every captured gRPC payload decodes with the prost types and re-encodes to the same bytes, and
//! so does the compact-protocol Thrift plan nested in fragment execution requests.
mod common;

use common::first_difference;
use doris_proto::doris::{
    PCancelPlanFragmentRequest, PCancelPlanFragmentResult, PExecPlanFragmentRequest,
    PExecPlanFragmentResult, PExecPlanFragmentStartRequest, PFetchDataRequest, PFetchDataResult,
    PFragmentRequestVersion,
};
use doris_thrift::dispatch::roundtrip_plan_compact;
use doris_wire::{Kind, Manifest, MessageEntry, Protocol};
use prost::Message;

type Reencode = fn(&[u8]) -> Result<Vec<u8>, prost::DecodeError>;

fn reencode<M: Message + Default>(bytes: &[u8]) -> Result<Vec<u8>, prost::DecodeError> {
    Ok(M::decode(bytes)?.encode_to_vec())
}

/// The prost types of each captured `PBackendService` method (`internal_service.proto`).
fn codec(method: &str, kind: Kind) -> Option<Reencode> {
    let request = kind == Kind::Request;
    Some(match (method, request) {
        ("exec_plan_fragment" | "exec_plan_fragment_prepare", true) => {
            reencode::<PExecPlanFragmentRequest>
        }
        ("exec_plan_fragment_start", true) => reencode::<PExecPlanFragmentStartRequest>,
        (
            "exec_plan_fragment" | "exec_plan_fragment_prepare" | "exec_plan_fragment_start",
            false,
        ) => reencode::<PExecPlanFragmentResult>,
        ("cancel_plan_fragment", true) => reencode::<PCancelPlanFragmentRequest>,
        ("cancel_plan_fragment", false) => reencode::<PCancelPlanFragmentResult>,
        ("fetch_data", true) => reencode::<PFetchDataRequest>,
        ("fetch_data", false) => reencode::<PFetchDataResult>,
        _ => return None,
    })
}

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
        let Some(reencode) = codec(&e.method, e.kind) else {
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
        e.protocol == Protocol::Grpc
            && e.kind == Kind::Request
            && matches!(
                e.method.as_str(),
                "exec_plan_fragment" | "exec_plan_fragment_prepare"
            )
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
        let plan = request.request.unwrap_or_default();
        match roundtrip_plan_compact(&plan) {
            Err(err) => failures.push(format!("{what}: plan: {err}")),
            Ok(out) => {
                if let Some(at) = first_difference(&plan, &out) {
                    failures.push(format!(
                        "{what}: plan re-encoding differs at byte {at} ({} bytes captured, {} re-encoded)",
                        plan.len(),
                        out.len()
                    ));
                }
            }
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
