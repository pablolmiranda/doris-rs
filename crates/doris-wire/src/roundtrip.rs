//! Re-encoding checks for one captured message, by its corpus header: Thrift messages through
//! `doris_thrift::dispatch`, gRPC payloads through the `doris-proto` type of the method, and the
//! compact-protocol plan inside fragment requests. A Thrift message whose re-encoding differs
//! only in set/map order (Java writes hash order, the Rust types sorted) is `Reordered`; so is
//! a fragment request whose compact-protocol plan does.
use doris_proto::doris::{
    PCancelPlanFragmentRequest, PCancelPlanFragmentResult, PExecPlanFragmentRequest,
    PExecPlanFragmentResult, PExecPlanFragmentStartRequest, PFetchDataRequest, PFetchDataResult,
};
use doris_thrift::dispatch::{roundtrip_message, roundtrip_plan_compact};
use prost::Message;

use crate::canonical::{binary_message, compact_struct};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Identical,
    Reordered,
    /// HTTP, and gRPC methods this crate has no type for yet, are not checked.
    NotChecked,
}

/// Offset of the first byte where two encodings differ, or `None` when they are equal.
pub fn first_difference(a: &[u8], b: &[u8]) -> Option<usize> {
    a.iter()
        .zip(b)
        .position(|(x, y)| x != y)
        .or((a.len() != b.len()).then(|| a.len().min(b.len())))
}

type Canonical = fn(&[u8]) -> Result<Vec<u8>, crate::canonical::CanonicalError>;

/// `Identical` when the re-encoding equals the capture, `Reordered` when only set/map order
/// differs (equal canonical forms), otherwise an error naming the first differing byte.
fn judge(
    what: &str,
    captured: &[u8],
    again: &[u8],
    canonical: Canonical,
) -> Result<Outcome, String> {
    let Some(at) = first_difference(captured, again) else {
        return Ok(Outcome::Identical);
    };
    let c = canonical(captured);
    if c.is_ok() && c == canonical(again) {
        return Ok(Outcome::Reordered);
    }
    Err(format!(
        "{what} re-encoding differs at byte {at} ({} bytes captured, {} re-encoded)",
        captured.len(),
        again.len()
    ))
}

fn exact(bytes: &[u8]) -> Result<Vec<u8>, crate::canonical::CanonicalError> {
    Ok(bytes.to_vec())
}

pub type Reencode = fn(&[u8]) -> Result<Vec<u8>, String>;

fn reencode<M: Message + Default>(bytes: &[u8]) -> Result<Vec<u8>, String> {
    M::decode(bytes)
        .map(|m| m.encode_to_vec())
        .map_err(|e| e.to_string())
}

/// The prost type of each captured `PBackendService` method and direction
/// (`internal_service.proto`), as a re-encoding function.
pub fn grpc_codec(method: &str, request: bool) -> Option<Reencode> {
    Some(match (method, request) {
        (m, true) if carries_plan(m) => reencode::<PExecPlanFragmentRequest>,
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

/// Whether a request of this `PBackendService` method carries a plan fragment
/// (a `PExecPlanFragmentRequest`).
pub fn carries_plan(method: &str) -> bool {
    matches!(method, "exec_plan_fragment" | "exec_plan_fragment_prepare")
}

/// The plan carried by a request of a method that [`carries_plan`].
pub fn fragment_plan(request: &[u8]) -> Result<Vec<u8>, String> {
    let r = PExecPlanFragmentRequest::decode(request).map_err(|e| e.to_string())?;
    if r.compact != Some(true) {
        return Err("fragment request is not a compact-protocol plan".into());
    }
    Ok(r.request.unwrap_or_default())
}

/// The compact-protocol plan of a fragment request, re-encoded and judged.
pub fn plan(request: &[u8]) -> Result<Outcome, String> {
    let plan = fragment_plan(request)?;
    let again = roundtrip_plan_compact(&plan).map_err(|e| e.to_string())?;
    judge("plan", &plan, &again, compact_struct)
}

pub fn thrift(service: &str, bytes: &[u8]) -> Result<Outcome, String> {
    let rt = roundtrip_message(service, bytes).map_err(|e| e.to_string())?;
    judge("Thrift message", bytes, &rt.bytes, binary_message)
}

pub fn grpc(method: &str, request: bool, bytes: &[u8]) -> Result<Outcome, String> {
    let Some(codec) = grpc_codec(method, request) else {
        return Ok(Outcome::NotChecked);
    };
    judge("gRPC payload", bytes, &codec(bytes)?, exact)?;
    if request && carries_plan(method) {
        return plan(bytes);
    }
    Ok(Outcome::Identical)
}

/// Checks one corpus record by its header.
pub fn check(header: &serde_json::Value, bytes: &[u8]) -> Result<Outcome, String> {
    let field = |k: &str| header.get(k).and_then(|v| v.as_str()).unwrap_or("");
    match field("protocol") {
        "thrift" => thrift(field("service"), bytes),
        "grpc" => {
            if header.get("compressed").and_then(|v| v.as_bool()) == Some(true) {
                return Err("compressed gRPC payload".into());
            }
            grpc(field("method"), field("kind") == "request", bytes)
        }
        _ => Ok(Outcome::NotChecked),
    }
}
