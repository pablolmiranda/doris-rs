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

fn first_difference(a: &[u8], b: &[u8]) -> Option<usize> {
    a.iter()
        .zip(b)
        .position(|(x, y)| x != y)
        .or((a.len() != b.len()).then(|| a.len().min(b.len())))
}

fn compare(what: &str, captured: &[u8], out: &[u8]) -> Result<(), String> {
    match first_difference(captured, out) {
        None => Ok(()),
        Some(at) => Err(format!(
            "{what} re-encoding differs at byte {at} ({} bytes captured, {} re-encoded)",
            captured.len(),
            out.len()
        )),
    }
}

fn reencode<M: Message + Default>(bytes: &[u8]) -> Result<Vec<u8>, String> {
    M::decode(bytes)
        .map(|m| m.encode_to_vec())
        .map_err(|e| e.to_string())
}

/// The plan carried by an `exec_plan_fragment` or `exec_plan_fragment_prepare` request.
pub fn fragment_plan(request: &[u8]) -> Result<Vec<u8>, String> {
    let r = PExecPlanFragmentRequest::decode(request).map_err(|e| e.to_string())?;
    if r.compact != Some(true) {
        return Err("fragment request is not a compact-protocol plan".into());
    }
    Ok(r.request.unwrap_or_default())
}

pub fn thrift(service: &str, bytes: &[u8]) -> Result<Outcome, String> {
    let rt = roundtrip_message(service, bytes).map_err(|e| e.to_string())?;
    if first_difference(bytes, &rt.bytes).is_none() {
        return Ok(Outcome::Identical);
    }
    let canonical = binary_message(bytes);
    if canonical.is_ok() && canonical == binary_message(&rt.bytes) {
        return Ok(Outcome::Reordered);
    }
    compare("Thrift message", bytes, &rt.bytes).map(|_| Outcome::Identical)
}

pub fn grpc(method: &str, request: bool, bytes: &[u8]) -> Result<Outcome, String> {
    let out = match (method, request) {
        ("exec_plan_fragment" | "exec_plan_fragment_prepare", true) => {
            let plan = fragment_plan(bytes)?;
            let again = roundtrip_plan_compact(&plan).map_err(|e| e.to_string())?;
            let outcome = if first_difference(&plan, &again).is_none() {
                Outcome::Identical
            } else {
                let canonical = compact_struct(&plan);
                if canonical.is_ok() && canonical == compact_struct(&again) {
                    Outcome::Reordered
                } else {
                    compare("plan", &plan, &again)?;
                    Outcome::Identical
                }
            };
            compare(
                "gRPC payload",
                bytes,
                &reencode::<PExecPlanFragmentRequest>(bytes)?,
            )?;
            return Ok(outcome);
        }
        ("exec_plan_fragment_start", true) => reencode::<PExecPlanFragmentStartRequest>(bytes)?,
        (
            "exec_plan_fragment" | "exec_plan_fragment_prepare" | "exec_plan_fragment_start",
            false,
        ) => reencode::<PExecPlanFragmentResult>(bytes)?,
        ("cancel_plan_fragment", true) => reencode::<PCancelPlanFragmentRequest>(bytes)?,
        ("cancel_plan_fragment", false) => reencode::<PCancelPlanFragmentResult>(bytes)?,
        ("fetch_data", true) => reencode::<PFetchDataRequest>(bytes)?,
        ("fetch_data", false) => reencode::<PFetchDataResult>(bytes)?,
        _ => return Ok(Outcome::NotChecked),
    };
    compare("gRPC payload", bytes, &out).map(|_| Outcome::Identical)
}

/// Checks one corpus record by its header.
pub fn check(header: &serde_json::Value, bytes: &[u8]) -> Result<Outcome, String> {
    let s = |k: &str| header.get(k).and_then(|v| v.as_str()).unwrap_or("");
    match s("protocol") {
        "thrift" => thrift(s("service"), bytes),
        "grpc" => {
            if header.get("compressed").and_then(|v| v.as_bool()) == Some(true) {
                return Err("compressed gRPC payload".into());
            }
            grpc(s("method"), s("kind") == "request", bytes)
        }
        _ => Ok(Outcome::NotChecked),
    }
}
