//! The needs index: per suite, what the reference BE handled (RPCs, agent task types, plan node
//! and data sink types, expression node types, function names and primitive types), so the
//! features doris-rs lacks can be ranked by the suites they block.
//!
//! Plans and task requests are decoded with the `doris-thrift` types; the enum values are found
//! in their debug form and named from the generated sources, which hold `pub const NAME: T = T(n)`
//! for every enum.
use std::collections::{BTreeMap, BTreeSet, HashMap};

use doris_thrift::gen::backend_service::BackendServiceSubmitTasksArgs;
use doris_thrift::gen::palo_internal_service::TPipelineFragmentParamsList;
use thrift::protocol::{
    TBinaryInputProtocol, TCompactInputProtocol, TInputProtocol, TSerializable,
};

use crate::roundtrip::fragment_plan;
use crate::suites::SuiteRecord;

pub type Needs = BTreeMap<&'static str, BTreeSet<String>>;

const GENERATED: &[&str] = &[
    include_str!("../../doris-thrift/src/gen/plan_nodes.rs"),
    include_str!("../../doris-thrift/src/gen/exprs.rs"),
    include_str!("../../doris-thrift/src/gen/data_sinks.rs"),
    include_str!("../../doris-thrift/src/gen/types.rs"),
];

/// Enum type → feature kind in the index.
const KINDS: &[(&str, &str)] = &[
    ("TPlanNodeType", "plan_node"),
    ("TDataSinkType", "sink"),
    ("TExprNodeType", "expr"),
    ("TPrimitiveType", "type"),
    ("TTaskType", "task"),
];

/// (enum type, value) → name, from the generated `pub const NAME: TType = TType(n);` lines.
fn enum_names() -> HashMap<(String, i32), String> {
    let mut out = HashMap::new();
    for src in GENERATED {
        for line in src.lines() {
            let Some(rest) = line.trim().strip_prefix("pub const ") else {
                continue;
            };
            let Some((name, rest)) = rest.split_once(": ") else {
                continue;
            };
            let Some((ty, rest)) = rest.split_once(" = ") else {
                continue;
            };
            let Some(n) = rest
                .strip_prefix(ty)
                .and_then(|r| r.strip_prefix('('))
                .and_then(|r| r.strip_suffix(");"))
            else {
                continue;
            };
            if let Ok(n) = n.parse() {
                out.insert((ty.to_string(), n), name.to_string());
            }
        }
    }
    out
}

/// Adds the named enum values and function names found in a decoded value's debug form.
fn collect(debug: &str, names: &HashMap<(String, i32), String>, needs: &mut Needs) {
    for (ty, kind) in KINDS {
        let open = format!("{ty}(");
        let mut rest = debug;
        while let Some(at) = rest.find(&open) {
            rest = &rest[at + open.len()..];
            let end = rest.find(')').unwrap_or(0);
            if let Ok(n) = rest[..end].parse::<i32>() {
                let name = names
                    .get(&(ty.to_string(), n))
                    .cloned()
                    .unwrap_or_else(|| format!("{ty}#{n}"));
                needs.entry(kind).or_default().insert(name);
            }
        }
    }
    let key = "function_name: \"";
    let mut rest = debug;
    while let Some(at) = rest.find(key) {
        rest = &rest[at + key.len()..];
        let end = rest.find('"').unwrap_or(0);
        needs
            .entry("function")
            .or_default()
            .insert(rest[..end].to_lowercase());
    }
}

/// The needs of one suite's records.
pub fn needs_of(records: &[SuiteRecord]) -> Result<Needs, String> {
    let names = enum_names();
    let mut needs = Needs::new();
    for r in records {
        let s = |k: &str| {
            r.header
                .get(k)
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string()
        };
        let (protocol, method, kind) = (s("protocol"), s("method"), s("kind"));
        let service = if protocol == "http" {
            "http".to_string()
        } else {
            s("service")
        };
        needs
            .entry("rpc")
            .or_default()
            .insert(format!("{service}.{method}"));
        if protocol == "thrift" && method == "submit_tasks" && kind == "call" {
            let mut bytes = &r.data[..];
            let mut i = TBinaryInputProtocol::new(&mut bytes, true);
            i.read_message_begin().map_err(|e| e.to_string())?;
            let args = BackendServiceSubmitTasksArgs::read_from_in_protocol(&mut i)
                .map_err(|e| e.to_string())?;
            collect(&format!("{args:?}"), &names, &mut needs);
        }
        if protocol == "grpc"
            && kind == "request"
            && matches!(
                method.as_str(),
                "exec_plan_fragment" | "exec_plan_fragment_prepare"
            )
        {
            let plan = fragment_plan(&r.data)?;
            let mut bytes = &plan[..];
            let mut i = TCompactInputProtocol::new(&mut bytes);
            let list = TPipelineFragmentParamsList::read_from_in_protocol(&mut i)
                .map_err(|e| e.to_string())?;
            collect(&format!("{list:?}"), &names, &mut needs);
        }
    }
    Ok(needs)
}
