//! The M2 fixtures captured from the reference C++ BE (`crates/doris-be/tests/fixtures/m2`, see
//! its README): the FE↔BE messages in capture order, the reference BE's tablet headers as JSON,
//! and its storage layout. For tests only.

use std::path::PathBuf;

use doris_thrift::gen::agent_service::{TAgentTaskRequest, TCreateTabletReq};
use doris_thrift::gen::backend_service::BackendServiceSubmitTasksArgs;
use doris_thrift::gen::frontend_service::FrontendServiceFinishTaskArgs;
use doris_thrift::gen::master_service::TFinishTaskRequest;
use doris_thrift::gen::types::TTaskType;
use serde::Deserialize;
use thrift::protocol::{TBinaryInputProtocol, TInputProtocol};

#[derive(Debug, Clone, Deserialize)]
pub struct Entry {
    pub file: String,
    pub protocol: String,
    #[serde(default)]
    pub service: String,
    pub method: String,
    pub kind: String,
    #[serde(default)]
    pub seqid: i64,
    pub tcp_stream: i64,
    pub frame: i64,
}

#[derive(Debug, Deserialize)]
struct Manifest {
    messages: Vec<Entry>,
    created_tablets: Vec<i64>,
    dropped_tablets: Vec<i64>,
}

pub struct Fixtures {
    dir: PathBuf,
    manifest: Manifest,
}

impl Fixtures {
    pub fn m2() -> Fixtures {
        let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../doris-be/tests/fixtures/m2");
        let text = std::fs::read_to_string(dir.join("manifest.json")).expect("m2 manifest.json");
        Fixtures { dir, manifest: serde_json::from_str(&text).expect("manifest") }
    }

    /// Every message, in capture order.
    pub fn entries(&self) -> &[Entry] {
        &self.manifest.messages
    }

    pub fn bytes(&self, e: &Entry) -> Vec<u8> {
        std::fs::read(self.dir.join(&e.file)).expect("fixture message")
    }

    /// Messages of one method and kind (`call`/`reply`), in capture order, with their bytes.
    pub fn messages(&self, method: &str, kind: &str) -> Vec<(Entry, Vec<u8>)> {
        self.entries()
            .iter()
            .filter(|e| e.method == method && e.kind == kind)
            .map(|e| (e.clone(), self.bytes(e)))
            .collect()
    }

    /// The reply to a call: same stream and sequence id, the first reply after it.
    pub fn reply_to(&self, call: &Entry) -> Option<(Entry, Vec<u8>)> {
        self.entries()
            .iter()
            .find(|e| e.kind == "reply" && e.method == call.method && e.tcp_stream == call.tcp_stream && e.seqid == call.seqid && e.frame > call.frame)
            .map(|e| (e.clone(), self.bytes(e)))
    }

    pub fn created_tablets(&self) -> &[i64] {
        &self.manifest.created_tablets
    }

    pub fn dropped_tablets(&self) -> &[i64] {
        &self.manifest.dropped_tablets
    }

    /// Every agent task the FE sent, in order.
    pub fn tasks(&self) -> Vec<TAgentTaskRequest> {
        self.messages("submit_tasks", "call")
            .into_iter()
            .flat_map(|(_, b)| decode(&b, BackendServiceSubmitTasksArgs::read_from_in_protocol).tasks)
            .collect()
    }

    pub fn create_requests(&self) -> Vec<TCreateTabletReq> {
        self.tasks()
            .into_iter()
            .filter(|t| t.task_type == TTaskType::CREATE)
            .filter_map(|t| t.create_tablet_req)
            .collect()
    }

    pub fn finish_requests(&self) -> Vec<TFinishTaskRequest> {
        self.messages("finishTask", "call")
            .into_iter()
            .map(|(_, b)| decode(&b, FrontendServiceFinishTaskArgs::read_from_in_protocol).request)
            .collect()
    }

    /// The reference BE's `/api/meta/header/<id>?byte_to_base64=true`, for the scenario's tablets.
    pub fn header_json(&self, tablet_id: i64) -> Option<String> {
        std::fs::read_to_string(self.dir.join("headers").join(format!("{tablet_id}.json"))).ok()
    }

    pub fn header_tablets(&self) -> Vec<i64> {
        let mut ids: Vec<i64> = std::fs::read_dir(self.dir.join("headers"))
            .expect("headers dir")
            .filter_map(|e| e.ok()?.file_name().to_str()?.strip_suffix(".json")?.parse().ok())
            .collect();
        ids.sort();
        ids
    }

    /// `layout.txt`: (before the drop, after the drop).
    pub fn layout(&self) -> (Vec<String>, Vec<String>) {
        let text = std::fs::read_to_string(self.dir.join("layout.txt")).expect("layout.txt");
        let mut parts = text.split("# after drop\n");
        let lines = |s: &str| s.lines().map(str::to_string).collect::<Vec<_>>();
        (lines(parts.next().unwrap_or_default()), lines(parts.next().unwrap_or_default()))
    }
}

/// Decodes a strict-binary Thrift message: the header, then the args or result struct.
pub fn decode<T>(bytes: &[u8], read: impl FnOnce(&mut dyn TInputProtocol) -> thrift::Result<T>) -> T {
    let mut p = TBinaryInputProtocol::new(bytes, true);
    p.read_message_begin().expect("message header");
    read(&mut p).expect("message body")
}
