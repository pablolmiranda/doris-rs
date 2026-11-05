//! `INSERT … VALUES` statements sampled from the traffic corpus by
//! `crates/doris-wire/src/bin/extract-values.rs` (`crates/doris-fixtures/values`): for each, the
//! `exec_plan_fragment` request, the final `reportExecStatus`, the PUBLISH_VERSION task and its
//! `finishTask`, as the reference C++ BE exchanged them.
use std::path::PathBuf;

use doris_proto::doris::PExecPlanFragmentRequest;
use doris_thrift::gen::agent_service::{TAgentTaskRequest, TCreateTabletReq};
use doris_thrift::gen::backend_service::BackendServiceSubmitTasksArgs;
use doris_thrift::gen::data_sinks::TOlapTableSink;
use doris_thrift::gen::frontend_service::{FrontendServiceFinishTaskArgs, FrontendServiceReportExecStatusArgs, TReportExecStatusParams};
use doris_thrift::gen::master_service::TFinishTaskRequest;
use doris_thrift::gen::palo_internal_service::{TPipelineFragmentParams, TPipelineFragmentParamsList};
use doris_thrift::gen::types::{TKeysType, TTaskType};
use prost::Message;
use serde::Deserialize;
use thrift::protocol::{TCompactInputProtocol, TSerializable};

use crate::{decode, Fixtures};

#[derive(Debug, Deserialize)]
struct IndexEntry {
    dir: String,
    suite: String,
}

/// One sampled statement.
#[derive(Debug, Clone)]
pub struct ValuesVector {
    pub dir: String,
    pub suite: String,
    pub request: PExecPlanFragmentRequest,
    pub report: TReportExecStatusParams,
    pub publish: TAgentTaskRequest,
    pub finish: TFinishTaskRequest,
}

pub fn values_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("values")
}

/// Every sampled statement, in index order.
pub fn values_vectors() -> Vec<ValuesVector> {
    let root = values_dir();
    let index: Vec<IndexEntry> = serde_json::from_str(&std::fs::read_to_string(root.join("index.json")).expect("index.json")).expect("index");
    index
        .into_iter()
        .map(|e| {
            let read = |f: &str| std::fs::read(root.join(&e.dir).join(f)).unwrap_or_else(|err| panic!("{}/{f}: {err}", e.dir));
            let request = PExecPlanFragmentRequest::decode(read("fragment.bin").as_slice()).expect("fragment");
            let report = decode(&read("report.bin"), FrontendServiceReportExecStatusArgs::read_from_in_protocol).params;
            let finish = decode(&read("finish.bin"), FrontendServiceFinishTaskArgs::read_from_in_protocol).request;
            let publish = decode(&read("publish.bin"), BackendServiceSubmitTasksArgs::read_from_in_protocol)
                .tasks
                .into_iter()
                .find(|t| t.task_type == TTaskType::PUBLISH_VERSION && t.signature == finish.signature)
                .expect("the PUBLISH_VERSION task");
            ValuesVector { dir: e.dir, suite: e.suite, request, report, publish, finish }
        })
        .collect()
}

impl ValuesVector {
    /// The one fragment of the request (compact Thrift, as `compact=true` says).
    pub fn params(&self) -> TPipelineFragmentParams {
        let mut b = self.request.request.as_deref().expect("request bytes");
        let list = TPipelineFragmentParamsList::read_from_in_protocol(&mut TCompactInputProtocol::new(&mut b)).expect("params");
        list.params_list.expect("params_list").into_iter().next().expect("a fragment")
    }

    pub fn sink(&self) -> TOlapTableSink {
        let p = self.params();
        p.fragment.and_then(|f| f.output_sink).and_then(|s| s.olap_table_sink).expect("OLAP_TABLE_SINK")
    }

    /// CREATE requests for every tablet the sink writes to (`create_requests_for`).
    pub fn create_requests(&self) -> Vec<TCreateTabletReq> {
        create_requests_for(&self.sink())
    }
}

/// CREATE requests for every tablet a sink writes to, for setting up a tablet manager: an M2
/// CREATE as the template, with the sink's index columns and schema hash, partition and table.
/// Duplicate keys; the first key columns (up to 3) as short keys.
pub fn create_requests_for(sink: &TOlapTableSink) -> Vec<TCreateTabletReq> {
    let template = Fixtures::m2().create_requests().into_iter().next().expect("an M2 CREATE");
    let mut out = Vec::new();
    for part in &sink.partition.partitions {
        for idx in &part.indexes {
            let schema = sink.schema.indexes.iter().find(|i| i.id == idx.index_id).expect("index schema");
            let columns = schema.columns_desc.clone().unwrap_or_default();
            let keys = columns.iter().filter(|c| c.is_key == Some(true)).count();
            for &tablet_id in &idx.tablets {
                let mut req = template.clone();
                req.tablet_id = tablet_id;
                req.partition_id = Some(part.id);
                req.table_id = Some(sink.table_id);
                req.version = Some(1);
                req.tablet_schema.schema_hash = schema.schema_hash;
                req.tablet_schema.keys_type = TKeysType::DUP_KEYS;
                req.tablet_schema.short_key_column_count = keys.clamp(1, 3) as i16;
                req.tablet_schema.columns = columns.clone();
                req.tablet_schema.indexes = None;
                req.enable_unique_key_merge_on_write = Some(false);
                out.push(req);
            }
        }
    }
    out
}
