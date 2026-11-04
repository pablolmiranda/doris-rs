use std::sync::{Arc, Mutex};

use doris_be::agent::AgentService;
use doris_be::backend_service::BackendHandler;
use doris_be::config::BeConfig;
use doris_be::master::MasterInfo;
use doris_be::testing::FakeFe;
use doris_storage::{DataDir, TabletManager};
use doris_thrift::gen::agent_service::{TAgentPublishRequest, TAgentServiceVersion, TSnapshotRequest};
use doris_thrift::gen::backend_service::*;
use doris_thrift::gen::doris_external_service::{TScanCloseParams, TScanNextBatchParams, TScanOpenParams};
use doris_thrift::gen::status::{TStatus, TStatusCode};

fn handler() -> (BackendHandler, tempfile::TempDir) {
    let tmp = tempfile::tempdir().unwrap();
    let tablets = Arc::new(Mutex::new(TabletManager::open(vec![DataDir::open(tmp.path(), "127.0.0.1").unwrap()]).unwrap()));
    let master = MasterInfo::new("127.0.0.1");
    let agent = AgentService::new(&BeConfig::default(), tablets.clone(), master.clone(), Arc::new(FakeFe::default()));
    (BackendHandler::new(agent, tablets, master), tmp)
}

/// The answer for a method doris-rs does not implement: a status with code 3 naming the
/// method, or (for results without a status) an application exception naming it.
fn names_itself(method: &str, status: Option<TStatus>, error: Option<thrift::Error>) {
    let msg = match (status, error) {
        (Some(st), None) => {
            assert_eq!(st.status_code, TStatusCode::NOT_IMPLEMENTED_ERROR, "{method}");
            st.error_msgs.unwrap().join(" ")
        }
        (None, Some(thrift::Error::Application(e))) => e.message,
        other => panic!("{method}: {other:?}"),
    };
    assert!(msg.contains(&format!("BackendService.{method}")), "{method}: {msg}");
}

/// A handler's answer: the status in its result, or the error it returned.
type Answer = (Option<TStatus>, Option<thrift::Error>);

fn st<T>(r: thrift::Result<T>, f: impl FnOnce(T) -> Option<TStatus>) -> Answer {
    match r {
        Ok(v) => (f(v), None),
        Err(e) => (None, Some(e)),
    }
}

#[test]
fn every_other_method_errors_and_names_itself() {
    let (h, _tmp) = handler();
    let snapshot = TSnapshotRequest {
        tablet_id: 0, schema_hash: 0, version: None, version_hash: None, timeout: None, missing_version: None, list_files: None,
        allow_incremental_clone: None, preferred_snapshot_version: None, is_copy_tablet_task: None, start_version: None,
        end_version: None, is_copy_binlog: None, ref_tablet_id: None,
    };
    let open = TScanOpenParams {
        cluster: String::new(), database: String::new(), table: String::new(), tablet_ids: vec![], opaqued_query_plan: String::new(),
        batch_size: None, properties: None, limit: None, user: None, passwd: None, keep_alive_min: None, execution_timeout: None,
        mem_limit: None,
    };
    let warm = TWarmUpCacheAsyncRequest { host: String::new(), brpc_port: 0, tablet_ids: vec![], cloud_compute_group_id: None };
    let warm_tablets = TWarmUpTabletsRequest {
        job_id: 0, batch_id: 0, job_metas: None, type_: TWarmUpTabletsRequestType(0), event: None, table_ids: None,
    };
    let cases: Vec<(&str, Answer)> = vec![
        ("make_snapshot", st(h.handle_make_snapshot(snapshot), |r| Some(r.status))),
        ("release_snapshot", st(h.handle_release_snapshot(String::new()), |r| Some(r.status))),
        (
            "publish_cluster_state",
            st(h.handle_publish_cluster_state(TAgentPublishRequest { protocol_version: TAgentServiceVersion(0), updates: vec![] }), |r| Some(r.status)),
        ),
        ("get_trash_used_capacity", st(h.handle_get_trash_used_capacity(), |_| None)),
        ("get_disk_trash_used_capacity", st(h.handle_get_disk_trash_used_capacity(), |_| None)),
        ("submit_routine_load_task", st(h.handle_submit_routine_load_task(vec![]), Some)),
        ("open_scanner", st(h.handle_open_scanner(open), |r| Some(r.status))),
        ("get_next", st(h.handle_get_next(TScanNextBatchParams::default()), |r| Some(r.status))),
        ("close_scanner", st(h.handle_close_scanner(TScanCloseParams::default()), |r| Some(r.status))),
        ("get_stream_load_record", st(h.handle_get_stream_load_record(0), |_| None)),
        ("check_storage_format", st(h.handle_check_storage_format(), |_| None)),
        ("warm_up_cache_async", st(h.handle_warm_up_cache_async(warm), |r| Some(r.status))),
        (
            "check_warm_up_cache_async",
            st(h.handle_check_warm_up_cache_async(TCheckWarmUpCacheAsyncRequest::default()), |r| Some(r.status)),
        ),
        ("sync_load_for_tablets", st(h.handle_sync_load_for_tablets(TSyncLoadForTabletsRequest { tablet_ids: vec![] }), |_| None)),
        ("get_top_n_hot_partitions", st(h.handle_get_top_n_hot_partitions(TGetTopNHotPartitionsRequest::default()), |_| None)),
        ("warm_up_tablets", st(h.handle_warm_up_tablets(warm_tablets), |r| Some(r.status))),
        ("ingest_binlog", st(h.handle_ingest_binlog(TIngestBinlogRequest::default()), |r| r.status)),
        ("query_ingest_binlog", st(h.handle_query_ingest_binlog(TQueryIngestBinlogRequest::default()), |_| None)),
        (
            "get_realtime_exec_status",
            st(h.handle_get_realtime_exec_status(TGetRealtimeExecStatusRequest::default()), |r| r.status),
        ),
        (
            "test_storage_connectivity",
            st(h.handle_test_storage_connectivity(TTestStorageConnectivityRequest::default()), |r| r.status),
        ),
        ("get_python_envs", st(h.handle_get_python_envs(), |_| None)),
        ("get_python_packages", st(h.handle_get_python_packages(String::new()), |_| None)),
    ];
    assert_eq!(cases.len(), 22);
    for (method, (status, error)) in cases {
        names_itself(method, status, error);
    }
}
