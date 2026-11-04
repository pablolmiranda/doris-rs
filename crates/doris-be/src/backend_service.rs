//! `BackendService` (be_port, 9060). `submit_tasks` and the calls the FE makes on a clock
//! (`get_tablet_stat`, `publish_topic_info`, `get_dictionary_status`) are answered as the C++
//! BE answers them (`be/src/service/backend_service.cpp`); every other method returns
//! NOT_IMPLEMENTED_ERROR naming itself, or, when its result has no status, an application
//! exception naming itself. A panic in a handler becomes such an error.

use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{Arc, Mutex};

use doris_storage::TabletManager;
use doris_thrift::gen::agent_service::{TAgentPublishRequest, TAgentResult, TAgentTaskRequest, TSnapshotRequest};
use doris_thrift::gen::backend_service::*;
use doris_thrift::gen::doris_external_service::{
    TScanBatchResult, TScanCloseParams, TScanCloseResult, TScanNextBatchParams, TScanOpenParams, TScanOpenResult,
};
use doris_thrift::gen::status::TStatus;
use thrift::{ApplicationError, ApplicationErrorKind};

use crate::agent::AgentService;
use crate::master::MasterInfo;
use crate::status;

pub struct BackendHandler {
    agent: AgentService,
    tablets: Arc<Mutex<TabletManager>>,
    master: MasterInfo,
}

impl BackendHandler {
    pub fn new(agent: AgentService, tablets: Arc<Mutex<TabletManager>>, master: MasterInfo) -> BackendHandler {
        BackendHandler { agent, tablets, master }
    }

    fn unsupported(&self, method: &str) -> TStatus {
        status::not_implemented(&self.master.localhost(), &format!("BackendService.{method}"))
    }

    fn agent_result(&self, method: &str) -> TAgentResult {
        TAgentResult { status: self.unsupported(method), snapshot_path: None, allow_incremental_clone: None, snapshot_version: Some(1) }
    }
}

fn exception<T>(method: &str) -> thrift::Result<T> {
    Err(thrift::Error::Application(ApplicationError::new(
        ApplicationErrorKind::Unknown,
        format!("doris-rs does not support BackendService.{method} yet"),
    )))
}

/// Runs a handler; a panic becomes an application exception naming the method.
fn guard<T>(method: &str, f: impl FnOnce() -> thrift::Result<T>) -> thrift::Result<T> {
    catch_unwind(AssertUnwindSafe(f)).unwrap_or_else(|_| {
        Err(thrift::Error::Application(ApplicationError::new(
            ApplicationErrorKind::InternalError,
            format!("doris-rs failed in BackendService.{method}"),
        )))
    })
}

impl BackendServiceSyncHandler for BackendHandler {
    fn handle_submit_tasks(&self, tasks: Vec<TAgentTaskRequest>) -> thrift::Result<TAgentResult> {
        guard("submit_tasks", || Ok(self.agent.submit_tasks(tasks)))
    }

    /// `TabletManager::get_tablet_stat`.
    fn handle_get_tablet_stat(&self) -> thrift::Result<TTabletStatResult> {
        guard("get_tablet_stat", || {
            let stats = self.tablets.lock().unwrap_or_else(|e| e.into_inner()).tablet_stats();
            Ok(TTabletStatResult { tablets_stats: Default::default(), tablet_stat_list: Some(stats) })
        })
    }

    /// `TopicSubscriber::handle_topic_info`: no listeners in M2; the result is left default.
    fn handle_publish_topic_info(&self, _req: TPublishTopicRequest) -> thrift::Result<TPublishTopicResult> {
        guard("publish_topic_info", || Ok(TPublishTopicResult { status: status::ok() }))
    }

    /// `get_dictionary_status`: no dictionaries are loaded, so no statuses.
    fn handle_get_dictionary_status(&self, _ids: Vec<i64>) -> thrift::Result<TDictionaryStatusList> {
        guard("get_dictionary_status", || Ok(TDictionaryStatusList { dictionary_status_list: Some(vec![]) }))
    }

    fn handle_make_snapshot(&self, _r: TSnapshotRequest) -> thrift::Result<TAgentResult> {
        Ok(self.agent_result("make_snapshot"))
    }

    fn handle_release_snapshot(&self, _p: String) -> thrift::Result<TAgentResult> {
        Ok(self.agent_result("release_snapshot"))
    }

    fn handle_publish_cluster_state(&self, _r: TAgentPublishRequest) -> thrift::Result<TAgentResult> {
        Ok(self.agent_result("publish_cluster_state"))
    }

    fn handle_get_trash_used_capacity(&self) -> thrift::Result<i64> {
        exception("get_trash_used_capacity")
    }

    fn handle_get_disk_trash_used_capacity(&self) -> thrift::Result<Vec<TDiskTrashInfo>> {
        exception("get_disk_trash_used_capacity")
    }

    fn handle_submit_routine_load_task(&self, _t: Vec<TRoutineLoadTask>) -> thrift::Result<TStatus> {
        Ok(self.unsupported("submit_routine_load_task"))
    }

    fn handle_open_scanner(&self, _p: TScanOpenParams) -> thrift::Result<TScanOpenResult> {
        Ok(TScanOpenResult { status: self.unsupported("open_scanner"), context_id: None, selected_columns: None })
    }

    fn handle_get_next(&self, _p: TScanNextBatchParams) -> thrift::Result<TScanBatchResult> {
        Ok(TScanBatchResult { status: self.unsupported("get_next"), eos: None, rows: None })
    }

    fn handle_close_scanner(&self, _p: TScanCloseParams) -> thrift::Result<TScanCloseResult> {
        Ok(TScanCloseResult { status: self.unsupported("close_scanner") })
    }

    fn handle_get_stream_load_record(&self, _t: i64) -> thrift::Result<TStreamLoadRecordResult> {
        exception("get_stream_load_record")
    }

    fn handle_check_storage_format(&self) -> thrift::Result<TCheckStorageFormatResult> {
        exception("check_storage_format")
    }

    fn handle_warm_up_cache_async(&self, _r: TWarmUpCacheAsyncRequest) -> thrift::Result<TWarmUpCacheAsyncResponse> {
        Ok(TWarmUpCacheAsyncResponse { status: self.unsupported("warm_up_cache_async") })
    }

    fn handle_check_warm_up_cache_async(
        &self,
        _r: TCheckWarmUpCacheAsyncRequest,
    ) -> thrift::Result<TCheckWarmUpCacheAsyncResponse> {
        Ok(TCheckWarmUpCacheAsyncResponse { status: self.unsupported("check_warm_up_cache_async"), task_done: None })
    }

    fn handle_sync_load_for_tablets(&self, _r: TSyncLoadForTabletsRequest) -> thrift::Result<TSyncLoadForTabletsResponse> {
        exception("sync_load_for_tablets")
    }

    fn handle_get_top_n_hot_partitions(
        &self,
        _r: TGetTopNHotPartitionsRequest,
    ) -> thrift::Result<TGetTopNHotPartitionsResponse> {
        exception("get_top_n_hot_partitions")
    }

    fn handle_warm_up_tablets(&self, _r: TWarmUpTabletsRequest) -> thrift::Result<TWarmUpTabletsResponse> {
        Ok(TWarmUpTabletsResponse {
            status: self.unsupported("warm_up_tablets"),
            job_id: None,
            batch_id: None,
            pending_job_size: None,
            finish_job_size: None,
        })
    }

    fn handle_ingest_binlog(&self, _r: TIngestBinlogRequest) -> thrift::Result<TIngestBinlogResult> {
        Ok(TIngestBinlogResult { status: Some(self.unsupported("ingest_binlog")), is_async: None })
    }

    fn handle_query_ingest_binlog(&self, _r: TQueryIngestBinlogRequest) -> thrift::Result<TQueryIngestBinlogResult> {
        exception("query_ingest_binlog")
    }

    fn handle_get_realtime_exec_status(
        &self,
        _r: TGetRealtimeExecStatusRequest,
    ) -> thrift::Result<TGetRealtimeExecStatusResponse> {
        Ok(TGetRealtimeExecStatusResponse {
            status: Some(self.unsupported("get_realtime_exec_status")),
            report_exec_status_params: None,
            query_stats: None,
        })
    }

    fn handle_test_storage_connectivity(
        &self,
        _r: TTestStorageConnectivityRequest,
    ) -> thrift::Result<TTestStorageConnectivityResponse> {
        Ok(TTestStorageConnectivityResponse { status: Some(self.unsupported("test_storage_connectivity")) })
    }

    fn handle_get_python_envs(&self) -> thrift::Result<Vec<TPythonEnvInfo>> {
        exception("get_python_envs")
    }

    fn handle_get_python_packages(&self, _v: String) -> thrift::Result<Vec<TPythonPackageInfo>> {
        exception("get_python_packages")
    }
}
