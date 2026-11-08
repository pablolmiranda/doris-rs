//! Agent tasks the FE sends with `BackendService.submit_tasks`, as `AgentServer::submit_tasks`
//! and the task worker pools do it (`be/src/agent/agent_server.cpp`, `task_worker_pool.cpp`):
//! the call returns at once, each task runs on a worker of its type, and the result goes back
//! with `FrontendService.finishTask`.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::mpsc::{channel, Sender};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use doris_storage::{StorageError, TabletManager};
use doris_thrift::gen::agent_service::{TAgentResult, TAgentTaskRequest};
use doris_thrift::gen::master_service::{TFinishTaskRequest, TTabletInfo};
use doris_thrift::gen::status::{TStatus, TStatusCode};
use doris_thrift::gen::types::{TBackend, TPushType, TTaskType};

use crate::config::BeConfig;
use crate::fe_client::FeClient;
use crate::master::MasterInfo;
use crate::status;

/// `finish_task`: TASK_FINISH_MAX_RETRY tries, one second apart.
const FINISH_TRIES: u32 = 3;
const FINISH_RETRY_DELAY: Duration = Duration::from_secs(1);

#[derive(Clone)]
pub struct AgentService {
    inner: Arc<Inner>,
}

struct Inner {
    tablets: Arc<Mutex<TabletManager>>,
    master: MasterInfo,
    fe: Arc<dyn FeClient>,
    ports: (i32, i32, i32),
    /// `s_task_signatures`: tasks received and not yet finished, by type.
    signatures: Mutex<BTreeMap<i32, BTreeSet<i64>>>,
    workers: Mutex<HashMap<i32, Sender<TAgentTaskRequest>>>,
    in_flight: Mutex<usize>,
    idle: Condvar,
}

impl AgentService {
    pub fn new(cfg: &BeConfig, tablets: Arc<Mutex<TabletManager>>, master: MasterInfo, fe: Arc<dyn FeClient>) -> AgentService {
        AgentService {
            inner: Arc::new(Inner {
                tablets,
                master,
                fe,
                ports: (i32::from(cfg.be_port), i32::from(cfg.webserver_port), i32::from(cfg.brpc_port)),
                signatures: Mutex::new(BTreeMap::new()),
                workers: Mutex::new(HashMap::new()),
                in_flight: Mutex::new(0),
                idle: Condvar::new(),
            }),
        }
    }

    /// `AgentServer::submit_tasks`.
    pub fn submit_tasks(&self, tasks: Vec<TAgentTaskRequest>) -> TAgentResult {
        let localhost = self.inner.master.localhost();
        if self.inner.master.fe_address().is_none() {
            let st = status::error(TStatusCode::CANCELLED, "CANCELLED", &localhost, "Have not get FE Master heartbeat yet");
            return agent_result(st);
        }
        for task in tasks {
            if !self.inner.register(task.task_type, task.signature) {
                continue; // a duplicate: already queued or running
            }
            // agent_server.cpp: "successfully submit task" with the type and signature
            eprintln!("doris-rs be: task {} signature={}", task_type_name(task.task_type), task.signature);
            self.inner.enqueue(task);
        }
        agent_result(status::ok())
    }

    /// `s_task_signatures`, for the task report.
    pub fn running_tasks(&self) -> BTreeMap<TTaskType, BTreeSet<i64>> {
        let sigs = self.inner.signatures.lock().unwrap_or_else(|e| e.into_inner());
        sigs.iter().map(|(t, s)| (TTaskType(*t), s.clone())).collect()
    }

    /// Waits until no task is queued or running; false on timeout.
    pub fn wait_idle(&self, timeout: Duration) -> bool {
        let n = self.inner.in_flight.lock().unwrap_or_else(|e| e.into_inner());
        let (_guard, res) = self.inner.idle.wait_timeout_while(n, timeout, |n| *n > 0).unwrap_or_else(|e| e.into_inner());
        !res.timed_out()
    }
}

/// `TAgentResult`; `snapshot_version` has the IDL default 1, which the C++ BE always writes.
fn agent_result(status: TStatus) -> TAgentResult {
    TAgentResult { status, snapshot_path: None, allow_incremental_clone: None, snapshot_version: Some(1) }
}

impl Inner {
    /// `register_task_info`: false for a signature already registered.
    fn register(&self, t: TTaskType, signature: i64) -> bool {
        if matches!(t, TTaskType::PUSH_STORAGE_POLICY | TTaskType::PUSH_COOLDOWN_CONF | TTaskType::COMPACTION) || signature == -1 {
            return true;
        }
        self.signatures.lock().unwrap_or_else(|e| e.into_inner()).entry(t.0).or_default().insert(signature)
    }

    fn remove(&self, t: TTaskType, signature: i64) {
        if let Some(set) = self.signatures.lock().unwrap_or_else(|e| e.into_inner()).get_mut(&t.0) {
            set.remove(&signature);
        }
    }

    /// One worker thread per task type, in arrival order. As in `AgentServer::submit_tasks`, a
    /// REALTIME_PUSH goes to the PUSH worker but keeps its own type, which its finishTask and
    /// its signature carry.
    fn enqueue(self: &Arc<Self>, task: TAgentTaskRequest) {
        *self.in_flight.lock().unwrap_or_else(|e| e.into_inner()) += 1;
        let worker = if task.task_type == TTaskType::REALTIME_PUSH { TTaskType::PUSH } else { task.task_type };
        let mut workers = self.workers.lock().unwrap_or_else(|e| e.into_inner());
        let sender = workers.entry(worker.0).or_insert_with(|| {
            let (tx, rx) = channel::<TAgentTaskRequest>();
            let inner = Arc::clone(self);
            std::thread::Builder::new()
                .name(format!("agent-{}", task_type_name(worker)))
                .spawn(move || {
                    for task in rx {
                        let ran = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| inner.run(&task)));
                        if ran.is_err() {
                            inner.remove(task.task_type, task.signature);
                        }
                        let mut n = inner.in_flight.lock().unwrap_or_else(|e| e.into_inner());
                        *n -= 1;
                        inner.idle.notify_all();
                    }
                })
                .expect("spawn agent worker");
            tx
        });
        sender.send(task).expect("agent worker alive");
    }

    fn run(&self, task: &TAgentTaskRequest) {
        match task.task_type {
            TTaskType::CREATE => self.create(task),
            TTaskType::DROP => self.drop_tablet(task),
            TTaskType::PUBLISH_VERSION => self.publish(task),
            TTaskType::CLEAR_TRANSACTION_TASK => self.clear_transaction(task),
            TTaskType::PUSH | TTaskType::REALTIME_PUSH => self.push(task),
            // visible_version_callback: no finishTask, and (as in the C++ BE) the signature
            // stays registered.
            TTaskType::UPDATE_VISIBLE_VERSION => {
                let versions = task.visible_version_req.as_ref().and_then(|r| r.partition_version.clone()).unwrap_or_default();
                let mut tablets = self.tablets();
                for (pid, version) in versions {
                    tablets.update_visible_version(pid, version);
                }
            }
            other => {
                let st = status::not_implemented(&self.master.localhost(), &format!("agent task {}", task_type_name(other)));
                self.finish(self.finish_request(task, st));
                self.remove(task.task_type, task.signature);
            }
        }
    }

    fn tablets(&self) -> std::sync::MutexGuard<'_, TabletManager> {
        self.tablets.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// `create_tablet_callback`.
    fn create(&self, task: &TAgentTaskRequest) {
        let (st, infos, report_version) = match &task.create_tablet_req {
            None => (self.storage_error(&StorageError::Unsupported { what: "a CREATE task without create_tablet_req".into() }), vec![], None),
            Some(req) => {
                let mut tablets = self.tablets();
                match tablets.create_tablet(req) {
                    Ok(info) => (status::ok(), vec![info], Some(tablets.report_version())),
                    Err(e) => (self.storage_error(&e), vec![], Some(tablets.report_version())),
                }
            }
        };
        let mut req = self.finish_request(task, st);
        req.finish_tablet_infos = Some(infos);
        req.report_version = report_version;
        self.finish(req);
        self.remove(task.task_type, task.signature);
    }

    /// `push_callback`: loads and DELETEs by push are not supported, so each finishes with an
    /// error the FE fails the job on, rather than waiting for a reply it never gets.
    fn push(&self, task: &TAgentTaskRequest) {
        let st = status::not_implemented(&self.master.localhost(), &format!("agent task {}", task_type_name(task.task_type)));
        let mut req = self.finish_request(task, st);
        if let Some(push) = task.push_req.as_ref().filter(|p| p.push_type == TPushType::DELETE) {
            req.request_version = Some(push.version);
        }
        req.report_version = Some(self.tablets().report_version());
        self.finish(req);
        self.remove(task.task_type, task.signature);
    }

    /// `drop_tablet_callback`.
    fn drop_tablet(&self, task: &TAgentTaskRequest) {
        let Some(req) = &task.drop_tablet_req else { return };
        let st = match self.tablets().drop_tablet(req) {
            Ok(()) => status::ok(),
            Err(e) => self.storage_error(&e),
        };
        let mut finish = self.finish_request(task, st);
        let info = doris_storage::tablet_info(req.tablet_id, req.schema_hash.unwrap_or(0), 0, 0, 0);
        finish.finish_tablet_infos = Some(vec![info]);
        self.finish(finish);
        self.remove(task.task_type, task.signature);
    }

    /// `publish_version_callback` (`EnginePublishVersionTask`): each partition's pending rowsets
    /// become visible at its version. The finishTask always carries `succ_tablets` (every tablet
    /// that holds the version, value 0), `error_tablet_ids`, the rows each base tablet gained by
    /// table, and the report version (not increased); its status is OK even with error tablets.
    fn publish(&self, task: &TAgentTaskRequest) {
        let mut succ = BTreeMap::new();
        let mut errors = Vec::new();
        let mut delta: BTreeMap<i64, BTreeMap<i64, i64>> = BTreeMap::new();
        let report_version;
        {
            let mut tablets = self.tablets();
            if let Some(req) = &task.publish_version_req {
                for p in &req.partition_version_infos {
                    let res = tablets.publish_txn(p.partition_id, req.transaction_id, p.version);
                    succ.extend(res.succ.iter().map(|t| (*t, 0i64)));
                    errors.extend(res.errors.iter().copied());
                    for (tablet, rows) in res.delta_rows {
                        if req.base_tablet_ids.as_ref().is_some_and(|b| !b.contains(&tablet)) {
                            continue;
                        }
                        if let Some(table) = tablets.tablet(tablet).map(|m| m.table_id()) {
                            delta.entry(table).or_default().insert(tablet, rows);
                        }
                    }
                }
            }
            report_version = tablets.report_version();
        }
        let mut finish = self.finish_request(task, status::ok());
        finish.succ_tablets = Some(succ);
        finish.error_tablet_ids = Some(errors);
        finish.table_id_to_tablet_id_to_delta_num_rows = Some(delta);
        finish.report_version = Some(report_version);
        self.finish(finish);
        self.remove(task.task_type, task.signature);
    }

    /// `clear_transaction_task_callback`: the transaction's pending rowsets are discarded; the
    /// finishTask has only the status, always OK.
    fn clear_transaction(&self, task: &TAgentTaskRequest) {
        if let Some(req) = &task.clear_transaction_task_req {
            if req.transaction_id > 0 {
                if let Err(e) = self.tablets().clear_txn(req.transaction_id, &req.partition_id) {
                    eprintln!("doris-rs be: clear transaction {}: {e}", req.transaction_id);
                }
            }
        }
        self.finish(self.finish_request(task, status::ok()));
        self.remove(task.task_type, task.signature);
    }

    /// The C++ status a storage error becomes.
    fn storage_error(&self, e: &StorageError) -> TStatus {
        let host = self.master.localhost();
        let msg = e.to_string();
        match e {
            StorageError::TabletNotFound { .. } => status::error(TStatusCode::NOT_FOUND, "NOT_FOUND", &host, &msg),
            StorageError::ReplicaMismatch { .. } => status::error(TStatusCode::ABORTED, "ABORTED", &host, &msg),
            StorageError::Unsupported { .. } => status::error(TStatusCode::NOT_IMPLEMENTED_ERROR, "NOT_IMPLEMENTED_ERROR", &host, &msg),
            _ => status::internal_error(&host, &msg),
        }
    }

    fn finish_request(&self, task: &TAgentTaskRequest, st: TStatus) -> TFinishTaskRequest {
        let (be_port, http_port, brpc_port) = self.ports;
        let backend = TBackend {
            host: self.master.localhost(),
            be_port,
            http_port,
            brpc_port: Some(brpc_port),
            is_alive: None,
            id: self.master.backend_id(),
        };
        TFinishTaskRequest::new(
            backend,
            task.task_type,
            task.signature,
            st,
            None,
            None::<Vec<TTabletInfo>>,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
    }

    /// `finish_task`: three tries, one second apart, then give up.
    fn finish(&self, req: TFinishTaskRequest) {
        for attempt in 1..=FINISH_TRIES {
            if self.fe.finish_task(req.clone()).is_ok() {
                return;
            }
            if attempt < FINISH_TRIES {
                std::thread::sleep(FINISH_RETRY_DELAY);
            }
        }
    }
}

/// `TTaskType` names (gensrc/thrift/Types.thrift).
pub fn task_type_name(t: TTaskType) -> String {
    let name = match t.0 {
        0 => "CREATE",
        1 => "DROP",
        2 => "PUSH",
        3 => "CLONE",
        4 => "STORAGE_MEDIUM_MIGRATE",
        5 => "ROLLUP",
        6 => "SCHEMA_CHANGE",
        7 => "CANCEL_DELETE",
        8 => "MAKE_SNAPSHOT",
        9 => "RELEASE_SNAPSHOT",
        10 => "CHECK_CONSISTENCY",
        11 => "UPLOAD",
        12 => "DOWNLOAD",
        13 => "CLEAR_REMOTE_FILE",
        14 => "MOVE",
        15 => "REALTIME_PUSH",
        16 => "PUBLISH_VERSION",
        17 => "CLEAR_ALTER_TASK",
        18 => "CLEAR_TRANSACTION_TASK",
        19 => "RECOVER_TABLET",
        20 => "STREAM_LOAD",
        21 => "UPDATE_TABLET_META_INFO",
        22 => "ALTER",
        23 => "INSTALL_PLUGIN",
        24 => "UNINSTALL_PLUGIN",
        25 => "COMPACTION",
        26 => "STORAGE_MEDIUM_MIGRATE_V2",
        27 => "NOTIFY_UPDATE_STORAGE_POLICY",
        28 => "PUSH_COOLDOWN_CONF",
        29 => "PUSH_STORAGE_POLICY",
        30 => "ALTER_INVERTED_INDEX",
        31 => "GC_BINLOG",
        32 => "CLEAN_TRASH",
        33 => "UPDATE_VISIBLE_VERSION",
        34 => "CLEAN_UDF_CACHE",
        35 => "PUSH_INDEX_POLICY",
        1000 => "CALCULATE_DELETE_BITMAP",
        1001 => "MAKE_CLOUD_COMMITTED_RS_VISIBLE",
        n => return format!("TTaskType({n})"),
    };
    name.to_string()
}
