//! Group commit (`GroupCommitMgr`, `LoadBlockQueue` in `group_commit_mgr.cpp`): loads into a
//! table with group commit on join one of the table's open queues, or start one by asking the FE
//! for an internal load (`streamLoadPut`). A queue commits its rows under the internal load's
//! transaction when its interval passes or its bytes reach the limit, then tells the FE
//! (`loadTxnCommit`, or `loadTxnRollback` when the write failed) and wakes the loads waiting on
//! it. The FE publishes the transaction as any other.
//!
//! Queued rows are held in memory: C++ also writes them to a write-ahead log, so that a restart
//! replays them; doris-rs loses a queue's rows if it stops before the commit.
use std::collections::HashMap;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use doris_exec::fragment::{plan_write, WriteFragment, WritePlan};
use doris_exec::sink::{commit_checked, Checked};
use doris_storage::TabletManager;
use doris_thrift::gen::frontend_service::{TLoadTxnCommitRequest, TLoadTxnRollbackRequest, TStreamLoadPutRequest};
use doris_thrift::gen::plan_nodes::TFileFormatType;
use doris_thrift::gen::status::{TStatus, TStatusCode};
use doris_thrift::gen::types::{TFileType, TUniqueId};
use thrift::OrderedFloat;

use crate::fe_client::FeClient;
use crate::master::MasterInfo;

/// `config::mow_stream_load_commit_retry_times`: `loadTxnCommit` calls at most.
const COMMIT_ATTEMPTS: usize = 5;

/// `config::txn_commit_rpc_timeout_ms`.
const COMMIT_RPC_TIMEOUT_MS: i64 = 180_000;

#[derive(Clone)]
pub struct GroupCommitMgr {
    inner: Arc<Inner>,
}

struct Inner {
    tablets: Arc<Mutex<TabletManager>>,
    fe: Arc<dyn FeClient>,
    master: MasterInfo,
    /// Each table's open queues. A table's lock is held while a queue is created, so two loads
    /// never start two queues for one table at once.
    tables: Mutex<HashMap<i64, Arc<TableQueues>>>,
    /// Tests only: runs after a load chose a queue, before it adds its rows.
    join_hook: Mutex<Option<JoinHook>>,
}

type JoinHook = Arc<dyn Fn() + Send + Sync>;

/// A table's open queues.
type TableQueues = Mutex<Vec<Arc<Queue>>>;

/// One internal load: the rows of the loads that joined it, committed together.
pub struct Queue {
    label: String,
    txn_id: i64,
    /// The table id its loads name: its key in `tables`.
    key: i64,
    db_id: Option<i64>,
    table_id: i64,
    schema_version: i64,
    index_size: usize,
    interval: Duration,
    data_bytes: i64,
    start: Instant,
    internal: WriteFragment,
    state: Mutex<State>,
    changed: Condvar,
}

#[derive(Default)]
struct State {
    rows: Vec<Checked>,
    bytes: i64,
    need_commit: bool,
    /// The internal load's outcome, once it ended.
    finished: Option<TStatus>,
}

fn status(code: TStatusCode, msg: Option<String>) -> TStatus {
    TStatus {
        status_code: code,
        error_msgs: msg.map(|m| vec![m]),
    }
}

fn random_id() -> TUniqueId {
    use std::hash::{BuildHasher, Hasher};
    let word = || {
        let mut h = std::collections::hash_map::RandomState::new().build_hasher();
        h.write_u128(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0),
        );
        h.finish() as i64
    };
    TUniqueId { hi: word(), lo: word() }
}

impl Queue {
    /// `group_commit_<load id>`: the internal load's label, which the FE gives the client.
    pub fn label(&self) -> &str {
        &self.label
    }

    pub fn txn_id(&self) -> i64 {
        self.txn_id
    }

    /// The internal load's outcome, if it ended.
    pub fn finished(&self) -> Option<TStatus> {
        self.state.lock().unwrap_or_else(|e| e.into_inner()).finished.clone()
    }

    /// Waits for the internal load to end (a sync load), or until `stop` says to stop waiting
    /// (checked every 50 ms); None when stopped first.
    pub fn wait(&self, stop: impl Fn() -> bool) -> Option<TStatus> {
        let mut s = self.state.lock().unwrap_or_else(|e| e.into_inner());
        loop {
            if let Some(st) = &s.finished {
                return Some(st.clone());
            }
            if stop() {
                return None;
            }
            s = self
                .changed
                .wait_timeout(s, Duration::from_millis(50))
                .unwrap_or_else(|e| e.into_inner())
                .0;
        }
    }

    /// `add_load_id` and `add_block` at once: the rows join if the queue is still open (under
    /// the same lock as the committer's check), and `rows` is emptied; false if it closed.
    /// A new queue (`first`) takes the load that started it whatever its interval.
    fn try_add(&self, rows: &mut Option<Checked>, first: bool) -> bool {
        let mut s = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if !first && !self.open(&mut s) {
            return false;
        }
        if let Some(r) = rows.take() {
            s.bytes += r.bytes();
            s.rows.push(r);
        }
        if s.bytes >= self.data_bytes {
            s.need_commit = true;
        }
        drop(s);
        self.changed.notify_all();
        true
    }

    /// Whether a load may still join: not full, not past its interval, not ended.
    fn open(&self, s: &mut State) -> bool {
        if self.start.elapsed() >= self.interval {
            s.need_commit = true;
        }
        !s.need_commit && s.finished.is_none()
    }
}

impl GroupCommitMgr {
    pub fn new(tablets: Arc<Mutex<TabletManager>>, fe: Arc<dyn FeClient>, master: MasterInfo) -> GroupCommitMgr {
        GroupCommitMgr {
            inner: Arc::new(Inner {
                tablets,
                fe,
                master,
                tables: Mutex::new(HashMap::new()),
                join_hook: Mutex::new(None),
            }),
        }
    }

    /// `GroupCommitTable::get_first_block_load_queue` and `LoadBlockQueue::add_block`: a load's
    /// checked rows join an open queue of its table with the same schema version and index
    /// count, or a new one. The error is the FE's when it refuses the internal load.
    pub fn join(&self, f: &WriteFragment, rows: Checked) -> Result<Arc<Queue>, String> {
        let table_id = f.sink.table_id;
        let schema_version = f.sink.base_schema_version.unwrap_or(0);
        let index_size = f.sink.schema.indexes.len();
        let table = Arc::clone(self.inner.tables.lock().unwrap_or_else(|e| e.into_inner()).entry(table_id).or_default());
        let mut rows = Some(rows);
        loop {
            let mut queues = table.lock().unwrap_or_else(|e| e.into_inner());
            queues.retain(|q| q.state.lock().unwrap_or_else(|e| e.into_inner()).finished.is_none());
            let found = queues
                .iter()
                .find(|q| {
                    q.schema_version == schema_version && q.index_size == index_size && q.open(&mut q.state.lock().unwrap_or_else(|e| e.into_inner()))
                })
                .cloned();
            let Some(queue) = found else {
                let q = Arc::new(self.create(table_id, schema_version, index_size)?);
                // the rows are in before the committer can run, and the queue is listed only
                // once it has a committer
                q.try_add(&mut rows, true);
                let inner = Arc::clone(&self.inner);
                let committer = Arc::clone(&q);
                std::thread::Builder::new()
                    .name("group-commit".into())
                    .spawn(move || inner.commit_when_due(&committer))
                    .map_err(|e| format!("cannot start the group commit: {e}"))?;
                queues.push(Arc::clone(&q));
                return Ok(q);
            };
            drop(queues);
            let hook = self.inner.join_hook.lock().unwrap_or_else(|e| e.into_inner()).clone();
            if let Some(h) = hook {
                h();
            }
            // the queue may have closed since it was chosen: then choose again
            if queue.try_add(&mut rows, false) {
                return Ok(queue);
            }
        }
    }

    /// Tables with an open (or committing) queue.
    pub fn tables_held(&self) -> usize {
        let tables: Vec<Arc<TableQueues>> = self.inner.tables.lock().unwrap_or_else(|e| e.into_inner()).values().cloned().collect();
        tables.iter().filter(|t| !t.lock().unwrap_or_else(|e| e.into_inner()).is_empty()).count()
    }

    /// Tests only: `hook` runs each time a load has chosen its queue, before it adds its rows.
    #[doc(hidden)]
    pub fn set_join_hook_for_tests(&self, hook: impl Fn() + Send + Sync + 'static) {
        *self.inner.join_hook.lock().unwrap_or_else(|e| e.into_inner()) = Some(Arc::new(hook));
    }

    /// `_create_group_commit_load`: the FE plans an internal load reading the table's queue.
    fn create(&self, table_id: i64, schema_version: i64, index_size: usize) -> Result<Queue, String> {
        let load_id = random_id();
        let label = format!("group_commit_{:016x}_{:016x}", load_id.hi as u64, load_id.lo as u64);
        let put = TStreamLoadPutRequest {
            cluster: None,
            user: "admin".into(),
            passwd: String::new(),
            db: String::new(),
            tbl: String::new(),
            user_ip: None,
            load_id,
            txn_id: 0,
            // unset in C++: the first values
            file_type: TFileType(0),
            format_type: TFileFormatType(0),
            path: None,
            columns: None,
            where_: None,
            column_separator: None,
            partitions: None,
            auth_code: None,
            negative: None,
            timeout: None,
            strict_mode: Some(false),
            timezone: None,
            exec_mem_limit: None,
            is_temp_partition: None,
            strip_outer_array: None,
            jsonpaths: None,
            thrift_rpc_timeout_ms: None,
            json_root: None,
            merge_type: None,
            delete_condition: None,
            sequence_col: None,
            num_as_string: None,
            fuzzy_parse: None,
            line_delimiter: None,
            read_json_by_line: None,
            token: Some("group_commit".into()),
            send_batch_parallelism: None,
            max_filter_ratio: Some(OrderedFloat(1.0)),
            load_to_single_tablet: None,
            header_type: None,
            hidden_columns: None,
            compress_type: None,
            file_size: None,
            trim_double_quotes: None,
            skip_lines: None,
            enable_profile: None,
            partial_update: Some(false),
            table_names: None,
            load_sql: Some(format!(
                "insert into doris_internal_table_id({table_id}) WITH LABEL {label} select * from group_commit(\"table_id\"=\"{table_id}\")"
            )),
            backend_id: self.inner.master.backend_id(),
            version: None,
            label: Some(label.clone()),
            enclose: None,
            escape: None,
            memtable_on_sink_node: None,
            group_commit: None,
            stream_per_node: None,
            group_commit_mode: None,
            unique_key_update_mode: None,
            partial_update_new_key_policy: None,
            empty_field_as_null: None,
            cert_based_auth: None,
            cloud_cluster: None,
            table_id: None,
        };
        let reply = self.inner.fe.stream_load_put(put).map_err(|e| e.to_string())?;
        if reply.status.status_code != TStatusCode::OK {
            return Err(reply
                .status
                .error_msgs
                .and_then(|m| m.into_iter().next())
                .unwrap_or_else(|| "the FE refused the group commit load".into()));
        }
        let params = reply.pipeline_params.ok_or("the FE planned no group commit load")?;
        let mut internal = plan_write(&params).map_err(|e| e.to_string())?;
        if internal.plan != WritePlan::GroupCommit {
            return Err(format!("a group commit load planned as {:?}", internal.plan));
        }
        if internal.backend_id == 0 {
            internal.backend_id = self.inner.master.backend_id().unwrap_or(0);
        }
        Ok(Queue {
            label,
            txn_id: internal.sink.txn_id,
            key: table_id,
            db_id: reply.db_id,
            table_id: reply.table_id.unwrap_or(table_id),
            schema_version,
            index_size,
            interval: Duration::from_millis(reply.group_commit_interval_ms.unwrap_or(10_000).max(0) as u64),
            data_bytes: reply.group_commit_data_bytes.unwrap_or(134_217_728),
            start: Instant::now(),
            internal,
            state: Mutex::new(State::default()),
            changed: Condvar::new(),
        })
    }
}

impl Inner {
    /// The queue's committer: waits until it must commit, then `_finish_group_commit_load`.
    fn commit_when_due(&self, q: &Queue) {
        let rows = {
            let mut s = q.state.lock().unwrap_or_else(|e| e.into_inner());
            loop {
                let due = q.start + q.interval;
                let now = Instant::now();
                if s.need_commit || now >= due {
                    s.need_commit = true;
                    break;
                }
                s = q.changed.wait_timeout(s, due - now).unwrap_or_else(|e| e.into_inner()).0;
            }
            std::mem::take(&mut s.rows)
        };
        let bytes: i64 = rows.iter().map(Checked::bytes).sum();
        let ran = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            q.internal.in_context(|| commit_checked(&q.internal, rows, &self.tablets))
        }));
        let written = match ran {
            Ok(o) => o,
            Err(_) => doris_exec::sink::SinkOutcome::failed("the group commit write panicked".into()),
        };
        let token = self.master.with(|s| s.auth_token.clone());
        let outcome = if written.status.status_code == TStatusCode::OK {
            let req = TLoadTxnCommitRequest {
                cluster: None,
                user: String::new(),
                passwd: String::new(),
                db: String::new(),
                tbl: String::new(),
                user_ip: None,
                txn_id: q.txn_id,
                sync: false,
                commit_infos: Some(written.commit_infos.clone()),
                auth_code: Some(0),
                txn_commit_attachment: None,
                thrift_rpc_timeout_ms: Some(COMMIT_RPC_TIMEOUT_MS),
                token,
                db_id: q.db_id,
                tbls: None,
                table_id: Some(q.table_id),
                auth_code_uuid: None,
                group_commit: Some(true),
                receive_bytes: Some(bytes),
                backend_id: self.master.backend_id(),
                cert_based_auth: None,
            };
            // retried on a transport error or DELETE_BITMAP_LOCK_ERROR; any other answer ends it.
            // The FE's answer is not the loads' outcome: C++ leaves a failed commit to the WAL
            // replay, and its sync loads see only the write's status.
            for _ in 0..COMMIT_ATTEMPTS {
                match self.fe.load_txn_commit(req.clone()) {
                    Ok(r) if r.status.status_code == TStatusCode::DELETE_BITMAP_LOCK_ERROR => {}
                    Ok(_) => break,
                    Err(_) => {}
                }
            }
            written.status
        } else {
            let reason = written.status.error_msgs.clone().unwrap_or_default().join("; ");
            let req = TLoadTxnRollbackRequest {
                cluster: None,
                user: String::new(),
                passwd: String::new(),
                db: String::new(),
                tbl: String::new(),
                user_ip: None,
                txn_id: q.txn_id,
                reason: Some(format!("[{}]{reason}", crate::status::code_name(written.status.status_code))),
                auth_code: Some(0),
                txn_commit_attachment: None,
                token,
                db_id: q.db_id,
                tbls: None,
                auth_code_uuid: None,
                label: None,
            };
            let _ = self.fe.load_txn_rollback(req);
            // LoadBlockQueue::cancel
            let msg = format!(
                "cancel group_commit, label={}, status=[{}]{reason}",
                q.label,
                crate::status::code_name(written.status.status_code)
            );
            status(TStatusCode::CANCELLED, Some(msg))
        };
        q.state.lock().unwrap_or_else(|e| e.into_inner()).finished = Some(outcome);
        q.changed.notify_all();
        // the ended queue (and its plan) leaves its table's list; the table's entry stays, empty
        let table = self.tables.lock().unwrap_or_else(|e| e.into_inner()).get(&q.key).cloned();
        if let Some(t) = table {
            t.lock().unwrap_or_else(|e| e.into_inner()).retain(|x| !std::ptr::eq(Arc::as_ptr(x), q));
        }
    }
}
