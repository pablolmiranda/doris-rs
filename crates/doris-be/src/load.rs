//! Loads: `INSERT … VALUES` fragments the FE sends over brpc (`exec_plan_fragment`, answered at
//! once, then run, then one final `reportExecStatus`), and stream loads over HTTP
//! (`StreamLoadAction` and `StreamLoadExecutor`: loadTxnBegin, streamLoadPut, run the returned
//! fragment over the body, loadTxnCommit or loadTxnRollback, and the JSON reply).
use std::collections::{BTreeMap, HashMap};
use std::hash::{BuildHasher, Hasher};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use doris_exec::file_scan::stream_rows;
use doris_exec::fragment::{decode_fragments, plan_write, WriteFragment, WritePlan};
use doris_exec::sink::{exec_status, run_sink, SinkOutcome, TableSink};
use doris_exec::ExecError;
use doris_proto::doris::{PCancelPlanFragmentRequest, PCancelPlanFragmentResult, PExecPlanFragmentRequest, PExecPlanFragmentResult, PStatus};
use doris_storage::TabletManager;
use doris_thrift::gen::backend_service::TStreamLoadRecord;
use doris_thrift::gen::data_sinks::TGroupCommitMode;
use doris_thrift::gen::frontend_service::TReportExecStatusParams;
use doris_thrift::gen::frontend_service::{TLoadTxnBeginRequest, TLoadTxnCommitRequest, TLoadTxnRollbackRequest, TStreamLoadPutRequest};
use doris_thrift::gen::plan_nodes::{TFileCompressType, TFileFormatType};
use doris_thrift::gen::status::TStatus;
use doris_thrift::gen::status::TStatusCode;
use doris_thrift::gen::types::{TFileType, TMergeType, TUniqueId};
use thrift::OrderedFloat;

use crate::fe_client::FeClient;
use crate::group_commit::{GroupCommitMgr, Queue};
use crate::master::MasterInfo;

/// `config::thrift_rpc_timeout_ms` and `txn_commit_rpc_timeout_ms`.
const PUT_RPC_TIMEOUT_MS: i32 = 60_000;
const COMMIT_RPC_TIMEOUT_MS: i32 = 180_000;
/// `stream_load_record_batch_size` (the oracle's be.conf).
const RECORD_BATCH: usize = 500;
const OK: i32 = 0;
const NOT_IMPLEMENTED_ERROR: i32 = 3;
const INTERNAL_ERROR: i32 = 6;

/// A stream load as the HTTP layer hands it over.
#[derive(Debug, Clone)]
pub struct StreamLoadRequest {
    pub db: String,
    pub table: String,
    /// Header names lower-cased.
    pub headers: BTreeMap<String, String>,
    /// The body, or why it could not be read whole (a client gone part way, a body over the limit).
    pub body: Result<Vec<u8>, String>,
    pub client_ip: String,
}

/// Where a cancel of a waiting sync load leaves its status.
type CancelSlot = Arc<Mutex<Option<PStatus>>>;

#[derive(Clone)]
pub struct LoadService {
    inner: Arc<Inner>,
}

struct Inner {
    tablets: Arc<Mutex<TabletManager>>,
    group_commit: GroupCommitMgr,
    /// Sync group-commit loads waiting for their queue, by query id: set to the cancel's
    /// status when the FE cancels one.
    sync_waits: Mutex<HashMap<(i64, i64), CancelSlot>>,
    fe: Arc<dyn FeClient>,
    master: MasterInfo,
    http_port: u16,
    error_dir: PathBuf,
    running: Mutex<usize>,
    idle: Condvar,
    /// `streaming_load_max_mb`, in bytes
    max_body_bytes: AtomicU64,
    /// `LoadPathMgr::_next_shard`: the next `__shard_` a saved body goes to
    next_shard: AtomicU64,
}

const MEBIBYTE: u64 = 1 << 20;

/// `LoadPathMgr`'s shards of saved bodies.
const MAX_SHARD_NUM: u64 = 1024;

fn random_u64() -> u64 {
    let mut h = std::collections::hash_map::RandomState::new().build_hasher();
    h.write_u128(SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_nanos()).unwrap_or(0));
    h.finish()
}

fn now_ms() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0)
}

fn pstatus(code: i32, msg: Option<String>) -> PStatus {
    PStatus {
        status_code: code,
        error_msgs: msg.into_iter().collect(),
    }
}

/// Where load error logs go: `<first data dir>/error_log`.
pub(crate) fn error_dir(tablets: &Mutex<TabletManager>) -> PathBuf {
    let mgr = tablets.lock().unwrap_or_else(|e| e.into_inner());
    mgr.data_dirs()
        .first()
        .map(|d| d.root().join("error_log"))
        .unwrap_or_else(|| PathBuf::from("error_log"))
}

impl LoadService {
    pub fn new(tablets: Arc<Mutex<TabletManager>>, fe: Arc<dyn FeClient>, master: MasterInfo, http_port: u16) -> LoadService {
        let error_dir = error_dir(&tablets);
        LoadService {
            inner: Arc::new(Inner {
                group_commit: GroupCommitMgr::new(Arc::clone(&tablets), Arc::clone(&fe), master.clone()),
                sync_waits: Mutex::new(HashMap::new()),
                tablets,
                fe,
                master,
                http_port,
                error_dir,
                running: Mutex::new(0),
                idle: Condvar::new(),
                max_body_bytes: AtomicU64::new(102_400 * MEBIBYTE),
                next_shard: AtomicU64::new(0),
            }),
        }
    }

    /// `streaming_load_max_mb`: the largest stream load body read (default 102400).
    pub fn set_streaming_load_max_mb(&self, mb: u64) {
        self.inner.max_body_bytes.store(mb.saturating_mul(MEBIBYTE), Ordering::Relaxed);
    }

    /// The largest stream load body, in bytes, the HTTP layer reads.
    pub fn max_body_bytes(&self) -> u64 {
        self.inner.max_body_bytes.load(Ordering::Relaxed)
    }

    /// `PInternalService::exec_plan_fragment`: a write fragment is accepted (or refused) at
    /// once and runs in the background; its outcome reaches the FE in `reportExecStatus`.
    pub fn exec_plan_fragment(&self, req: &PExecPlanFragmentRequest) -> PExecPlanFragmentResult {
        let received = now_ms();
        let refuse = |code: i32, msg: String| PExecPlanFragmentResult {
            status: pstatus(code, Some(msg)),
            received_time: Some(received),
            execution_time: Some(now_ms()),
            execution_done_time: Some(now_ms()),
        };
        let planned = decode_fragments(req).and_then(|list| {
            let params = list.params_list.unwrap_or_default();
            let [p] = params.as_slice() else {
                return Err(ExecError::Unsupported {
                    what: format!("a request of {} fragments", params.len()),
                });
            };
            let f = plan_write(p)?;
            if f.plan != WritePlan::Values {
                return Err(ExecError::Unsupported {
                    what: "a file scan outside a stream load".into(),
                });
            }
            Ok(f)
        });
        let f = match planned {
            Ok(f) => f,
            Err(e @ ExecError::Unsupported { .. }) => return refuse(NOT_IMPLEMENTED_ERROR, e.to_string()),
            Err(e) => return refuse(INTERNAL_ERROR, e.to_string()),
        };
        let rows = match f.in_context(|| f.values_rows()) {
            Ok(r) => r,
            Err(e) => return refuse(NOT_IMPLEMENTED_ERROR, e.to_string()),
        };
        *self.inner.running.lock().unwrap_or_else(|e| e.into_inner()) += 1;
        let inner = Arc::clone(&self.inner);
        let spawned = std::thread::Builder::new().name("load-insert".into()).spawn(move || {
            // a panic in the sink still reaches the FE as a failed fragment, not a timeout
            let ran = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                if f.group_commit.is_some() {
                    return f.in_context(|| inner.group_commit_insert(&f, rows));
                }
                let outcome = f.in_context(|| run_sink(&f, rows, &inner.tablets, &inner.error_dir));
                let url = inner.tracking_url(&outcome);
                exec_status(&f, &outcome, url)
            }));
            let params = ran.unwrap_or_else(|p| {
                let what = p
                    .downcast_ref::<&str>()
                    .map(|s| s.to_string())
                    .or_else(|| p.downcast_ref::<String>().cloned())
                    .unwrap_or_default();
                exec_status(&f, &SinkOutcome::failed(format!("the write panicked: {what}")), None)
            });
            inner.report(&f, params);
            inner.done();
        });
        if let Err(e) = spawned {
            self.inner.done();
            return refuse(INTERNAL_ERROR, format!("cannot start the write: {e}"));
        }
        PExecPlanFragmentResult {
            status: pstatus(OK, None),
            received_time: Some(received),
            execution_time: Some(now_ms()),
            execution_done_time: Some(now_ms()),
        }
    }

    /// `PInternalService::cancel_plan_fragment`: a write runs to its end, except a sync group
    /// commit load waiting for its queue, which stops waiting and reports the cancel. FINISHED,
    /// the FE's usual reason after a load, is a no-op.
    pub fn cancel_plan_fragment(&self, req: &PCancelPlanFragmentRequest) -> PCancelPlanFragmentResult {
        let finished = req.cancel_status.as_ref().is_some_and(|s| s.status_code == TStatusCode::FINISHED.0);
        if let (Some(q), false) = (&req.query_id, finished) {
            let wait = self
                .inner
                .sync_waits
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .get(&(q.hi, q.lo))
                .cloned();
            if let Some(w) = wait {
                let status = req
                    .cancel_status
                    .clone()
                    .unwrap_or_else(|| pstatus(TStatusCode::CANCELLED.0, Some("cancelled".into())));
                *w.lock().unwrap_or_else(|e| e.into_inner()) = Some(status);
            }
        }
        PCancelPlanFragmentResult { status: pstatus(OK, None) }
    }

    /// Waits until no insert is running; false on timeout.
    pub fn wait_idle(&self, timeout: Duration) -> bool {
        let n = self.inner.running.lock().unwrap_or_else(|e| e.into_inner());
        let (_g, res) = self
            .inner
            .idle
            .wait_timeout_while(n, timeout, |n| *n > 0)
            .unwrap_or_else(|e| e.into_inner());
        !res.timed_out()
    }

    /// `GET /api/_load_error_log?file=<name>`: the file, if it is one of the BE's error logs.
    pub fn error_log(&self, file: &str) -> Option<Vec<u8>> {
        if file.contains("..") || file.starts_with('/') {
            return None;
        }
        std::fs::read(self.inner.error_dir.join(file)).ok()
    }

    /// `BackendService.get_stream_load_record`: records finished after `last_ms`.
    pub fn stream_load_records(&self, last_ms: i64) -> BTreeMap<String, TStreamLoadRecord> {
        let mgr = self.inner.tablets.lock().unwrap_or_else(|e| e.into_inner());
        mgr.stream_load_records(&last_ms.to_string(), RECORD_BATCH)
            .into_iter()
            .filter_map(|(k, v)| Some((k, record_from_json(&serde_json::from_str(&v).ok()?))))
            .collect()
    }

    /// A stream load, start to end; the reply's JSON.
    pub fn stream_load(&self, req: StreamLoadRequest) -> String {
        let mut load = StreamLoad::new(&self.inner, req);
        load.run();
        let reply = load.reply_json();
        load.record(&self.inner);
        reply
    }
}

impl Inner {
    /// `_coordinator_callback`: the fragment's report, with one retry on a transport error.
    fn report(&self, f: &WriteFragment, params: TReportExecStatusParams) {
        let coord = f.coord.clone().or_else(|| self.master.fe_address());
        if let Some(coord) = coord {
            if self.fe.report_exec_status(&coord, params.clone()).is_err() {
                let _ = self.fe.report_exec_status(&coord, params);
            }
        }
    }

    /// A GROUP_COMMIT_BLOCK_SINK (`GroupCommitBlockSinkOperatorX`) fed one block: the rows are
    /// checked and join a queue of the table. An async load ends there; a sync load waits for the
    /// queue's internal load to end, or for `cancelled` to give the status to end with. The
    /// outcome's status is the load's; the queue is the one it joined, if any.
    fn group_commit_rows(
        &self,
        f: &WriteFragment,
        rows: Vec<Result<Vec<doris_exec::value::Value>, doris_exec::RowError>>,
        cancelled: &dyn Fn() -> Option<TStatus>,
    ) -> (SinkOutcome, Option<Arc<Queue>>) {
        let sink = match TableSink::new(f, vec![f.instance_id.clone()]) {
            Ok(s) => s,
            Err(e) => return (SinkOutcome::failed(e.to_string()), None),
        };
        if let Err(e) = sink.push(0, rows, &f.input_nullable()) {
            let failure = TStatus {
                status_code: TStatusCode::CANCELLED,
                error_msgs: Some(vec![e.to_string()]),
            };
            return (sink.finish(&self.tablets, &self.error_dir, Some(failure)), None);
        }
        let (mut outcome, checked) = sink.check(&self.error_dir);
        // sink_impl's wind_up: too many filtered rows fail the load before it joins a queue
        let ratio = f.sink.max_filter_ratio.map_or(0.0, |r| r.0);
        let selected = outcome.loaded + outcome.filtered;
        if selected > 0 && outcome.filtered as f64 / selected as f64 > ratio {
            let msg = format!("too many filtered rows, {}", outcome.first_error.clone().unwrap_or_default());
            outcome.status = TStatus {
                status_code: TStatusCode::DATA_QUALITY_ERROR,
                error_msgs: Some(vec![msg]),
            };
            return (outcome, None);
        }
        let queue = match self.group_commit.join(f, checked) {
            Ok(q) => q,
            Err(msg) => {
                outcome.status = TStatus {
                    status_code: TStatusCode::INTERNAL_ERROR,
                    error_msgs: Some(vec![msg]),
                };
                return (outcome, None);
            }
        };
        if f.group_commit == Some(TGroupCommitMode::SYNC_MODE) {
            outcome.status = match queue.wait(|| cancelled().is_some()) {
                Some(st) => st,
                None => cancelled().unwrap_or(TStatus {
                    status_code: TStatusCode::CANCELLED,
                    error_msgs: None,
                }),
            };
        }
        (outcome, Some(queue))
    }

    /// An `INSERT … VALUES` into a GROUP_COMMIT_BLOCK_SINK: an async load reports at once with
    /// the queue's label and transaction, a sync load once the queue's internal load ends or the
    /// FE cancels the fragment.
    fn group_commit_insert(
        &self,
        f: &WriteFragment,
        rows: Vec<Result<Vec<doris_exec::value::Value>, doris_exec::RowError>>,
    ) -> TReportExecStatusParams {
        let key = (f.query_id.hi, f.query_id.lo);
        let cancel: CancelSlot = Arc::default();
        self.sync_waits.lock().unwrap_or_else(|e| e.into_inner()).insert(key, Arc::clone(&cancel));
        let cancelled = || {
            // the fragment's cancel reason, as its report words it
            let c = cancel.lock().unwrap_or_else(|e| e.into_inner()).clone()?;
            let code = TStatusCode(c.status_code);
            let msg = c.error_msgs.first().cloned().unwrap_or_default();
            Some(TStatus {
                status_code: code,
                error_msgs: Some(vec![format!(
                    "({})[{}]PStatus: {msg}",
                    self.master.localhost(),
                    crate::status::code_name(code)
                )]),
            })
        };
        let (outcome, queue) = self.group_commit_rows(f, rows, &cancelled);
        self.sync_waits.lock().unwrap_or_else(|e| e.into_inner()).remove(&key);
        let url = self.tracking_url(&outcome);
        let mut params = exec_status(f, &outcome, url);
        if let Some(q) = queue {
            params.txn_id = Some(q.txn_id());
            params.label = Some(q.label().to_string());
        }
        params
    }

    fn done(&self) {
        let mut n = self.running.lock().unwrap_or_else(|e| e.into_inner());
        *n -= 1;
        self.idle.notify_all();
    }

    fn tracking_url(&self, o: &SinkOutcome) -> Option<String> {
        o.error_log
            .as_ref()
            .map(|f| format!("http://{}:{}/api/_load_error_log?file={f}", self.master.localhost(), self.http_port))
    }
}

/// `StreamLoadContext`: what the load learned, for the reply and the record.
struct StreamLoad<'a> {
    inner: &'a Inner,
    req: StreamLoadRequest,
    label: String,
    user: String,
    passwd: String,
    token: Option<String>,
    user_ip: Option<String>,
    load_id: TUniqueId,
    txn_id: i64,
    db_id: Option<i64>,
    started: Instant,
    start_ms: i64,
    status: Option<String>,
    publish_timeout: bool,
    total: i64,
    loaded: i64,
    filtered: i64,
    unselected: i64,
    error_url: Option<String>,
    first_error: Option<String>,
    times: [u128; 6],
    /// The `group_commit` header's mode when the load goes through group commit.
    group_commit: Option<String>,
}

/// `Basic <base64(user:passwd)>`.
fn basic_auth(h: &str) -> Option<(String, String)> {
    let b64 = h.strip_prefix("Basic ")?.trim();
    let mut bits = 0u32;
    let mut nbits = 0;
    let mut out = Vec::new();
    for c in b64.bytes() {
        let v = match c {
            b'A'..=b'Z' => c - b'A',
            b'a'..=b'z' => c - b'a' + 26,
            b'0'..=b'9' => c - b'0' + 52,
            b'+' => 62,
            b'/' => 63,
            _ => continue,
        };
        bits = bits << 6 | u32::from(v);
        nbits += 6;
        if nbits >= 8 {
            nbits -= 8;
            out.push((bits >> nbits) as u8);
        }
    }
    let text = String::from_utf8(out).ok()?;
    let (u, p) = text.split_once(':')?;
    Some((u.to_string(), p.to_string()))
}

fn uuid() -> String {
    let (a, b) = (random_u64(), random_u64());
    format!(
        "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
        a >> 32,
        (a >> 16) & 0xffff,
        a & 0xfff | 0x4000,
        (b >> 48) & 0x3fff | 0x8000,
        b & 0xffff_ffff_ffff
    )
}

impl<'a> StreamLoad<'a> {
    fn new(inner: &'a Inner, req: StreamLoadRequest) -> StreamLoad<'a> {
        let h = |k: &str| req.headers.get(k).filter(|v| !v.is_empty()).cloned();
        let (user, passwd) = h("authorization").and_then(|a| basic_auth(&a)).unwrap_or_default();
        let token = h("auth-token");
        let user_ip = if token.is_some() { None } else { Some(req.client_ip.clone()) };
        StreamLoad {
            inner,
            label: h("label").unwrap_or_else(uuid),
            user,
            passwd,
            token,
            user_ip,
            load_id: TUniqueId {
                hi: random_u64() as i64,
                lo: random_u64() as i64,
            },
            txn_id: -1,
            db_id: None,
            started: Instant::now(),
            start_ms: now_ms(),
            status: None,
            publish_timeout: false,
            total: 0,
            loaded: 0,
            filtered: 0,
            unselected: 0,
            error_url: None,
            first_error: None,
            times: [0; 6],
            group_commit: None,
            req,
        }
    }

    fn header(&self, k: &str) -> Option<String> {
        self.req.headers.get(k).filter(|v| !v.is_empty()).cloned()
    }

    /// `StreamLoadAction::_can_group_commit`: whether the FE may answer with a group commit
    /// mode (only then is `use_table_group_commit_mode` set).
    fn can_group_commit(&self) -> bool {
        let chunked = self.header("transfer-encoding").is_some_and(|t| t.contains("chunked"));
        let empty = self.header("content-length").and_then(|l| l.parse::<i64>().ok()).unwrap_or(0) == 0;
        if empty && !chunked {
            return false;
        }
        let update_mode = self
            .header("unique_key_update_mode")
            .is_some_and(|m| m.eq_ignore_ascii_case("UPDATE_FIXED_COLUMNS") || m.eq_ignore_ascii_case("UPDATE_FLEXIBLE_COLUMNS"));
        !(self.header("partial_columns").is_some_and(|v| v.eq_ignore_ascii_case("true"))
            || self.header("partitions").is_some()
            || self.header("temporary_partitions").is_some()
            || self.header("two_phase_commit").is_some_and(|v| v.eq_ignore_ascii_case("true"))
            || update_mode)
    }

    fn fail(&mut self, msg: String) {
        if self.status.is_none() {
            self.status = Some(msg);
        }
    }

    /// `StreamLoadAction::_handle_group_commit`: the `group_commit` header's mode when the load
    /// goes through group commit (not `off_mode`, and `_can_group_commit`).
    fn group_commit_mode(&self) -> Result<Option<String>, String> {
        let Some(mode) = self.header("group_commit") else { return Ok(None) };
        if !["sync_mode", "async_mode", "off_mode"].iter().any(|m| mode.eq_ignore_ascii_case(m)) {
            return Err("[INVALID_ARGUMENT]group_commit can only be [async_mode, sync_mode, off_mode]".into());
        }
        if mode.eq_ignore_ascii_case("off_mode") || !self.can_group_commit() {
            return Ok(None);
        }
        // a chunked body forgets the label
        let chunked = self.header("transfer-encoding").is_some_and(|t| t.contains("chunked"));
        if self.header("label").is_some() && !chunked {
            return Err("[INVALID_ARGUMENT]label and group_commit can't be set at the same time".into());
        }
        Ok(Some(mode))
    }

    fn run(&mut self) {
        match self.group_commit_mode() {
            Ok(Some(mode)) => {
                // the queue's label is the load's
                self.label = String::new();
                self.group_commit = Some(mode);
            }
            Ok(None) => {}
            Err(msg) => return self.fail(msg),
        }
        let fmt = self.header("format").unwrap_or_else(|| "csv".into()).to_ascii_lowercase();
        // StreamLoadAction::_on_header: a header-carrying CSV is read as CSV
        let read_as = if self.header_type().is_empty() { fmt.as_str() } else { "csv" };
        let Some((format, compress)) = parse_format(read_as, &self.header("compress_type").unwrap_or_default()) else {
            self.fail(format!("[NOT_IMPLEMENTED_ERROR]doris-rs does not support stream load format {fmt} yet"));
            return;
        };
        // _on_header: Content-Length against streaming_load_max_mb, before the transaction begins
        let max = self.inner.max_body_bytes.load(Ordering::Relaxed);
        if let Some(len) = self
            .header("content-length")
            .and_then(|l| l.trim().parse::<u64>().ok())
            .filter(|l| *l > max)
        {
            return self.fail(format!(
                "[EXCEEDED_LIMIT]body size {len} bytes ({:.2} MiB) exceeds the limit of {max} bytes ({} MiB) set by BE's conf streaming_load_max_mb. Increase it if you are sure this load is reasonable",
                len as f64 / MEBIBYTE as f64,
                max / MEBIBYTE
            ));
        }
        // a body not read whole loads nothing: no transaction, so the label stays free
        if let Err(e) = &self.req.body {
            return self.fail(format!("[INTERNAL_ERROR]cannot read the body: {e}"));
        }
        // _on_header: the timeout is read before the transaction begins
        let timeout = match self.header("timeout").filter(|t| !t.is_empty()).map(|t| stoi(&t, "timeout")).transpose() {
            Ok(t) => t,
            Err(msg) => return self.fail(msg),
        };
        let fe = Arc::clone(&self.inner.fe);
        // a group commit load has no transaction of its own: the queue's internal load does
        if self.group_commit.is_none() && !self.begin(fe.as_ref(), timeout) {
            return;
        }
        self.put_and_load(fe.as_ref(), format, compress, timeout, max);
    }

    /// `loadTxnBegin`; false when the load failed (and was rolled back if it began).
    fn begin(&mut self, fe: &dyn FeClient, timeout: Option<i32>) -> bool {
        let t = Instant::now();
        let begin = TLoadTxnBeginRequest {
            cluster: None,
            user: self.user.clone(),
            passwd: self.passwd.clone(),
            db: self.req.db.clone(),
            tbl: self.req.table.clone(),
            user_ip: self.user_ip.clone(),
            label: self.label.clone(),
            timestamp: Some(SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_micros() as i64).unwrap_or(0)),
            auth_code: None,
            timeout: timeout.map(i64::from),
            request_id: Some(self.load_id.clone()),
            token: self.token.clone(),
            auth_code_uuid: None,
            table_id: None,
            backend_id: self.inner.master.backend_id(),
            cert_based_auth: None,
            use_table_group_commit_mode: self.can_group_commit().then_some(true),
        };
        let begun = fe.load_txn_begin(begin);
        self.times[0] = t.elapsed().as_millis();
        let begun = match begun {
            Ok(r) if r.status.status_code == TStatusCode::OK => r,
            Ok(r) => {
                self.fail(fe_message(&r.status));
                return false;
            }
            Err(e) => {
                self.fail(format!("[INTERNAL_ERROR]{e}"));
                return false;
            }
        };
        self.txn_id = begun.txn_id.unwrap_or(-1);
        self.db_id = begun.db_id;
        // a table's own group commit mode (no group_commit header) is not done yet
        if begun
            .table_group_commit_mode
            .as_deref()
            .is_some_and(|m| m.contains("async") || m.contains("sync"))
        {
            self.fail("[NOT_IMPLEMENTED_ERROR]doris-rs does not support a table's own group commit mode yet".into());
            self.rollback();
            return false;
        }
        true
    }

    /// `streamLoadPut`, then the body through the planned fragment: committed as the load's
    /// transaction, or queued for group commit.
    fn put_and_load(&mut self, fe: &dyn FeClient, format: TFileFormatType, compress: TFileCompressType, timeout: Option<i32>, max: u64) {
        let t = Instant::now();
        let request = match self.put_request(format, compress, timeout) {
            Ok(r) => r,
            Err(msg) => {
                self.fail(msg);
                return self.rollback();
            }
        };
        let put = fe.stream_load_put(request);
        self.times[1] = t.elapsed().as_millis();
        let put = match put {
            Ok(r) if r.status.status_code == TStatusCode::OK => r,
            Ok(r) => {
                self.fail(fe_message(&r.status));
                return self.rollback();
            }
            Err(e) => {
                self.fail(format!("[INTERNAL_ERROR]{e}"));
                return self.rollback();
            }
        };
        let planned = put
            .pipeline_params
            .ok_or_else(|| ExecError::BadRequest("no plan".into()))
            .and_then(|p| plan_write(&p));
        let mut f: WriteFragment = match planned {
            Ok(f) => f,
            Err(e) => {
                self.fail(format!("[NOT_IMPLEMENTED_ERROR]{e}"));
                return self.rollback();
            }
        };
        // StreamLoadAction: enable_insert_strict follows the load's strict mode
        f.insert_strict = self.header("strict_mode").is_some_and(|v| v.eq_ignore_ascii_case("true"));
        // the stream load plan names no backend: commit infos carry this BE's id
        if f.backend_id == 0 {
            f.backend_id = self.inner.master.backend_id().unwrap_or(0);
        }
        let t = Instant::now();
        let body = self.req.body.as_deref().unwrap_or_default();
        let rows = match f.in_context(|| stream_rows(&f, body, max)) {
            Ok(r) => r,
            Err(e @ ExecError::ExceededLimit(_)) => {
                self.fail(format!("[EXCEEDED_LIMIT]{e}"));
                return self.rollback();
            }
            Err(e) => {
                self.fail(format!("[NOT_IMPLEMENTED_ERROR]{e}"));
                return self.rollback();
            }
        };
        if self.group_commit.is_some() {
            let (outcome, queue) = f.in_context(|| self.inner.group_commit_rows(&f, rows, &|| None));
            self.times[3] = t.elapsed().as_millis();
            if let Some(q) = queue {
                self.txn_id = q.txn_id();
                self.label = q.label().to_string();
            }
            self.total = outcome.loaded + outcome.filtered + outcome.unselected;
            self.loaded = outcome.loaded;
            self.filtered = outcome.filtered;
            self.unselected = outcome.unselected;
            self.error_url = self.inner.tracking_url(&outcome);
            if outcome.status.status_code != TStatusCode::OK {
                self.first_error = outcome.first_error.clone();
                let msg = outcome.status.error_msgs.clone().unwrap_or_default().join("; ");
                self.fail(format!("[{}]{msg}", crate::status::code_name(outcome.status.status_code)));
            }
            return;
        }
        let outcome = f.in_context(|| run_sink(&f, rows, &self.inner.tablets, &self.inner.error_dir));
        self.times[3] = t.elapsed().as_millis();
        self.total = outcome.loaded + outcome.filtered + outcome.unselected;
        self.loaded = outcome.loaded;
        self.filtered = outcome.filtered;
        self.unselected = outcome.unselected;
        self.error_url = self.inner.tracking_url(&outcome);
        if outcome.status.status_code != TStatusCode::OK {
            self.first_error = outcome.first_error.clone();
            self.fail(format!(
                "[INTERNAL_ERROR]{}",
                outcome.status.error_msgs.clone().unwrap_or_default().join("; ")
            ));
            return self.rollback();
        }
        // StreamLoadExecutor: the filter ratio over the rows that were not unselected
        let ratio = self.header("max_filter_ratio").and_then(|r| r.parse::<f64>().ok()).unwrap_or(0.0);
        let considered = self.total - self.unselected;
        if considered > 0 && self.filtered as f64 / considered as f64 > ratio {
            self.first_error = outcome.first_error.clone();
            self.fail("[DATA_QUALITY_ERROR]too many filtered rows".into());
            return self.rollback();
        }
        let t = Instant::now();
        let commit = TLoadTxnCommitRequest {
            cluster: None,
            user: self.user.clone(),
            passwd: self.passwd.clone(),
            db: self.req.db.clone(),
            tbl: self.req.table.clone(),
            user_ip: self.user_ip.clone(),
            txn_id: self.txn_id,
            sync: true,
            commit_infos: Some(outcome.commit_infos.clone()),
            auth_code: None,
            txn_commit_attachment: None,
            thrift_rpc_timeout_ms: Some(i64::from(COMMIT_RPC_TIMEOUT_MS)),
            token: self.token.clone(),
            db_id: self.db_id,
            tbls: Some(vec![]),
            table_id: None,
            auth_code_uuid: None,
            group_commit: None,
            receive_bytes: None,
            backend_id: None,
            cert_based_auth: None,
        };
        let committed = fe.load_txn_commit(commit);
        self.times[5] = t.elapsed().as_millis();
        match committed {
            Ok(r) if r.status.status_code == TStatusCode::OK => {}
            Ok(r) if r.status.status_code == TStatusCode::PUBLISH_TIMEOUT => self.publish_timeout = true,
            Ok(r) => {
                self.fail(fe_message(&r.status));
                self.rollback();
            }
            Err(e) => {
                self.fail(format!("[INTERNAL_ERROR]{e}"));
                self.rollback();
            }
        }
    }

    /// `StreamLoadAction::_process_put`: the headers as the FE's put request.
    /// The format header when it names a CSV with a header (`csv_with_names`,
    /// `csv_with_names_and_types`, any case), as given; else empty.
    fn header_type(&self) -> String {
        self.header("format")
            .filter(|f| f.eq_ignore_ascii_case("csv_with_names") || f.eq_ignore_ascii_case("csv_with_names_and_types"))
            .unwrap_or_default()
    }

    fn put_request(&self, format: TFileFormatType, compress: TFileCompressType, timeout: Option<i32>) -> Result<TStreamLoadPutRequest, String> {
        let h = |k: &str| self.header(k);
        let skip_lines = match h("skip_lines").filter(|v| !v.is_empty()) {
            None => None,
            Some(v) => match stoi(&v, "skip_lines")? {
                n if n < 0 => return Err(format!("[INVALID_ARGUMENT]Invalid 'skip_lines': {n}")),
                n => Some(n),
            },
        };
        let is_true = |k: &str| h(k).is_some_and(|v| v.eq_ignore_ascii_case("true"));
        // a Parquet or ORC body is saved to a file first (`StreamLoadAction::_process_put`): the
        // FE plans a local file scan of it. doris-rs reads the body from memory, so only the
        // path is formed, as `_data_saved_path` forms it.
        let saved = matches!(format, TFileFormatType::FORMAT_PARQUET | TFileFormatType::FORMAT_ORC);
        let path = saved.then(|| {
            let root = {
                let mgr = self.inner.tablets.lock().unwrap_or_else(|e| e.into_inner());
                mgr.data_dirs().first().map(|d| d.root().display().to_string()).unwrap_or_default()
            };
            let shard = self.inner.next_shard.fetch_add(1, Ordering::Relaxed) % MAX_SHARD_NUM;
            let usec = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.subsec_micros());
            format!(
                "{root}/mini_download/{}/__shard_{shard}//{}.{}.{usec}",
                self.req.db,
                self.req.table,
                doris_storage::timestamp()
            )
        });
        let (mut read_json_by_line, mut strip_outer_array) = (is_true("read_json_by_line"), is_true("strip_outer_array"));
        if h("read_json_by_line").is_none() && h("strip_outer_array").is_none() {
            read_json_by_line = true;
            strip_outer_array = false;
        }
        Ok(TStreamLoadPutRequest {
            cluster: None,
            user: self.user.clone(),
            passwd: self.passwd.clone(),
            db: self.req.db.clone(),
            tbl: self.req.table.clone(),
            user_ip: self.user_ip.clone(),
            load_id: self.load_id.clone(),
            txn_id: self.txn_id,
            file_type: if saved { TFileType::FILE_LOCAL } else { TFileType::FILE_STREAM },
            format_type: format,
            path,
            columns: h("columns"),
            where_: h("where"),
            column_separator: h("column_separator"),
            partitions: h("partitions"),
            auth_code: None,
            negative: Some(is_true("negative")),
            timeout,
            strict_mode: h("strict_mode").map(|v| v.eq_ignore_ascii_case("true")),
            timezone: h("timezone").or_else(|| h("time_zone")),
            exec_mem_limit: h("exec_mem_limit").and_then(|v| v.parse().ok()),
            is_temp_partition: h("partitions").map(|_| false),
            strip_outer_array: Some(strip_outer_array),
            jsonpaths: h("jsonpaths"),
            thrift_rpc_timeout_ms: Some(i64::from(PUT_RPC_TIMEOUT_MS)),
            json_root: h("json_root"),
            merge_type: Some(TMergeType::APPEND),
            delete_condition: None,
            sequence_col: h("function_column.sequence_col"),
            num_as_string: Some(is_true("num_as_string")),
            fuzzy_parse: Some(is_true("fuzzy_parse")),
            line_delimiter: h("line_delimiter"),
            read_json_by_line: Some(read_json_by_line),
            token: self.token.clone(),
            send_batch_parallelism: h("send_batch_parallelism").and_then(|v| v.parse().ok()),
            max_filter_ratio: h("max_filter_ratio").map(|v| OrderedFloat(v.parse().unwrap_or(0.0))),
            load_to_single_tablet: h("load_to_single_tablet").map(|v| v.eq_ignore_ascii_case("true")),
            header_type: Some(self.header_type()),
            hidden_columns: h("hidden_columns"),
            compress_type: Some(compress),
            file_size: saved.then(|| self.req.body.as_ref().map_or(0, Vec::len) as i64),
            trim_double_quotes: h("trim_double_quotes").map(|v| v.eq_ignore_ascii_case("true")),
            skip_lines,
            enable_profile: h("enable_profile").map(|v| v.eq_ignore_ascii_case("true")),
            partial_update: None,
            table_names: None,
            load_sql: None,
            backend_id: self.inner.master.backend_id(),
            version: None,
            label: None,
            enclose: h("enclose").and_then(|v| v.bytes().next()).map(|b| b as i8),
            escape: h("escape").and_then(|v| v.bytes().next()).map(|b| b as i8),
            memtable_on_sink_node: h("memtable_on_sink_node").map(|v| v.eq_ignore_ascii_case("true")),
            group_commit: None,
            stream_per_node: None,
            group_commit_mode: self.group_commit.clone(),
            unique_key_update_mode: None,
            partial_update_new_key_policy: None,
            empty_field_as_null: h("empty_field_as_null").map(|v| v.eq_ignore_ascii_case("true")),
            cert_based_auth: None,
            cloud_cluster: None,
            table_id: None,
        })
    }

    /// `StreamLoadExecutor::rollback_txn`; nothing for a group commit load, which began none.
    fn rollback(&mut self) {
        if self.group_commit.is_some() {
            return;
        }
        let req = TLoadTxnRollbackRequest {
            cluster: None,
            user: self.user.clone(),
            passwd: self.passwd.clone(),
            db: self.req.db.clone(),
            tbl: self.req.table.clone(),
            user_ip: self.user_ip.clone(),
            txn_id: self.txn_id,
            reason: self.status.clone(),
            auth_code: None,
            txn_commit_attachment: None,
            token: self.token.clone(),
            db_id: self.db_id,
            tbls: Some(vec![]),
            auth_code_uuid: None,
            label: Some(self.label.clone()),
        };
        let _ = self.inner.fe.load_txn_rollback(req);
    }

    fn status_word(&self) -> &'static str {
        if self.status.is_some() {
            "Fail"
        } else if self.publish_timeout {
            "Publish Timeout"
        } else {
            "Success"
        }
    }

    /// `StreamLoadContext::to_json`: rapidjson's pretty writer (4 spaces), a newline after.
    fn reply_json(&self) -> String {
        let s = |v: &str| serde_json::Value::String(v.to_string()).to_string();
        let ok = self.status.is_none();
        let mut fields: Vec<(&str, String)> = vec![
            ("TxnId", self.txn_id.to_string()),
            ("Label", s(&self.label)),
            ("Comment", s(&self.header("comment").unwrap_or_default())),
        ];
        match &self.group_commit {
            None => fields.push(("TwoPhaseCommit", s("false"))),
            Some(mode) => {
                fields.push(("GroupCommit", "true".into()));
                fields.push(("GroupCommitMode", s(mode)));
                fields.push(("LoadId", s(&format!("{:016x}-{:016x}", self.load_id.hi as u64, self.load_id.lo as u64))));
            }
        }
        fields.extend([
            ("Status", s(self.status_word())),
            ("Message", s(self.status.as_deref().unwrap_or("OK"))),
            ("NumberTotalRows", self.total.to_string()),
            ("NumberLoadedRows", if ok { self.loaded } else { 0 }.to_string()),
            ("NumberFilteredRows", self.filtered.to_string()),
            ("NumberUnselectedRows", self.unselected.to_string()),
            ("LoadBytes", self.req.body.as_ref().map_or(0, Vec::len).to_string()),
            ("LoadTimeMs", self.started.elapsed().as_millis().to_string()),
        ]);
        let gc = self.group_commit.is_some();
        if !gc {
            fields.push(("BeginTxnTimeMs", self.times[0].to_string()));
        }
        fields.extend([
            ("StreamLoadPutTimeMs", self.times[1].to_string()),
            ("ReadDataTimeMs", self.times[2].to_string()),
            ("WriteDataTimeMs", self.times[3].to_string()),
            ("ReceiveDataTimeMs", self.times[4].to_string()),
        ]);
        if !gc {
            fields.push(("CommitAndPublishTimeMs", self.times[5].to_string()));
        }
        if let Some(u) = &self.error_url {
            fields.push(("ErrorURL", s(u)));
        }
        if let Some(m) = self.first_error.as_ref().filter(|_| !ok) {
            fields.push(("FirstErrorMsg", s(m)));
        }
        let body: Vec<String> = fields.iter().map(|(k, v)| format!("    \"{k}\": {v}")).collect();
        format!("{{\n{}\n}}\n", body.join(",\n"))
    }

    /// `StreamLoadAction::_save_stream_load_record`, unless the client asked to skip it.
    fn record(&self, inner: &Inner) {
        if self.req.headers.contains_key("skip_record_to_audit_log_table") {
            return;
        }
        let finish = now_ms();
        let mut rec: HashMap<&str, serde_json::Value> = HashMap::new();
        rec.insert("Label", self.label.clone().into());
        rec.insert("Db", self.req.db.clone().into());
        rec.insert("Table", self.req.table.clone().into());
        rec.insert("User", self.user.clone().into());
        rec.insert("ClientIp", self.user_ip.clone().unwrap_or_default().into());
        rec.insert("Status", self.status_word().into());
        rec.insert("Message", self.status.clone().unwrap_or_else(|| "OK".into()).into());
        rec.insert("ErrorURL", self.error_url.clone().unwrap_or_default().into());
        rec.insert("FirstErrorMsg", self.first_error.clone().unwrap_or_default().into());
        rec.insert("NumberTotalRows", self.total.into());
        rec.insert("NumberLoadedRows", (if self.status.is_none() { self.loaded } else { 0 }).into());
        rec.insert("NumberFilteredRows", self.filtered.into());
        rec.insert("NumberUnselectedRows", self.unselected.into());
        rec.insert("LoadBytes", self.req.body.as_ref().map_or(0, Vec::len).into());
        rec.insert("StartTime", self.start_ms.into());
        rec.insert("FinishTime", finish.into());
        rec.insert("Comment", self.header("comment").unwrap_or_default().into());
        let value = serde_json::to_string(&rec).unwrap_or_default();
        let key = format!("{finish}_{}", self.label);
        let mut mgr = inner.tablets.lock().unwrap_or_else(|e| e.into_inner());
        if let Err(e) = mgr.put_stream_load_record(&key, &value) {
            eprintln!("doris-rs be: cannot record stream load {}: {e}", self.label);
        }
    }
}

/// An FE status as the C++ BE quotes it (`Status::create(TStatus)`): `[<code>]TStatus: <first
/// message>`.
fn fe_message(st: &doris_thrift::gen::status::TStatus) -> String {
    let first = st.error_msgs.as_ref().and_then(|m| m.first()).cloned().unwrap_or_default();
    format!("[{}]TStatus: {first}", crate::status::code_name(st.status_code))
}

/// `LoadUtil::parse_format` for the formats doris-rs reads: the format the FE plans and the
/// compression, or None for another format.
fn parse_format(format: &str, compress: &str) -> Option<(TFileFormatType, TFileCompressType)> {
    let c = compress.to_ascii_uppercase();
    let compress_type = match c.as_str() {
        "GZ" => TFileCompressType::GZ,
        "LZO" | "LZOP" => TFileCompressType::LZO,
        "BZ2" => TFileCompressType::BZ2,
        "ZSTD" => TFileCompressType::ZSTD,
        "LZ4" | "LZ4FRAME" => TFileCompressType::LZ4FRAME,
        "LZ4_BLOCK" => TFileCompressType::LZ4BLOCK,
        "SNAPPY_BLOCK" => TFileCompressType::SNAPPYBLOCK,
        "DEFLATE" => TFileCompressType::DEFLATE,
        _ => TFileCompressType::PLAIN,
    };
    let format_type = match format {
        "csv" => match c.as_str() {
            "" | "ZSTD" => TFileFormatType::FORMAT_CSV_PLAIN,
            "GZ" => TFileFormatType::FORMAT_CSV_GZ,
            "LZO" => TFileFormatType::FORMAT_CSV_LZO,
            "BZ2" => TFileFormatType::FORMAT_CSV_BZ2,
            "LZ4" | "LZ4FRAME" => TFileFormatType::FORMAT_CSV_LZ4FRAME,
            "LZ4_BLOCK" => TFileFormatType::FORMAT_CSV_LZ4BLOCK,
            "LZOP" => TFileFormatType::FORMAT_CSV_LZOP,
            "SNAPPY_BLOCK" => TFileFormatType::FORMAT_CSV_SNAPPYBLOCK,
            "DEFLATE" => TFileFormatType::FORMAT_CSV_DEFLATE,
            _ => return None,
        },
        "json" => TFileFormatType::FORMAT_JSON,
        // read whole, not streamed: compress_type does not apply
        "parquet" => return Some((TFileFormatType::FORMAT_PARQUET, TFileCompressType::PLAIN)),
        "orc" => return Some((TFileFormatType::FORMAT_ORC, TFileCompressType::PLAIN)),
        _ => return None,
    };
    Some((format_type, compress_type))
}

/// `safe_stoi`: `std::stoi` (leading whitespace, a sign, the digits, anything after ignored),
/// its exceptions as the C++ BE words them.
fn stoi(input: &str, name: &str) -> Result<i32, String> {
    let t = input.trim_start_matches([' ', '\t', '\n', '\x0b', '\x0c', '\r']);
    let (neg, rest) = match t.as_bytes().first() {
        Some(b'-') => (true, &t[1..]),
        Some(b'+') => (false, &t[1..]),
        _ => (false, t),
    };
    let digits: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
    if digits.is_empty() {
        return Err(format!("[INVALID_ARGUMENT]Invalid format of '{name}': '{input}', stoi"));
    }
    let n: i128 = digits.parse().unwrap_or(i128::MAX);
    i32::try_from(if neg { -n } else { n }).map_err(|_| format!("[INVALID_ARGUMENT]'{name}' value out of range: '{input}', stoi"))
}

/// A stored record as `TStreamLoadRecord` (`StreamLoadRecorder`, `"N/A"` for empty fields).
fn record_from_json(v: &serde_json::Value) -> TStreamLoadRecord {
    let s = |k: &str| v.get(k).and_then(|x| x.as_str()).unwrap_or_default().to_string();
    let n = |k: &str| v.get(k).and_then(|x| x.as_i64()).unwrap_or(0);
    let na = |k: &str| Some(Some(s(k)).filter(|x| !x.is_empty()).unwrap_or_else(|| "N/A".into()));
    TStreamLoadRecord {
        cluster: None,
        user: s("User"),
        passwd: String::new(),
        db: s("Db"),
        tbl: s("Table"),
        user_ip: Some(s("ClientIp")),
        label: s("Label"),
        status: s("Status"),
        message: s("Message"),
        url: na("ErrorURL"),
        auth_code: None,
        total_rows: n("NumberTotalRows"),
        loaded_rows: n("NumberLoadedRows"),
        filtered_rows: n("NumberFilteredRows"),
        unselected_rows: n("NumberUnselectedRows"),
        load_bytes: n("LoadBytes"),
        start_time: n("StartTime"),
        finish_time: n("FinishTime"),
        comment: Some(s("Comment")),
        first_error_msg: na("FirstErrorMsg"),
    }
}
