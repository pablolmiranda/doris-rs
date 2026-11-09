//! Queries ([wire] §1–§4): `exec_plan_fragment` (one fragment) or `exec_plan_fragment_prepare`
//! then `_start` (several), every fragment instance run on its own thread, and the result
//! sink's rows long-polled by the FE with `fetch_data`. The result buffer exists once prepare
//! returns, since the FE's first fetch can arrive before its start. A successful query reports
//! nothing; a failed one fails the parked fetch at once, with the error, and sends one
//! `reportExecStatus` per fragment. A query that writes a table (`INSERT … SELECT`) feeds each
//! sink fragment's `TableSink` from its instances; once every instance has ended the sink
//! commits (or, on failure, does not), and every fragment reports, as a LOAD.
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use doris_exec::block::Block;
use doris_exec::exchange::Exchanges;
use doris_exec::fragment::decode_fragments;
use doris_exec::pipeline::{run_instance, ExecCtx};
use doris_exec::query::{check_plan, plan_query, QueryPlan};
use doris_exec::sink::{exec_status, TableSink};
use doris_exec::value::datum_value;
use doris_exec::row_text::result_batch;
use doris_exec::ExecError;
use doris_proto::doris::{
    PCacheResponse, PCacheStatus, PCancelPlanFragmentRequest, PCancelPlanFragmentResult, PExecPlanFragmentRequest, PExecPlanFragmentResult,
    PExecPlanFragmentStartRequest, PFetchCacheResult, PFetchDataRequest, PFetchDataResult, PPlanFragmentCancelReason, PQueryStatistics, PStatus,
};
use doris_storage::reader::scan_tablet;
use doris_storage::TabletManager;
use doris_thrift::gen::frontend_service::{
    FrontendServiceVersion, TDescribeTablesParams, TDescribeTablesResult, TGetDbsParams, TGetDbsResult, TGetTablesParams, TGetTablesResult,
    TListTableStatusResult, TReportExecStatusParams, TShowVariableRequest, TShowVariableResult,
};
use doris_thrift::gen::palo_internal_service::TQueryType;
use doris_thrift::gen::status::{TStatus, TStatusCode};
use doris_thrift::gen::types::{TNetworkAddress, TUniqueId};

use crate::fe_client::FeClient;
use crate::master::MasterInfo;

type Id = (i64, i64);

const OK: i32 = 0;
const CANCELLED: i32 = 1;
const NOT_IMPLEMENTED_ERROR: i32 = 3;
const INTERNAL_ERROR: i32 = 6;
const TIMEOUT: i32 = 31;

fn now_ms() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0)
}

fn pstatus(code: i32, msg: Option<String>) -> PStatus {
    PStatus {
        status_code: code,
        error_msgs: msg.into_iter().collect(),
    }
}

/// An error as the C++ BE writes it: its code, and `(<be ip>)[<code name>]<message>`.
fn error_status(localhost: &str, e: &ExecError) -> (i32, String) {
    let (code, name) = match e {
        ExecError::Unsupported { .. } => (NOT_IMPLEMENTED_ERROR, "NOT_IMPLEMENTED_ERROR"),
        ExecError::Cancelled(_) => (CANCELLED, "CANCELLED"),
        _ => (INTERNAL_ERROR, "INTERNAL_ERROR"),
    };
    (code, format!("({localhost})[{name}]{e}"))
}

#[derive(Default)]
struct Buffer {
    packets: VecDeque<Vec<u8>>,
    /// Data packets handed to the FE so far: the next packet's sequence number.
    sent: i64,
    rows: i64,
    closed: bool,
    failed: Option<PStatus>,
}

struct Query {
    plan: QueryPlan,
    /// Every instance has ended: a later cancel has nothing to stop.
    finished: std::sync::atomic::AtomicBool,
    started: Mutex<bool>,
    buffer: Mutex<Buffer>,
    changed: Condvar,
}

impl Query {
    fn buffer(&self) -> std::sync::MutexGuard<'_, Buffer> {
        self.buffer.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn fail(&self, st: PStatus) {
        let mut b = self.buffer();
        if b.failed.is_none() && !b.closed {
            b.failed = Some(st);
        }
        self.changed.notify_all();
    }
}

#[derive(Clone)]
pub struct QueryService {
    inner: Arc<Inner>,
}

struct Inner {
    tablets: Arc<Mutex<TabletManager>>,
    fe: Arc<dyn FeClient>,
    master: MasterInfo,
    exchanges: Exchanges,
    /// By query id, and by result instance id when results are fetched by instance.
    queries: Mutex<HashMap<Id, Arc<Query>>>,
    /// Where a write's error logs go (the load service's), and the port that serves them.
    error_dir: std::path::PathBuf,
    http_port: std::sync::atomic::AtomicU16,
    /// Queries running.
    running: Mutex<usize>,
    idle: Condvar,
}

impl QueryService {
    pub fn new(tablets: Arc<Mutex<TabletManager>>, fe: Arc<dyn FeClient>, master: MasterInfo) -> QueryService {
        let error_dir = crate::load::error_dir(&tablets);
        QueryService {
            inner: Arc::new(Inner {
                tablets,
                fe,
                master,
                exchanges: Exchanges::default(),
                queries: Mutex::new(HashMap::new()),
                error_dir,
                http_port: std::sync::atomic::AtomicU16::new(8040),
                running: Mutex::new(0),
                idle: Condvar::new(),
            }),
        }
    }

    /// The BE's webserver port, which serves a write's error log (`webserver_port`).
    pub fn set_http_port(&self, port: u16) {
        self.inner.http_port.store(port, std::sync::atomic::Ordering::Relaxed);
    }

    /// Waits until no query is running; false on timeout.
    pub fn wait_idle(&self, timeout: Duration) -> bool {
        let n = self.inner.running.lock().unwrap_or_else(|e| e.into_inner());
        let (_g, res) = self.inner.idle.wait_timeout_while(n, timeout, |n| *n > 0).unwrap_or_else(|e| e.into_inner());
        !res.timed_out()
    }

    fn queries(&self) -> std::sync::MutexGuard<'_, HashMap<Id, Arc<Query>>> {
        self.inner.queries.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// `exec_plan_fragment` of a query: one fragment, run at once.
    pub fn exec_plan_fragment(&self, req: &PExecPlanFragmentRequest) -> PExecPlanFragmentResult {
        self.submit(req)
    }

    /// `exec_plan_fragment_prepare`: every fragment built; run now, or at `start` when the FE
    /// asks to wait for it.
    pub fn prepare(&self, req: &PExecPlanFragmentRequest) -> PExecPlanFragmentResult {
        self.submit(req)
    }

    fn submit(&self, req: &PExecPlanFragmentRequest) -> PExecPlanFragmentResult {
        let received = now_ms();
        let reply = |st: PStatus| PExecPlanFragmentResult {
            status: st,
            received_time: Some(received),
            execution_time: Some(now_ms()),
            execution_done_time: Some(now_ms()),
        };
        let planned = decode_fragments(req).and_then(|l| plan_query(&l)).and_then(|p| check_plan(&p).map(|_| p));
        let plan = match planned {
            Ok(p) => p,
            Err(e) => {
                let (code, msg) = error_status(&self.inner.master.localhost(), &e);
                return reply(pstatus(code, Some(msg)));
            }
        };
        let id = (plan.query_id.hi, plan.query_id.lo);
        let need_wait = plan.need_wait;
        let mut keys = vec![id];
        if !plan.parallel_result_sink {
            keys.extend(
                plan.result_fragment()
                    .into_iter()
                    .flat_map(|f| f.instances.iter().map(|i| (i.id.hi, i.id.lo))),
            );
        }
        let q = Arc::new(Query {
            plan,
            finished: std::sync::atomic::AtomicBool::new(false),
            started: Mutex::new(false),
            buffer: Mutex::new(Buffer::default()),
            changed: Condvar::new(),
        });
        {
            let mut queries = self.queries();
            // one (query, fragment) per BE, as FragmentMgr::exec_plan_fragment; a query sent in
            // more than one request is not one doris-rs runs
            if let Some(held) = queries.get(&id) {
                let (code, msg) = match q.plan.fragments.iter().find(|f| held.plan.fragments.iter().any(|h| h.fragment_id == f.fragment_id)) {
                    Some(f) => (
                        INTERNAL_ERROR,
                        format!(
                            "({})[INTERNAL_ERROR]exec_plan_fragment query_id({:x}-{:x}) input duplicated fragment_id({})",
                            self.inner.master.localhost(),
                            id.0,
                            id.1,
                            f.fragment_id
                        ),
                    ),
                    None => (
                        NOT_IMPLEMENTED_ERROR,
                        format!("({})[NOT_IMPLEMENTED_ERROR]a query sent in more than one request", self.inner.master.localhost()),
                    ),
                };
                return reply(pstatus(code, Some(msg)));
            }
            for k in keys {
                queries.insert(k, q.clone());
            }
        }
        if !need_wait {
            self.run(q);
        }
        reply(pstatus(OK, None))
    }

    /// `exec_plan_fragment_start`: runs a prepared query.
    pub fn start(&self, req: &PExecPlanFragmentStartRequest) -> PExecPlanFragmentResult {
        let id = req.query_id.as_ref().map(|q| (q.hi, q.lo)).unwrap_or_default();
        let q = self.queries().get(&id).cloned();
        let st = match q {
            Some(q) => {
                self.run(q);
                pstatus(OK, None)
            }
            None => pstatus(
                INTERNAL_ERROR,
                Some(format!(
                    "({})[INTERNAL_ERROR]Failed to get query fragments context. Query {:x}-{:x} may be timeout or be cancelled.",
                    self.inner.master.localhost(),
                    id.0,
                    id.1
                )),
            ),
        };
        PExecPlanFragmentResult {
            status: st,
            received_time: Some(now_ms()),
            execution_time: Some(now_ms()),
            execution_done_time: Some(now_ms()),
        }
    }

    /// Starts every instance of the query on its own thread, once.
    fn run(&self, q: Arc<Query>) {
        {
            let mut started = q.started.lock().unwrap_or_else(|e| e.into_inner());
            if *started {
                return;
            }
            *started = true;
        }
        let (inner, ran) = (self.inner.clone(), q.clone());
        *self.inner.running.lock().unwrap_or_else(|e| e.into_inner()) += 1;
        let spawned = std::thread::Builder::new().name("query".into()).spawn(move || {
            inner.execute(&ran);
            let mut n = inner.running.lock().unwrap_or_else(|e| e.into_inner());
            *n -= 1;
            inner.idle.notify_all();
        });
        if let Err(e) = spawned {
            let mut n = self.inner.running.lock().unwrap_or_else(|e| e.into_inner());
            *n -= 1;
            self.inner.idle.notify_all();
            drop(n);
            // nothing will run: the FE's fetch gets the error instead of waiting out its timeout
            q.finished.store(true, std::sync::atomic::Ordering::SeqCst);
            q.fail(pstatus(
                INTERNAL_ERROR,
                Some(format!("({})[INTERNAL_ERROR]cannot start the query: {e}", self.inner.master.localhost())),
            ));
        }
    }

    /// `fetch_data`: the next packet, waiting for one while the query runs.
    pub fn fetch_data(&self, req: &PFetchDataRequest) -> PFetchDataResult {
        let id = (req.finst_id.hi, req.finst_id.lo);
        let Some(q) = self.queries().get(&id).cloned() else {
            return PFetchDataResult {
                status: pstatus(
                    INTERNAL_ERROR,
                    Some(format!(
                        "({})[INTERNAL_ERROR]no result for query {:x}-{:x}",
                        self.inner.master.localhost(),
                        id.0,
                        id.1
                    )),
                ),
                ..Default::default()
            };
        };
        let deadline = Instant::now() + Duration::from_secs(q.plan.timeout_s.max(1) as u64);
        let mut b = q.buffer();
        loop {
            if let Some(st) = b.failed.clone() {
                drop(b);
                self.forget(&q);
                return PFetchDataResult { status: st, ..Default::default() };
            }
            if let Some(packet) = b.packets.pop_front() {
                let seq = b.sent;
                b.sent += 1;
                return PFetchDataResult {
                    status: pstatus(OK, None),
                    packet_seq: Some(seq),
                    eos: Some(false),
                    row_batch: Some(packet),
                    ..Default::default()
                };
            }
            if b.closed {
                let (sent, rows) = (b.sent, b.rows);
                drop(b);
                // the result is served: the query is done (the FE's FINISHED cancel finds nothing)
                self.forget(&q);
                return PFetchDataResult {
                    status: pstatus(OK, None),
                    packet_seq: Some(sent),
                    eos: Some(true),
                    query_statistics: Some(PQueryStatistics {
                        returned_rows: Some(rows),
                        ..Default::default()
                    }),
                    ..Default::default()
                };
            }
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return PFetchDataResult {
                    status: pstatus(TIMEOUT, Some("query timeout".into())),
                    ..Default::default()
                };
            }
            b = q.changed.wait_timeout(b, left).unwrap_or_else(|e| e.into_inner()).0;
        }
    }

    /// `cancel_plan_fragment`: FINISHED (sent after every query) forgets the query; any other
    /// reason also stops it and fails a parked fetch.
    pub fn cancel(&self, req: &PCancelPlanFragmentRequest) -> PCancelPlanFragmentResult {
        let id = req.query_id.as_ref().map(|q| (q.hi, q.lo)).unwrap_or((req.finst_id.hi, req.finst_id.lo));
        let removed: Vec<Arc<Query>> = {
            let mut queries = self.queries();
            let keys: Vec<Id> = queries
                .iter()
                .filter(|(_, q)| (q.plan.query_id.hi, q.plan.query_id.lo) == id)
                .map(|(k, _)| *k)
                .collect();
            keys.iter().filter_map(|k| queries.remove(k)).collect()
        };
        let finished = req.cancel_status.as_ref().is_some_and(|s| s.status_code == TStatusCode::FINISHED.0)
            || (req.cancel_status.is_none() && req.cancel_reason.is_none());
        if let Some(q) = removed.first() {
            if !finished && !q.finished.load(std::sync::atomic::Ordering::SeqCst) {
                let why = req.cancel_status.as_ref().and_then(|s| s.error_msgs.first().cloned()).unwrap_or_else(|| {
                    format!(
                        "cancelled: {:?}",
                        req.cancel_reason.and_then(|r| PPlanFragmentCancelReason::try_from(r).ok())
                    )
                });
                self.inner.exchanges.cancel(id, why.clone());
                // the query may have ended (and forgotten its exchanges) since the check
                if q.finished.load(std::sync::atomic::Ordering::SeqCst) {
                    self.inner.exchanges.forget(id);
                }
                q.fail(pstatus(CANCELLED, Some(why)));
            }
        }
        PCancelPlanFragmentResult { status: pstatus(OK, None) }
    }

    /// Drops every key of a query whose outcome the FE has had.
    fn forget(&self, q: &Arc<Query>) {
        self.queries().retain(|_, x| !Arc::ptr_eq(x, q));
    }

    /// Queries held and exchange entries held: what is left of past queries.
    pub fn live_state(&self) -> (usize, usize) {
        (self.queries().len(), self.inner.exchanges.entries())
    }

    /// `update_cache`: doris-rs keeps no SQL cache; the FE's store succeeds.
    pub fn update_cache(&self) -> PCacheResponse {
        PCacheResponse {
            status: PCacheStatus::CacheOk as i32,
        }
    }

    /// `fetch_cache`: always a miss, so the FE runs the query.
    pub fn fetch_cache(&self) -> PFetchCacheResult {
        PFetchCacheResult {
            status: PCacheStatus::NoSqlKey as i32,
            ..Default::default()
        }
    }
}

impl Inner {
    fn execute(self: &Arc<Self>, q: &Arc<Query>) {
        let plan = &q.plan;
        let id = (plan.query_id.hi, plan.query_id.lo);
        let batch = plan.batch_size.max(1);
        let scan = |tablet: i64, version: i64, cols: &[doris_storage::reader::ScanColumn]| {
            let mgr = self.tablets.lock().unwrap_or_else(|e| e.into_inner());
            scan_tablet(&mgr, tablet, version, cols)
        };
        let result = |block: Block| -> Result<(), ExecError> {
            let mut b = q.buffer();
            b.rows += block.len() as i64;
            // vmysql_result_writer: a dry run counts the rows and sends none
            if plan.dry_run {
                return Ok(());
            }
            let mut rows = block.rows.into_iter().peekable();
            while rows.peek().is_some() {
                let part = Block {
                    types: block.types.clone(),
                    rows: rows.by_ref().take(batch).collect(),
                };
                b.packets.push_back(result_batch(&part));
            }
            q.changed.notify_all();
            Ok(())
        };
        let fetch = |ids: &[Vec<u8>], cols: &[doris_storage::reader::ScanColumn]| {
            let mgr = self.tablets.lock().unwrap_or_else(|e| e.into_inner());
            doris_storage::reader::fetch_by_row_ids(&mgr, ids, cols)
        };
        let fe = SchemaCalls(self.fe.as_ref());
        // each sink fragment's table sink, fed by its instances
        let first_error: Mutex<Option<ExecError>> = Mutex::new(None);
        let mut sinks: Vec<Option<TableSink>> = Vec::with_capacity(plan.fragments.len());
        for f in &plan.fragments {
            sinks.push(match &f.write {
                Some(w) => match TableSink::new(w, f.instances.iter().map(|i| i.id.clone()).collect()) {
                    Ok(s) => Some(s),
                    Err(e) => {
                        first_error.lock().unwrap_or_else(|e| e.into_inner()).get_or_insert(e);
                        None
                    }
                },
                None => None,
            });
        }
        let table = |fragment: usize, instance: usize, block: Block| -> Result<(), ExecError> {
            let sink = sinks
                .get(fragment)
                .and_then(Option::as_ref)
                .ok_or_else(|| ExecError::BadRequest(format!("fragment {fragment} has no table sink")))?;
            let nullable: Vec<bool> = block.types.iter().map(|t| t.nullable).collect();
            let rows = block.rows.iter().map(|r| Ok(r.iter().zip(&block.types).map(|(d, c)| datum_value(d, c)).collect())).collect();
            sink.push(instance, rows, &nullable)
        };
        let ctx = ExecCtx { scan: &scan, exchanges: &self.exchanges, result: &result, fetch: &fetch, fe: &fe, table: &table };
        let ready = first_error.lock().unwrap_or_else(|e| e.into_inner()).is_none();
        std::thread::scope(|s| {
            if !ready {
                return;
            }
            for (fi, f) in plan.fragments.iter().enumerate() {
                for ii in 0..f.instances.len() {
                    let (ctx, first_error) = (&ctx, &first_error);
                    s.spawn(move || {
                        let ran = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| run_instance(plan, fi, ii, ctx)));
                        let err = match ran {
                            Ok(Ok(())) => return,
                            Ok(Err(e)) => e,
                            Err(p) => ExecError::Internal(format!(
                                "the query panicked: {}",
                                p.downcast_ref::<&str>()
                                    .map(|s| s.to_string())
                                    .or_else(|| p.downcast_ref::<String>().cloned())
                                    .unwrap_or_default()
                            )),
                        };
                        let mut first = first_error.lock().unwrap_or_else(|e| e.into_inner());
                        // the first failure is the query's error; the others are its cancellation
                        if first.is_none() {
                            self.exchanges.cancel(id, err.to_string());
                            *first = Some(err);
                        }
                    });
                }
            }
        });
        let failed = first_error.into_inner().unwrap_or_else(|e| e.into_inner());
        // ended before the FE can see the outcome, so its cancel finds nothing to stop
        q.finished.store(true, std::sync::atomic::Ordering::SeqCst);
        self.exchanges.forget(id);
        if plan.fragments.iter().any(|f| f.write.is_some()) {
            self.report_write(plan, sinks, failed.as_ref());
            // nothing fetches a write: the FE has its outcome now
            self.queries.lock().unwrap_or_else(|e| e.into_inner()).retain(|_, x| !Arc::ptr_eq(x, q));
            return;
        }
        match failed {
            None => {
                let mut b = q.buffer();
                b.closed = true;
                q.changed.notify_all();
            }
            Some(e) => {
                let (code, msg) = error_status(&self.master.localhost(), &e);
                q.fail(pstatus(code, Some(msg.clone())));
                self.report_failure(plan, &e, &msg);
            }
        }
    }

    /// A write's status as its fragments report it: OK, or the error as `error_status` words it.
    fn status_of(&self, e: Option<&ExecError>) -> TStatus {
        match e {
            None => TStatus { status_code: TStatusCode::OK, error_msgs: None },
            Some(e) => {
                let (_, msg) = error_status(&self.master.localhost(), e);
                TStatus { status_code: status_code(e), error_msgs: Some(vec![msg]) }
            }
        }
    }

    /// A write's reports, one per fragment (`PipelineFragmentContext::send_report`), all LOAD: a
    /// sink fragment's ends its sink first, committing what it took unless the query failed,
    /// and carries the sink's counters, instance reports, commit infos and errors.
    fn report_write(&self, plan: &QueryPlan, sinks: Vec<Option<TableSink>>, failed: Option<&ExecError>) {
        let st = self.status_of(failed);
        let failure = failed.map(|_| st.clone());
        let coord = plan.coord.clone().or_else(|| self.master.fe_address());
        for (f, sink) in plan.fragments.iter().zip(sinks) {
            let params = match (&f.write, sink) {
                (Some(w), Some(sink)) => {
                    let outcome = sink.finish(&self.tablets, &self.error_dir, failure.clone());
                    let port = self.http_port.load(std::sync::atomic::Ordering::Relaxed);
                    let url = outcome.error_log.as_ref().map(|file| format!("http://{}:{port}/api/_load_error_log?file={file}", self.master.localhost()));
                    let mut p = exec_status(w, &outcome, url);
                    p.fragment_id = Some(f.fragment_id);
                    p.backend_id = plan.backend_id.or_else(|| self.master.backend_id());
                    p
                }
                _ => TReportExecStatusParams {
                    protocol_version: FrontendServiceVersion::V1,
                    query_id: Some(plan.query_id.clone()),
                    backend_num: Some(-1),
                    fragment_instance_id: Some(TUniqueId { hi: 0, lo: 0 }),
                    status: Some(st.clone()),
                    done: Some(true),
                    backend_id: plan.backend_id.or_else(|| self.master.backend_id()),
                    fragment_id: Some(f.fragment_id),
                    query_type: Some(TQueryType::LOAD),
                    loaded_rows: Some(0),
                    loaded_bytes: Some(0),
                    ..blank_report()
                },
            };
            let Some(coord) = coord.clone() else { continue };
            if self.fe.report_exec_status(&coord, params.clone()).is_err() {
                let _ = self.fe.report_exec_status(&coord, params);
            }
        }
    }

    /// One `reportExecStatus` per fragment (`PipelineFragmentContext::send_report`).
    fn report_failure(&self, plan: &QueryPlan, e: &ExecError, msg: &str) {
        let Some(coord) = plan.coord.clone().or_else(|| self.master.fe_address()) else {
            return;
        };
        let st = doris_thrift::gen::status::TStatus {
            status_code: status_code(e),
            error_msgs: Some(vec![msg.to_string()]),
        };
        for f in &plan.fragments {
            let params = TReportExecStatusParams {
                protocol_version: FrontendServiceVersion::V1,
                query_id: Some(plan.query_id.clone()),
                backend_num: Some(-1),
                fragment_instance_id: Some(TUniqueId { hi: 0, lo: 0 }),
                status: Some(st.clone()),
                done: Some(true),
                backend_id: plan.backend_id.or_else(|| self.master.backend_id()),
                fragment_id: Some(f.fragment_id),
                query_type: Some(TQueryType::SELECT),
                ..blank_report()
            };
            if self.fe.report_exec_status(&coord, params.clone()).is_err() {
                let _ = self.fe.report_exec_status(&coord, params);
            }
        }
    }
}

fn status_code(e: &ExecError) -> TStatusCode {
    match e {
        ExecError::Unsupported { .. } => TStatusCode::NOT_IMPLEMENTED_ERROR,
        ExecError::Cancelled(_) => TStatusCode::CANCELLED,
        _ => TStatusCode::INTERNAL_ERROR,
    }
}

/// The information_schema scanners' FE calls, over the BE's FE client.
struct SchemaCalls<'a>(&'a dyn FeClient);

impl doris_exec::schema_scan::SchemaFe for SchemaCalls<'_> {
    fn get_db_names(&self, fe: &TNetworkAddress, p: TGetDbsParams) -> Result<TGetDbsResult, String> {
        self.0.get_db_names(fe, p).map_err(|e| e.to_string())
    }
    fn get_table_names(&self, fe: &TNetworkAddress, p: TGetTablesParams) -> Result<TGetTablesResult, String> {
        self.0.get_table_names(fe, p).map_err(|e| e.to_string())
    }
    fn describe_tables(&self, fe: &TNetworkAddress, p: TDescribeTablesParams) -> Result<TDescribeTablesResult, String> {
        self.0.describe_tables(fe, p).map_err(|e| e.to_string())
    }
    fn list_table_status(&self, fe: &TNetworkAddress, p: TGetTablesParams) -> Result<TListTableStatusResult, String> {
        self.0.list_table_status(fe, p).map_err(|e| e.to_string())
    }
    fn show_variables(&self, fe: &TNetworkAddress, p: TShowVariableRequest) -> Result<TShowVariableResult, String> {
        self.0.show_variables(fe, p).map_err(|e| e.to_string())
    }
}

fn blank_report() -> TReportExecStatusParams {
    TReportExecStatusParams::new(
        FrontendServiceVersion::V1,
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
        None,
        None,
        None,
        None,
    )
}
