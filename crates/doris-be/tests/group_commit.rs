//! Group commit: loads join a table's open queue (or start one with `streamLoadPut`); the queue
//! commits under its own transaction when its interval passes or its bytes reach the limit.
//! Checked against the recorded FE messages of `test_group_commit_interval_ms_property`.
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use doris_be::group_commit::GroupCommitMgr;
use doris_be::load::LoadService;
use doris_be::master::MasterInfo;
use doris_be::testing::{FakeFe, LoadCall, LoadScript};
use doris_exec::fragment::{plan_write, WriteFragment};
use doris_exec::sink::{Checked, TableSink};
use doris_fixtures::group_commit::{sequence, Msg, INTERVAL, TIMEOUT};
use doris_proto::doris::PExecPlanFragmentRequest;
use doris_storage::{DataDir, TabletManager, TabletOptions};
use doris_thrift::gen::frontend_service::TReportExecStatusParams;
use doris_thrift::gen::frontend_service::{
    TLoadTxnCommitRequest, TLoadTxnCommitResult, TLoadTxnRollbackResult, TStreamLoadPutRequest, TStreamLoadPutResult,
};
use doris_thrift::gen::status::{TStatus, TStatusCode};
use doris_thrift::gen::types::TTaskType;

pub const BACKEND: i64 = 1793677838735;

pub struct Setup {
    pub seq: Vec<Msg>,
    pub tmp: tempfile::TempDir,
    pub tablets: Arc<Mutex<TabletManager>>,
    pub fe: Arc<FakeFe>,
    pub master: MasterInfo,
}

fn ok() -> TStatus {
    TStatus {
        status_code: TStatusCode::OK,
        error_msgs: None,
    }
}

/// The interval suite's tablets, and an FE answering its put with the recorded reply (`edit`ed).
pub fn setup(edit: impl FnOnce(&mut TStreamLoadPutResult)) -> Setup {
    setup_for(INTERVAL, edit)
}

/// A suite's tablets, and an FE answering its first put with the recorded reply (`edit`ed).
pub fn setup_for(suite: &str, edit: impl FnOnce(&mut TStreamLoadPutResult)) -> Setup {
    let seq = sequence(suite);
    let tmp = tempfile::tempdir().unwrap();
    let mut mgr = TabletManager::open(vec![DataDir::open(tmp.path(), "127.0.0.1").unwrap()], TabletOptions::default()).unwrap();
    for m in &seq {
        if let Msg::Create(tasks) = m {
            for t in tasks.iter().filter(|t| t.task_type == TTaskType::CREATE) {
                mgr.create_tablet(t.create_tablet_req.as_ref().unwrap()).unwrap();
            }
        }
    }
    let mut put = seq
        .iter()
        .find_map(|m| if let Msg::PutReply(r) = m { Some(r.clone()) } else { None })
        .unwrap();
    edit(&mut put);
    let fe = Arc::new(FakeFe::default());
    fe.script(LoadScript {
        put: Some(put),
        commit: Some(TLoadTxnCommitResult { status: ok() }),
        rollback: Some(TLoadTxnRollbackResult { status: ok() }),
        ..Default::default()
    });
    let master = MasterInfo::new("127.0.0.1");
    master.set_backend_id_for_tests(BACKEND);
    Setup {
        seq,
        tmp,
        tablets: Arc::new(Mutex::new(mgr)),
        fe,
        master,
    }
}

impl Setup {
    pub fn mgr(&self) -> GroupCommitMgr {
        GroupCommitMgr::new(self.tablets.clone(), self.fe.clone(), self.master.clone())
    }

    /// The suite's `n`th insert, planned.
    pub fn insert(&self, n: usize) -> WriteFragment {
        let p = self
            .seq
            .iter()
            .filter_map(|m| if let Msg::Exec(r) = m { Some(Msg::params(r)) } else { None })
            .nth(n)
            .unwrap();
        plan_write(&p).unwrap()
    }

    /// The insert's rows, checked.
    pub fn checked(&self, f: &WriteFragment) -> Checked {
        let sink = TableSink::new(f, vec![f.instance_id.clone()]).unwrap();
        sink.push(0, f.values_rows().unwrap(), &f.input_nullable()).unwrap();
        sink.check(&self.tmp.path().join("error_log")).1
    }

    pub fn puts(&self) -> Vec<TStreamLoadPutRequest> {
        self.fe
            .load_calls()
            .into_iter()
            .filter_map(|c| if let LoadCall::Put(p) = c { Some(p) } else { None })
            .collect()
    }

    pub fn commits(&self) -> Vec<TLoadTxnCommitRequest> {
        self.fe
            .load_calls()
            .into_iter()
            .filter_map(|c| if let LoadCall::Commit(p) = c { Some(p) } else { None })
            .collect()
    }

    fn recorded_put(&self) -> TStreamLoadPutRequest {
        self.seq
            .iter()
            .find_map(|m| if let Msg::Put(p) = m { Some(p.clone()) } else { None })
            .unwrap()
    }

    fn recorded_commit(&self) -> TLoadTxnCommitRequest {
        self.seq
            .iter()
            .find_map(|m| if let Msg::Commit(p) = m { Some(p.clone()) } else { None })
            .unwrap()
    }
}

fn until(what: &str, f: impl Fn() -> bool) {
    let end = Instant::now() + Duration::from_secs(5);
    while !f() {
        assert!(Instant::now() < end, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn first_load_puts_with_the_recorded_request() {
    let s = setup(|_| {});
    let mgr = s.mgr();
    let f = s.insert(0);
    let q = mgr.join(&f, s.checked(&f)).unwrap();
    let puts = s.puts();
    assert_eq!(puts.len(), 1);
    let mut got = puts[0].clone();
    let want = s.recorded_put();
    assert!(
        q.label().starts_with("group_commit_") && q.label().len() == "group_commit_".len() + 33,
        "{}",
        q.label()
    );
    assert_eq!(
        q.label(),
        format!("group_commit_{:016x}_{:016x}", got.load_id.hi as u64, got.load_id.lo as u64)
    );
    assert_eq!(q.txn_id(), 9036);
    // the label is that of a fresh load id: everything else as recorded
    let (label, want_label) = (got.label.clone().unwrap(), want.label.clone().unwrap());
    got.load_sql = got.load_sql.map(|s| s.replace(&label, &want_label));
    got.label = want.label.clone();
    got.load_id = want.load_id.clone();
    assert_eq!(got, want);
}

#[test]
fn loads_within_the_interval_share_a_queue() {
    let s = setup(|_| {});
    let mgr = s.mgr();
    let (a, b) = (s.insert(0), s.insert(1));
    let qa = mgr.join(&a, s.checked(&a)).unwrap();
    let qb = mgr.join(&b, s.checked(&b)).unwrap();
    assert_eq!(s.puts().len(), 1);
    assert_eq!((qa.label(), qa.txn_id()), (qb.label(), qb.txn_id()));
    assert!(s.commits().is_empty());
}

#[test]
fn the_interval_commits_the_queue() {
    let s = setup(|r| r.group_commit_interval_ms = Some(50));
    let mgr = s.mgr();
    let (a, b, c) = (s.insert(0), s.insert(1), s.insert(2));
    let q = mgr.join(&a, s.checked(&a)).unwrap();
    mgr.join(&b, s.checked(&b)).unwrap();
    mgr.join(&c, s.checked(&c)).unwrap();
    let st = q.wait(|| false).unwrap();
    assert_eq!(st.status_code, TStatusCode::OK);
    let commits = s.commits();
    assert_eq!(commits.len(), 1);
    let mut got = commits[0].clone();
    let mut want = s.recorded_commit();
    // the token is the heartbeat's; the tablets are those the rows went to
    got.token = want.token.clone();
    for c in [&mut got, &mut want] {
        c.commit_infos.as_mut().unwrap().sort_by_key(|i| i.tablet_id);
    }
    assert_eq!(got, want);
    // the next load starts a new queue
    let d = s.insert(3);
    mgr.join(&d, s.checked(&d)).unwrap();
    assert_eq!(s.puts().len(), 2);
}

#[test]
fn reaching_the_bytes_commits_at_once() {
    let s = setup(|r| r.group_commit_data_bytes = Some(1));
    let mgr = s.mgr();
    let a = s.insert(0);
    let q = mgr.join(&a, s.checked(&a)).unwrap();
    let started = Instant::now();
    assert_eq!(q.wait(|| false).unwrap().status_code, TStatusCode::OK);
    // the interval (5 s) did not pass
    assert!(started.elapsed() < Duration::from_secs(4));
    assert_eq!(s.commits().len(), 1);
}

#[test]
fn a_failed_internal_write_rolls_back() {
    let s = setup(|r| r.group_commit_data_bytes = Some(1));
    // a BE without the table's tablets
    let empty = tempfile::tempdir().unwrap();
    let none = TabletManager::open(vec![DataDir::open(empty.path(), "127.0.0.1").unwrap()], TabletOptions::default()).unwrap();
    let mgr = GroupCommitMgr::new(Arc::new(Mutex::new(none)), s.fe.clone(), s.master.clone());
    let a = s.insert(0);
    let q = mgr.join(&a, s.checked(&a)).unwrap();
    let st = q.wait(|| false).unwrap();
    assert_ne!(st.status_code, TStatusCode::OK);
    until("the rollback", || {
        s.fe.load_calls().iter().any(|c| matches!(c, LoadCall::Rollback(r) if r.txn_id == 9036))
    });
    assert!(s.commits().is_empty());
}

#[test]
fn a_refused_put_fails_the_join() {
    let s = setup(|r| {
        r.status = TStatus {
            status_code: TStatusCode::ANALYSIS_ERROR,
            error_msgs: Some(vec!["no table".into()]),
        }
    });
    let mgr = s.mgr();
    let a = s.insert(0);
    let Err(err) = mgr.join(&a, s.checked(&a)) else { panic!("joined") };
    assert!(err.contains("no table"), "{err}");
}

#[test]
fn a_stopped_wait_returns_none() {
    let s = setup(|_| {});
    let mgr = s.mgr();
    let a = s.insert(0);
    let q = mgr.join(&a, s.checked(&a)).unwrap();
    let t = Instant::now();
    assert!(q.wait(|| t.elapsed() > Duration::from_millis(100)).is_none());
}

// INSERT … VALUES through the load service

impl Setup {
    fn service(&self) -> LoadService {
        LoadService::new(self.tablets.clone(), self.fe.clone(), self.master.clone(), 8040)
    }

    fn exec(&self, n: usize) -> PExecPlanFragmentRequest {
        self.seq
            .iter()
            .filter_map(|m| if let Msg::Exec(r) = m { Some(r.clone()) } else { None })
            .nth(n)
            .unwrap()
    }

    /// The recorded final report of the `n`th insert.
    fn recorded_report(&self, n: usize) -> TReportExecStatusParams {
        let q = Msg::params(&self.exec(n)).query_id;
        self.seq
            .iter()
            .find_map(|m| match m {
                Msg::Report(r) if r.done == Some(true) && r.query_id.as_ref() == Some(&q) => Some(r.clone()),
                _ => None,
            })
            .unwrap()
    }

    fn reports(&self) -> Vec<TReportExecStatusParams> {
        self.fe
            .load_calls()
            .into_iter()
            .filter_map(|c| if let LoadCall::ExecStatus(_, r) = c { Some(r) } else { None })
            .collect()
    }
}

#[test]
fn async_insert_reports_the_queue() {
    let s = setup(|_| {});
    let svc = s.service();
    assert_eq!(svc.exec_plan_fragment(&s.exec(0)).status.status_code, 0);
    assert!(svc.wait_idle(Duration::from_secs(10)));
    let reports = s.reports();
    assert_eq!(reports.len(), 1, "{reports:?}");
    let mut got = reports[0].clone();
    assert_eq!(got.label, s.puts()[0].label);
    let want = s.recorded_report(0);
    got.label = want.label.clone();
    assert_eq!(got, want);
    // not committed yet: the queue's interval is 5 s
    assert!(s.commits().is_empty());
}

#[test]
fn sync_insert_waits_and_cancel_reports_cancelled() {
    let s = setup_for(TIMEOUT, |_| {});
    let svc = s.service();
    assert_eq!(svc.exec_plan_fragment(&s.exec(0)).status.status_code, 0);
    std::thread::sleep(Duration::from_millis(500));
    assert!(s.reports().is_empty(), "a sync load reports once its queue commits");
    let cancel = s
        .seq
        .iter()
        .find_map(|m| if let Msg::Cancel(c) = m { Some(c.clone()) } else { None })
        .unwrap();
    assert_eq!(svc.cancel_plan_fragment(&cancel).status.status_code, 0);
    assert!(svc.wait_idle(Duration::from_secs(5)));
    let reports = s.reports();
    assert_eq!(reports.len(), 1, "{reports:?}");
    let mut got = reports[0].clone();
    let want = s.recorded_report(0);
    got.label = want.label.clone();
    assert_eq!(got, want);
}

#[test]
fn sync_insert_reports_after_commit() {
    let s = setup_for(TIMEOUT, |r| r.group_commit_interval_ms = Some(50));
    let svc = s.service();
    assert_eq!(svc.exec_plan_fragment(&s.exec(0)).status.status_code, 0);
    assert!(svc.wait_idle(Duration::from_secs(10)));
    let calls = s.fe.load_calls();
    let commit = calls.iter().position(|c| matches!(c, LoadCall::Commit(_))).expect("a commit");
    let report = calls.iter().position(|c| matches!(c, LoadCall::ExecStatus(..))).expect("a report");
    assert!(commit < report, "{calls:?}");
    let LoadCall::ExecStatus(_, r) = &calls[report] else { unreachable!() };
    assert_eq!(r.status.as_ref().unwrap().status_code, TStatusCode::OK);
    assert_eq!(r.txn_id, Some(8996));
}

#[test]
fn wait_idle_ignores_open_queues() {
    let s = setup_for(INTERVAL, |r| r.group_commit_interval_ms = Some(300_000));
    let svc = s.service();
    svc.exec_plan_fragment(&s.exec(0));
    assert!(svc.wait_idle(Duration::from_secs(2)));
    assert_eq!(s.reports().len(), 1);
}

#[test]
fn a_filtered_group_commit_load_over_the_ratio_fails() {
    let s = setup_for(TIMEOUT, |_| {});
    let mut req = s.exec(0);
    // the row's id (INT NOT NULL) becomes NULL
    let mut p = Msg::params(&req);
    let union = p.fragment.as_mut().unwrap().plan.as_mut().unwrap().nodes[0].union_node.as_mut().unwrap();
    union.const_expr_lists[0][0] = doris_fixtures::build::null(doris_fixtures::build::ty(doris_thrift::gen::types::TPrimitiveType::INT));
    req.request = Some(encode(p));
    let svc = s.service();
    svc.exec_plan_fragment(&req);
    assert!(svc.wait_idle(Duration::from_secs(10)));
    let reports = s.reports();
    let st = reports[0].status.clone().unwrap();
    assert_eq!(st.status_code, TStatusCode::DATA_QUALITY_ERROR, "{st:?}");
    assert!(st.error_msgs.unwrap()[0].starts_with("too many filtered rows"));
    assert!(s.puts().is_empty() || s.commits().is_empty());
}

fn encode(p: doris_thrift::gen::palo_internal_service::TPipelineFragmentParams) -> Vec<u8> {
    use thrift::protocol::TSerializable;
    let list = doris_thrift::gen::palo_internal_service::TPipelineFragmentParamsList {
        params_list: Some(vec![p]),
        ..Default::default()
    };
    let mut buf = Vec::new();
    list.write_to_out_protocol(&mut thrift::protocol::TCompactOutputProtocol::new(&mut buf))
        .unwrap();
    buf
}
