//! Stream load through the BE, replayed against the reference's own loads from the corpus: the
//! FE answers as it answered the C++ BE, and the calls doris-rs makes and the reply it sends
//! must be the reference's.
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use doris_be::load::{LoadService, StreamLoadRequest};
use doris_be::master::MasterInfo;
use doris_be::testing::{FakeFe, LoadCall, LoadScript};
use doris_exec::fragment::plan_write;
use doris_fixtures::stream_loads::{stream_loads, LoadEnd, StreamLoadSample};
use doris_fixtures::values::create_requests_for;
use doris_storage::{DataDir, TabletManager, TabletOptions};

struct Run {
    reply: String,
    calls: Vec<LoadCall>,
    svc: LoadService,
    _tmp: tempfile::TempDir,
}

/// Runs a sample; None when its table or plan is outside M4.
fn run(s: &StreamLoadSample) -> Option<Run> {
    // the corpus keeps bodies over 64 KiB only as their length and hash
    if s.headers.get("content-length").and_then(|l| l.parse::<usize>().ok()).is_some_and(|l| l != s.body.len()) {
        return None;
    }
    let params = s.put.1.pipeline_params.clone()?;
    let f = plan_write(&params).ok()?;
    let tmp = tempfile::tempdir().unwrap();
    let tablets = Arc::new(Mutex::new(TabletManager::open(vec![DataDir::open(tmp.path(), "127.0.0.1").unwrap()], TabletOptions::default()).unwrap()));
    for req in create_requests_for(&f.sink) {
        tablets.lock().unwrap().create_tablet(&req).ok()?;
    }
    let fe = Arc::new(FakeFe::default());
    let (commit, rollback) = match &s.end {
        LoadEnd::Commit(_, r) => (Some(r.clone()), None),
        LoadEnd::Rollback(_, r) => (None, Some(r.clone())),
    };
    fe.script(LoadScript { begin: Some(s.begin.1.clone()), put: Some(s.put.1.clone()), commit, rollback });
    let master = MasterInfo::new("127.0.0.1");
    master.set_backend_id_for_tests(s.put.0.backend_id.unwrap_or(0));
    let svc = LoadService::new(tablets, fe.clone(), master, 8040);
    let parts: Vec<&str> = s.path.split('/').collect();
    let req = StreamLoadRequest {
        db: parts[2].to_string(),
        table: parts[3].to_string(),
        headers: s.headers.clone(),
        body: s.body.clone(),
        client_ip: s.begin.0.user_ip.clone().unwrap_or_default(),
    };
    let reply = svc.stream_load(req);
    Some(Run { reply, calls: fe.load_calls(), svc, _tmp: tmp })
}

/// The reply's keys in order, and its values with the times (and the error log's file, which
/// names this run's ids) taken out.
fn reply_shape(json: &str) -> (Vec<String>, BTreeMap<String, String>) {
    let mut keys = Vec::new();
    let mut values = BTreeMap::new();
    for line in json.lines() {
        let Some((k, v)) = line.trim().split_once("\": ") else { continue };
        let k = k.trim_start_matches('"').to_string();
        let v = v.trim_end_matches(',').to_string();
        keys.push(k.clone());
        if !k.ends_with("TimeMs") && k != "ErrorURL" {
            values.insert(k, v);
        }
    }
    (keys, values)
}

fn check(s: &StreamLoadSample, r: &Run) {
    let calls = &r.calls;
    let ctx = format!("{} ({})", s.dir, s.suite);
    // loadTxnBegin: the reference's request, but for the time and this load's id
    let LoadCall::Begin(mut begin) = calls[0].clone() else { panic!("{ctx}: {calls:?}") };
    begin.timestamp = s.begin.0.timestamp;
    begin.request_id = s.begin.0.request_id.clone();
    assert_eq!(begin, s.begin.0, "{ctx}: loadTxnBegin");
    let LoadCall::Put(mut put) = calls[1].clone() else { panic!("{ctx}: {calls:?}") };
    put.load_id = s.put.0.load_id.clone();
    assert_eq!(put, s.put.0, "{ctx}: streamLoadPut");
    match (&s.end, &calls[2]) {
        (LoadEnd::Commit(want, _), LoadCall::Commit(got)) => {
            let mut got = got.clone();
            let mut want = want.clone();
            for c in [&mut got, &mut want] {
                if let Some(i) = c.commit_infos.as_mut() {
                    i.sort_by_key(|x| x.tablet_id);
                }
            }
            assert_eq!(got, want, "{ctx}: loadTxnCommit");
        }
        (LoadEnd::Rollback(want, _), LoadCall::Rollback(got)) => assert_eq!(got, want, "{ctx}: loadTxnRollback"),
        (_, other) => panic!("{ctx}: ended with {other:?}"),
    }
    assert_eq!(calls.len(), 3, "{ctx}");
    let (want_keys, want) = reply_shape(&s.reply);
    let (got_keys, got) = reply_shape(&r.reply);
    assert_eq!(got_keys, want_keys, "{ctx}: reply keys\n{}", r.reply);
    assert_eq!(got, want, "{ctx}: reply\n{}", r.reply);
    assert!(r.reply.ends_with("}\n"), "{ctx}");
    if s.reply.contains("\"ErrorURL\"") {
        let url = r.reply.lines().find(|l| l.contains("\"ErrorURL\"")).unwrap();
        assert!(url.contains("\"http://127.0.0.1:8040/api/_load_error_log?file=__shard_"), "{ctx}: {url}");
        let file = url.split("file=").nth(1).unwrap().trim_end_matches(['"', ',']);
        assert!(r.svc.error_log(file).is_some_and(|b| !b.is_empty()), "{ctx}: {file}");
    }
}

/// Loads whose reference outcome came from the reference's own environment (memory limits).
fn noise(s: &StreamLoadSample) -> bool {
    s.reply.contains("MEM_LIMIT") || s.reply.contains("MEM_ALLOC")
}

fn replay(format: &str, rolled_back: bool) -> usize {
    let mut n = 0;
    for s in stream_loads() {
        let fmt = s.headers.get("format").map_or("csv", String::as_str).to_ascii_lowercase();
        if fmt != format || matches!(s.end, LoadEnd::Rollback(..)) != rolled_back || noise(&s) {
            continue;
        }
        let Some(r) = run(&s) else { continue };
        check(&s, &r);
        n += 1;
    }
    n
}

#[test]
fn csv_stream_loads_match_the_corpus() {
    let n = replay("csv", false);
    assert!(n >= 5, "{n}");
}

#[test]
fn json_stream_loads_match_the_corpus() {
    let n = replay("json", false);
    assert!(n >= 3, "{n}");
}

#[test]
fn rolled_back_loads_match_the_corpus() {
    // too many filtered rows: the reply fails, names the error log, and the log is served
    let n = replay("csv", true) + replay("json", true);
    assert!(n >= 3, "{n}");
}

#[test]
fn group_commit_is_refused() {
    // the FE answers loadTxnBegin with a group commit mode: refused, rolled back
    let s = stream_loads().into_iter().find(|s| matches!(s.end, LoadEnd::Commit(..)) && !noise(s) && run(s).is_some()).unwrap();
    let mut s = s.clone();
    s.begin.1.table_group_commit_mode = Some("async_mode".into());
    let rollback = doris_thrift::gen::frontend_service::TLoadTxnRollbackResult::new(s.begin.1.status.clone());
    let rollback_req = match &s.end {
        LoadEnd::Commit(c, _) => doris_thrift::gen::frontend_service::TLoadTxnRollbackRequest::new(
            None, c.user.clone(), c.passwd.clone(), c.db.clone(), c.tbl.clone(), c.user_ip.clone(), c.txn_id, None, None, None, None, None, None, None, None,
        ),
        LoadEnd::Rollback(r, _) => r.clone(),
    };
    s.end = LoadEnd::Rollback(rollback_req, rollback);
    let r = run(&s).unwrap();
    assert!(r.reply.contains("\"Status\": \"Fail\""), "{}", r.reply);
    assert!(r.reply.contains("group commit"), "{}", r.reply);
    assert!(matches!(r.calls.last(), Some(LoadCall::Rollback(_))), "{:?}", r.calls);
    assert_eq!(r.calls.len(), 2, "no streamLoadPut: {:?}", r.calls);
}

#[test]
fn stream_load_records_are_served() {
    let s = stream_loads().into_iter().find(|s| matches!(s.end, LoadEnd::Commit(..)) && !noise(s) && run(s).is_some()).unwrap();
    let r = run(&s).unwrap();
    let records = r.svc.stream_load_records(0);
    assert_eq!(records.len(), 1);
    let rec = records.values().next().unwrap();
    assert_eq!(rec.label, s.headers["label"]);
    assert_eq!(rec.status, "Success");
}
