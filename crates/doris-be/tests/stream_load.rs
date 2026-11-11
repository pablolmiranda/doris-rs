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
    run_with(s, |_, _| {})
}

/// Runs a sample after `adjust` has changed the service or the request the HTTP layer hands over.
fn run_with(s: &StreamLoadSample, adjust: impl FnOnce(&LoadService, &mut StreamLoadRequest)) -> Option<Run> {
    // the corpus keeps bodies over 64 KiB only as their length and hash
    if s.headers
        .get("content-length")
        .and_then(|l| l.parse::<usize>().ok())
        .is_some_and(|l| l != s.body.len())
    {
        return None;
    }
    let params = s.put.1.pipeline_params.clone()?;
    let f = plan_write(&params).ok()?;
    let tmp = tempfile::tempdir().unwrap();
    let tablets = Arc::new(Mutex::new(
        TabletManager::open(vec![DataDir::open(tmp.path(), "127.0.0.1").unwrap()], TabletOptions::default()).unwrap(),
    ));
    for req in create_requests_for(&f.sink) {
        tablets.lock().unwrap().create_tablet(&req).ok()?;
    }
    let fe = Arc::new(FakeFe::default());
    let (commit, rollback) = match &s.end {
        LoadEnd::Commit(_, r) => (Some(r.clone()), None),
        LoadEnd::Rollback(_, r) => (None, Some(r.clone())),
    };
    fe.script(LoadScript {
        begin: Some(s.begin.1.clone()),
        put: Some(s.put.1.clone()),
        commit,
        rollback,
    });
    let master = MasterInfo::new("127.0.0.1");
    master.set_backend_id_for_tests(s.put.0.backend_id.unwrap_or(0));
    let svc = LoadService::new(tablets, fe.clone(), master, 8040);
    let parts: Vec<&str> = s.path.split('/').collect();
    let mut req = StreamLoadRequest {
        db: parts[2].to_string(),
        table: parts[3].to_string(),
        headers: s.headers.clone(),
        body: Ok(s.body.clone()),
        client_ip: s.begin.0.user_ip.clone().unwrap_or_default(),
    };
    adjust(&svc, &mut req);
    let reply = svc.stream_load(req);
    Some(Run {
        reply,
        calls: fe.load_calls(),
        svc,
        _tmp: tmp,
    })
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
    let LoadCall::Begin(mut begin) = calls[0].clone() else {
        panic!("{ctx}: {calls:?}")
    };
    begin.timestamp = s.begin.0.timestamp;
    begin.request_id = s.begin.0.request_id.clone();
    assert_eq!(begin, s.begin.0, "{ctx}: loadTxnBegin");
    let LoadCall::Put(mut put) = calls[1].clone() else {
        panic!("{ctx}: {calls:?}")
    };
    put.load_id = s.put.0.load_id.clone();
    // a Parquet or ORC body is saved first (`_data_saved_path`): its path holds the time
    if let Some(path) = put.path.as_deref() {
        let db = &s.put.0.db;
        assert!(path.contains(&format!("/mini_download/{db}/__shard_")), "{ctx}: {path}");
        assert!(path.contains(&format!("//{}.", s.put.0.tbl)), "{ctx}: {path}");
        put.path = s.put.0.path.clone();
    }
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
fn parquet_and_orc_stream_loads_match_the_corpus() {
    // test_load_with_decimal: a Parquet and an ORC file of decimals, read as text and cast
    assert!(replay("parquet", false) >= 1);
    assert!(replay("orc", false) >= 1);
}

#[test]
fn csv_with_names_stream_loads_match_the_corpus() {
    // test_csv_with_skip_lines: format csv_with_names[_and_types] is CSV whose first one or two
    // lines are a header (StreamLoadAction::_on_header sets header_type)
    assert!(replay("csv_with_names", false) >= 1);
    assert!(replay("csv_with_names_and_types", false) >= 1);
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
    let s = stream_loads()
        .into_iter()
        .find(|s| matches!(s.end, LoadEnd::Commit(..)) && !noise(s) && run(s).is_some())
        .unwrap();
    let mut s = s.clone();
    s.begin.1.table_group_commit_mode = Some("async_mode".into());
    let rollback = doris_thrift::gen::frontend_service::TLoadTxnRollbackResult::new(s.begin.1.status.clone());
    let rollback_req = match &s.end {
        LoadEnd::Commit(c, _) => doris_thrift::gen::frontend_service::TLoadTxnRollbackRequest::new(
            None,
            c.user.clone(),
            c.passwd.clone(),
            c.db.clone(),
            c.tbl.clone(),
            c.user_ip.clone(),
            c.txn_id,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
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
    let s = stream_loads()
        .into_iter()
        .find(|s| matches!(s.end, LoadEnd::Commit(..)) && !noise(s) && run(s).is_some())
        .unwrap();
    let r = run(&s).unwrap();
    let records = r.svc.stream_load_records(0);
    assert_eq!(records.len(), 1);
    let rec = records.values().next().unwrap();
    assert_eq!(rec.label, s.headers["label"]);
    assert_eq!(rec.status, "Success");
}

/// A plain CSV load the corpus committed, which runs under M4.
fn committed_csv() -> StreamLoadSample {
    stream_loads()
        .into_iter()
        .find(|s| {
            matches!(s.end, LoadEnd::Commit(..))
                && !noise(s)
                && s.headers.get("format").is_none_or(|f| f.eq_ignore_ascii_case("csv"))
                && !s.headers.contains_key("compress_type")
                && run(s).is_some()
        })
        .unwrap()
}

/// The reply's value for a key, as its JSON text.
fn reply_value(reply: &str, key: &str) -> String {
    reply_shape(reply).1.get(key).cloned().unwrap_or_default()
}

#[test]
fn a_gzipped_csv_load_loads_what_the_plain_one_does() {
    // compress_type GZ: the put asks the FE for FORMAT_CSV_GZ, and the body is read through gzip
    use doris_thrift::gen::plan_nodes::{TFileCompressType, TFileFormatType};
    use std::io::Write;
    let s = committed_csv();
    let plain = run(&s).unwrap();
    let mut gz = s.clone();
    let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    enc.write_all(&s.body).unwrap();
    gz.body = enc.finish().unwrap();
    gz.headers.insert("content-length".into(), gz.body.len().to_string());
    gz.headers.insert("compress_type".into(), "gz".into());
    // the FE plans the scan as it was asked to
    let params = gz.put.1.pipeline_params.as_mut().unwrap();
    for p in params.file_scan_params.iter_mut().flat_map(|m| m.values_mut()) {
        p.format_type = Some(TFileFormatType::FORMAT_CSV_GZ);
        p.compress_type = Some(TFileCompressType::GZ);
    }
    for r in params
        .local_params
        .iter_mut()
        .flatten()
        .flat_map(|l| l.per_node_scan_ranges.values_mut())
        .flatten()
    {
        if let Some(p) = r
            .scan_range
            .ext_scan_range
            .as_mut()
            .and_then(|e| e.file_scan_range.as_mut())
            .and_then(|f| f.params.as_mut())
        {
            p.format_type = Some(TFileFormatType::FORMAT_CSV_GZ);
            p.compress_type = Some(TFileCompressType::GZ);
        }
    }
    let r = run(&gz).unwrap();
    let LoadCall::Put(put) = &r.calls[1] else { panic!("{:?}", r.calls) };
    assert_eq!(
        (put.format_type, put.compress_type),
        (TFileFormatType::FORMAT_CSV_GZ, Some(TFileCompressType::GZ))
    );
    for key in ["Status", "Message", "NumberTotalRows", "NumberLoadedRows", "NumberFilteredRows"] {
        assert_eq!(reply_value(&r.reply, key), reply_value(&plain.reply, key), "{key}: {}", r.reply);
    }
}

#[test]
fn a_timeout_that_is_not_a_number_fails_before_the_transaction() {
    // StreamLoadAction::_on_header: safe_stoi(timeout), before loadTxnBegin
    let mut s = committed_csv();
    s.headers.insert("timeout".into(), "abc".into());
    let r = run(&s).unwrap();
    assert_eq!(reply_value(&r.reply, "Status"), "\"Fail\"", "{}", r.reply);
    assert_eq!(
        reply_value(&r.reply, "Message"),
        "\"[INVALID_ARGUMENT]Invalid format of 'timeout': 'abc', stoi\""
    );
    assert!(r.calls.is_empty(), "{:?}", r.calls);
}

#[test]
fn a_negative_skip_lines_fails_and_rolls_back() {
    // StreamLoadAction::_process_put: safe_stoi(skip_lines), then the sign, after loadTxnBegin
    let mut s = committed_csv();
    s.headers.insert("skip_lines".into(), "-3".into());
    let LoadEnd::Commit(c, _) = &s.end else { unreachable!() };
    let rollback_req = doris_thrift::gen::frontend_service::TLoadTxnRollbackRequest::new(
        None,
        c.user.clone(),
        c.passwd.clone(),
        c.db.clone(),
        c.tbl.clone(),
        c.user_ip.clone(),
        c.txn_id,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    );
    let rollback = doris_thrift::gen::frontend_service::TLoadTxnRollbackResult::new(s.begin.1.status.clone());
    s.end = LoadEnd::Rollback(rollback_req, rollback);
    let r = run(&s).unwrap();
    assert_eq!(
        reply_value(&r.reply, "Message"),
        "\"[INVALID_ARGUMENT]Invalid 'skip_lines': -3\"",
        "{}",
        r.reply
    );
    assert!(matches!(r.calls[..], [LoadCall::Begin(_), LoadCall::Rollback(_)]), "{:?}", r.calls);
}

#[test]
fn an_fe_error_is_quoted_by_its_code_name() {
    // Status::create(TStatus): "[<code>]TStatus: <first message>"
    use doris_thrift::gen::status::TStatusCode;
    let mut s = committed_csv();
    s.put.1.status.status_code = TStatusCode::ANALYSIS_ERROR;
    s.put.1.status.error_msgs = Some(vec!["AnalysisException: no such column".into(), "second".into()]);
    let LoadEnd::Commit(c, _) = &s.end else { unreachable!() };
    let rollback_req = doris_thrift::gen::frontend_service::TLoadTxnRollbackRequest::new(
        None,
        c.user.clone(),
        c.passwd.clone(),
        c.db.clone(),
        c.tbl.clone(),
        c.user_ip.clone(),
        c.txn_id,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    );
    let rollback = doris_thrift::gen::frontend_service::TLoadTxnRollbackResult::new(s.begin.1.status.clone());
    s.end = LoadEnd::Rollback(rollback_req, rollback);
    let r = run(&s).unwrap();
    assert_eq!(
        reply_value(&r.reply, "Message"),
        "\"[ANALYSIS_ERROR]TStatus: AnalysisException: no such column\"",
        "{}",
        r.reply
    );
}

/// The sample, ending in the rollback of its transaction instead of its commit.
fn rolled_back(mut s: StreamLoadSample) -> StreamLoadSample {
    let LoadEnd::Commit(c, _) = &s.end else { unreachable!() };
    let rollback_req = doris_thrift::gen::frontend_service::TLoadTxnRollbackRequest::new(
        None,
        c.user.clone(),
        c.passwd.clone(),
        c.db.clone(),
        c.tbl.clone(),
        c.user_ip.clone(),
        c.txn_id,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    );
    let rollback = doris_thrift::gen::frontend_service::TLoadTxnRollbackResult::new(s.begin.1.status.clone());
    s.end = LoadEnd::Rollback(rollback_req, rollback);
    s
}

#[test]
fn a_body_that_could_not_be_read_fails_without_a_transaction() {
    // a client that disconnects part way: nothing is loaded, and the label stays free
    let s = committed_csv();
    let r = run_with(&s, |_, req| req.body = Err("connection closed before message completed".into())).unwrap();
    assert_eq!(reply_value(&r.reply, "Status"), "\"Fail\"", "{}", r.reply);
    assert!(
        reply_value(&r.reply, "Message").contains("connection closed before message completed"),
        "{}",
        r.reply
    );
    assert!(r.calls.is_empty(), "{:?}", r.calls);
}

#[test]
fn a_body_over_streaming_load_max_mb_fails_before_the_transaction() {
    // StreamLoadAction::_on_header: Content-Length against streaming_load_max_mb
    let s = committed_csv();
    let r = run_with(&s, |svc, req| {
        svc.set_streaming_load_max_mb(1);
        req.headers.insert("content-length".into(), "2097152".into());
    })
    .unwrap();
    assert_eq!(
        reply_value(&r.reply, "Message"),
        "\"[EXCEEDED_LIMIT]body size 2097152 bytes (2.00 MiB) exceeds the limit of 1048576 bytes (1 MiB) set by BE's conf streaming_load_max_mb. Increase it if you are sure this load is reasonable\"",
        "{}",
        r.reply
    );
    assert!(r.calls.is_empty(), "{:?}", r.calls);
}

#[test]
fn a_gzip_body_that_expands_past_the_limit_fails_and_rolls_back() {
    // a small gzip body of many zeros: it is read up to streaming_load_max_mb, not whole
    use doris_thrift::gen::plan_nodes::{TFileCompressType, TFileFormatType};
    use std::io::Write;
    let mut s = rolled_back(committed_csv());
    let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::best());
    enc.write_all(&vec![b'0'; 3 << 20]).unwrap();
    s.body = enc.finish().unwrap();
    s.headers.insert("content-length".into(), s.body.len().to_string());
    s.headers.insert("compress_type".into(), "gz".into());
    let params = s.put.1.pipeline_params.as_mut().unwrap();
    for p in params.file_scan_params.iter_mut().flat_map(|m| m.values_mut()) {
        p.format_type = Some(TFileFormatType::FORMAT_CSV_GZ);
        p.compress_type = Some(TFileCompressType::GZ);
    }
    for r in params
        .local_params
        .iter_mut()
        .flatten()
        .flat_map(|l| l.per_node_scan_ranges.values_mut())
        .flatten()
    {
        if let Some(p) = r
            .scan_range
            .ext_scan_range
            .as_mut()
            .and_then(|e| e.file_scan_range.as_mut())
            .and_then(|f| f.params.as_mut())
        {
            p.format_type = Some(TFileFormatType::FORMAT_CSV_GZ);
            p.compress_type = Some(TFileCompressType::GZ);
        }
    }
    let r = run_with(&s, |svc, _| svc.set_streaming_load_max_mb(1)).unwrap();
    assert_eq!(reply_value(&r.reply, "Status"), "\"Fail\"", "{}", r.reply);
    assert!(reply_value(&r.reply, "Message").contains("streaming_load_max_mb"), "{}", r.reply);
    assert!(
        matches!(r.calls[..], [LoadCall::Begin(_), LoadCall::Put(_), LoadCall::Rollback(_)]),
        "{:?}",
        r.calls
    );
}

#[test]
fn empty_jsonpaths_match_columns_by_name() {
    // `jsonpaths: []` is no jsonpaths: fields are found by column name (load_json_with_jsonpath)
    let s = stream_loads()
        .into_iter()
        .find(|s| {
            matches!(s.end, LoadEnd::Commit(..))
                && !noise(s)
                && s.headers.get("format").is_some_and(|f| f.eq_ignore_ascii_case("json"))
                && s.headers.get("jsonpaths").is_none_or(|p| p.is_empty())
                && run(s).is_some_and(|r| reply_value(&r.reply, "NumberLoadedRows") != "0")
        })
        .expect("a committed JSON load without jsonpaths");
    let plain = run(&s).unwrap();
    let mut empty = s.clone();
    empty.headers.insert("jsonpaths".into(), "[]".into());
    let params = empty.put.1.pipeline_params.as_mut().unwrap();
    let ranges = params
        .local_params
        .iter_mut()
        .flatten()
        .flat_map(|l| l.per_node_scan_ranges.values_mut())
        .flatten();
    let in_ranges = ranges.filter_map(|r| {
        r.scan_range
            .ext_scan_range
            .as_mut()
            .and_then(|e| e.file_scan_range.as_mut())
            .and_then(|f| f.params.as_mut())
    });
    let mut set = 0;
    for p in params.file_scan_params.iter_mut().flat_map(|m| m.values_mut()).chain(in_ranges) {
        if let Some(a) = p.file_attributes.as_mut() {
            a.jsonpaths = Some("[]".into());
            set += 1;
        }
    }
    assert!(set > 0, "no file attributes to set");
    let r = run(&empty).unwrap();
    for key in ["Status", "NumberTotalRows", "NumberLoadedRows", "NumberFilteredRows"] {
        assert_eq!(reply_value(&r.reply, key), reply_value(&plain.reply, key), "{key}: {}", r.reply);
    }
}

// group commit (`insert_group_commit_with_large_data`'s stream load)

mod group_commit {
    use super::*;
    use doris_fixtures::group_commit::{sequence, Msg, LARGE_DATA};
    use doris_thrift::gen::frontend_service::{TLoadTxnCommitResult, TStreamLoadPutRequest};
    use doris_thrift::gen::status::{TStatus, TStatusCode};
    use doris_thrift::gen::types::TTaskType;
    use std::time::{Duration, Instant};

    struct Recorded {
        headers: BTreeMap<String, String>,
        body: Vec<u8>,
        put: TStreamLoadPutRequest,
        reply: String,
        svc: LoadService,
        fe: Arc<FakeFe>,
        _tmp: tempfile::TempDir,
    }

    /// The recorded load, ready to run: the suite's tablets, the FE's two put replies.
    fn recorded() -> Recorded {
        let seq = sequence(LARGE_DATA);
        let h = seq.iter().position(|m| matches!(m, Msg::HttpRequest { .. })).unwrap();
        let Msg::HttpRequest { headers, body } = seq[h].clone() else {
            unreachable!()
        };
        let put = seq[..h]
            .iter()
            .rev()
            .find_map(|m| if let Msg::Put(p) = m { Some(p.clone()) } else { None })
            .unwrap();
        let replies: Vec<_> = seq[h..]
            .iter()
            .filter_map(|m| if let Msg::PutReply(r) = m { Some(r.clone()) } else { None })
            .take(2)
            .collect();
        let reply = seq
            .iter()
            .find_map(|m| if let Msg::HttpResponse(r) = m { Some(r.clone()) } else { None })
            .unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let mut mgr = TabletManager::open(vec![DataDir::open(tmp.path(), "127.0.0.1").unwrap()], TabletOptions::default()).unwrap();
        for m in &seq {
            if let Msg::Create(tasks) = m {
                for t in tasks.iter().filter(|t| t.task_type == TTaskType::CREATE) {
                    mgr.create_tablet(t.create_tablet_req.as_ref().unwrap()).unwrap();
                }
            }
        }
        let fe = Arc::new(FakeFe::default());
        fe.queue_puts(replies);
        fe.script(LoadScript {
            commit: Some(TLoadTxnCommitResult {
                status: TStatus {
                    status_code: TStatusCode::OK,
                    error_msgs: None,
                },
            }),
            ..Default::default()
        });
        let master = MasterInfo::new("127.0.0.1");
        master.set_backend_id_for_tests(put.backend_id.unwrap());
        let svc = LoadService::new(Arc::new(Mutex::new(mgr)), fe.clone(), master, 8040);
        Recorded {
            headers,
            body,
            put,
            reply,
            svc,
            fe,
            _tmp: tmp,
        }
    }

    impl Recorded {
        fn load(&self, headers: BTreeMap<String, String>) -> String {
            self.svc.stream_load(StreamLoadRequest {
                db: self.put.db.clone(),
                table: self.put.tbl.clone(),
                headers,
                body: Ok(self.body.clone()),
                client_ip: "127.0.0.1".into(),
            })
        }
    }

    #[test]
    fn group_commit_stream_load_skips_begin() {
        let r = recorded();
        let got = r.load(r.headers.clone());
        let calls = r.fe.load_calls();
        assert!(!calls.iter().any(|c| matches!(c, LoadCall::Begin(_))), "{calls:?}");
        let LoadCall::Put(mut put) = calls[0].clone() else { panic!("{calls:?}") };
        put.load_id = r.put.load_id.clone();
        assert_eq!(put, r.put);
        // the reply: the recorded keys in order, and values but for times, the label and load id
        let (got_keys, mut got_values) = reply_shape(&got);
        let (want_keys, mut want_values) = reply_shape(&r.reply);
        assert_eq!(got_keys, want_keys, "{got}");
        let label = got_values["Label"].trim_matches('"').to_string();
        assert!(label.starts_with("group_commit_"), "{got}");
        for k in ["Label", "LoadId"] {
            got_values.remove(k);
            want_values.remove(k);
        }
        assert_eq!(got_values, want_values, "{got}");
        // the queue (interval 40 ms) commits the rows
        let end = Instant::now() + Duration::from_secs(5);
        while !r.fe.load_calls().iter().any(|c| matches!(c, LoadCall::Commit(c) if c.txn_id == 9062)) {
            assert!(Instant::now() < end, "no commit");
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    #[test]
    fn the_load_id_is_the_puts() {
        let r = recorded();
        let got = r.load(r.headers.clone());
        let LoadCall::Put(put) = r.fe.load_calls()[0].clone() else { panic!() };
        let id = format!("\"{:016x}-{:016x}\"", put.load_id.hi as u64, put.load_id.lo as u64);
        assert_eq!(reply_shape(&got).1["LoadId"], id);
    }

    #[test]
    fn group_commit_header_errors() {
        let r = recorded();
        let mut h = r.headers.clone();
        h.insert("group_commit".into(), "bad".into());
        let got = r.load(h);
        assert!(
            got.contains("[INVALID_ARGUMENT]group_commit can only be [async_mode, sync_mode, off_mode]"),
            "{got}"
        );
        let mut h = r.headers.clone();
        h.insert("label".into(), "l1".into());
        let got = r.load(h);
        assert!(
            got.contains("[INVALID_ARGUMENT]label and group_commit can't be set at the same time"),
            "{got}"
        );
        assert!(r.fe.load_calls().is_empty());
    }

    #[test]
    fn off_mode_loads_as_before() {
        let r = recorded();
        let mut h = r.headers.clone();
        h.insert("group_commit".into(), "OFF_MODE".into());
        let _ = r.load(h);
        // a transaction of its own (the FE here answers no begin)
        assert!(matches!(r.fe.load_calls().first(), Some(LoadCall::Begin(_))), "{:?}", r.fe.load_calls());
    }
}
