//! The recorded group-commit messages (`crates/doris-fixtures/group_commit`) decode, in order.
use doris_fixtures::group_commit::{sequence, Msg, INTERVAL, LARGE_DATA, TIMEOUT};
use doris_thrift::gen::data_sinks::{TDataSinkType, TGroupCommitMode};
use doris_thrift::gen::status::TStatusCode;

fn count(seq: &[Msg], f: impl Fn(&Msg) -> bool) -> usize {
    seq.iter().filter(|m| f(m)).count()
}

#[test]
fn recorded_group_commit_sequences() {
    let t = sequence(TIMEOUT);
    let exec = t
        .iter()
        .find_map(|m| if let Msg::Exec(r) = m { Some(Msg::params(r)) } else { None })
        .unwrap();
    let sink = exec.fragment.as_ref().unwrap().output_sink.as_ref().unwrap();
    assert_eq!(sink.type_, TDataSinkType::GROUP_COMMIT_BLOCK_SINK);
    assert_eq!(
        sink.olap_table_sink.as_ref().unwrap().group_commit_mode,
        Some(TGroupCommitMode::SYNC_MODE)
    );
    let put = t.iter().find_map(|m| if let Msg::Put(p) = m { Some(p.clone()) } else { None }).unwrap();
    assert!(
        put.load_sql.as_deref().unwrap().starts_with("insert into doris_internal_table_id("),
        "{:?}",
        put.load_sql
    );
    let reply = t
        .iter()
        .find_map(|m| if let Msg::PutReply(p) = m { Some(p.clone()) } else { None })
        .unwrap();
    assert_eq!(reply.group_commit_interval_ms, Some(300000));
    assert_eq!(count(&t, |m| matches!(m, Msg::Cancel(_))), 1);
    let report = t
        .iter()
        .find_map(|m| if let Msg::Report(r) = m { Some(r.clone()) } else { None })
        .unwrap();
    assert_eq!(report.status.unwrap().status_code, TStatusCode::CANCELLED);

    let i = sequence(INTERVAL);
    assert_eq!(count(&i, |m| matches!(m, Msg::Exec(_))), 4);
    assert_eq!(count(&i, |m| matches!(m, Msg::Put(_))), 2);
    // a done=false report while the first queue is created, then one per insert
    assert_eq!(count(&i, |m| matches!(m, Msg::Report(r) if r.done == Some(true))), 4);
    assert_eq!(count(&i, |m| matches!(m, Msg::Commit(c) if c.group_commit == Some(true))), 1);

    let l = sequence(LARGE_DATA);
    let http = l
        .iter()
        .find_map(|m| {
            if let Msg::HttpRequest { headers, .. } = m {
                Some(headers.clone())
            } else {
                None
            }
        })
        .unwrap();
    assert_eq!(http.get("group_commit").map(String::as_str), Some("async_mode"));
    assert_eq!(count(&l, |m| matches!(m, Msg::HttpResponse(_))), 1);
}
