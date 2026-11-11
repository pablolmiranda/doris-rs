//! Group-commit plans (`GROUP_COMMIT_BLOCK_SINK`, and the FE's internal `GROUP_COMMIT_SCAN_NODE`
//! plan), and a table sink's rows checked now and committed later under another transaction.
use std::sync::Mutex;

use doris_exec::fragment::{is_load_plan, plan_write, WritePlan};
use doris_exec::sink::{commit_checked, run_sink, TableSink};
use doris_exec::value::Value;
use doris_fixtures::group_commit::{sequence, Msg, TIMEOUT};
use doris_fixtures::values::{values_vectors, ValuesVector};
use doris_storage::{DataDir, TabletManager, TabletOptions};
use doris_thrift::gen::data_sinks::TGroupCommitMode;
use doris_thrift::gen::palo_internal_service::TPipelineFragmentParamsList;
use doris_thrift::gen::status::TStatusCode;

fn manager(v: &ValuesVector, root: &std::path::Path) -> Option<Mutex<TabletManager>> {
    let mut mgr = TabletManager::open(vec![DataDir::open(root, "127.0.0.1").unwrap()], TabletOptions::default()).unwrap();
    for req in v.create_requests() {
        mgr.create_tablet(&req).ok()?;
        doris_storage::rowset::segment_schema(mgr.tablet(req.tablet_id)?.schema.as_ref()?).ok()?;
    }
    Some(Mutex::new(mgr))
}

#[test]
fn group_commit_fragment_plans() {
    let seq = sequence(TIMEOUT);
    let exec = seq
        .iter()
        .find_map(|m| if let Msg::Exec(r) = m { Some(Msg::params(r)) } else { None })
        .unwrap();
    let f = plan_write(&exec).unwrap();
    assert_eq!(f.group_commit, Some(TGroupCommitMode::SYNC_MODE));
    assert_eq!(f.plan, WritePlan::Values);
    assert_eq!(f.values_rows().unwrap().len(), 1);
    let list = TPipelineFragmentParamsList {
        params_list: Some(vec![exec]),
        ..Default::default()
    };
    assert!(is_load_plan(&list));

    let reply = seq
        .iter()
        .find_map(|m| if let Msg::PutReply(r) = m { Some(r.clone()) } else { None })
        .unwrap();
    let internal = plan_write(reply.pipeline_params.as_ref().unwrap()).unwrap();
    assert_eq!(internal.plan, WritePlan::GroupCommit);
    assert_eq!(internal.group_commit, None);
    assert_eq!(internal.sink.txn_id, 8996);
}

/// The first vector whose tablets M4 can create, with at least two rows.
fn vector() -> (ValuesVector, tempfile::TempDir, Mutex<TabletManager>) {
    for v in values_vectors() {
        let Ok(f) = plan_write(&v.params()) else { continue };
        if !f.values_rows().is_ok_and(|r| r.len() >= 2) {
            continue;
        }
        let tmp = tempfile::tempdir().unwrap();
        if let Some(m) = manager(&v, tmp.path()) {
            return (v, tmp, m);
        }
    }
    panic!("no vector");
}

#[test]
fn checked_rows_commit_later() {
    let (v, tmp, mgr) = vector();
    let f = plan_write(&v.params()).unwrap();
    let whole = run_sink(&f, f.values_rows().unwrap(), &mgr, &tmp.path().join("error_log"));

    let (v2, tmp2, mgr2) = vector();
    assert_eq!(v2.dir, v.dir);
    let sink = TableSink::new(&f, vec![f.instance_id.clone()]).unwrap();
    sink.push(0, f.values_rows().unwrap(), &f.input_nullable()).unwrap();
    let (checked_outcome, checked) = sink.check(&tmp2.path().join("error_log"));
    // checking counts as the whole sink does, and writes nothing
    assert_eq!(
        (checked_outcome.loaded, checked_outcome.filtered, checked_outcome.bytes),
        (whole.loaded, whole.filtered, whole.bytes)
    );
    assert!(checked_outcome.commit_infos.is_empty());
    assert_eq!(checked.loaded(), whole.loaded);

    // committed under another transaction: the same tablets
    let mut later = f.clone();
    later.sink.txn_id = f.sink.txn_id + 1000;
    let committed = commit_checked(&later, vec![checked], &mgr2);
    assert_eq!(committed.status.status_code, TStatusCode::OK);
    let ids = |o: &doris_exec::sink::SinkOutcome| {
        let mut t: Vec<i64> = o.commit_infos.iter().map(|c| c.tablet_id).collect();
        t.sort();
        t
    };
    assert_eq!(ids(&committed), ids(&whole));
    assert_eq!(committed.loaded, whole.loaded);
}

#[test]
fn a_filtered_row_is_not_checked_in() {
    let (v, tmp, _mgr) = vector();
    let f = plan_write(&v.params()).unwrap();
    let Some(col) = f.dest_columns().iter().position(|c| !c.nullable) else {
        // every column nullable: a row error stands in for the bad row
        let mut rows = f.values_rows().unwrap();
        let total = rows.len() as i64;
        rows[0] = Err(doris_exec::RowError::new("bad"));
        let sink = TableSink::new(&f, vec![f.instance_id.clone()]).unwrap();
        sink.push(0, rows, &f.input_nullable()).unwrap();
        let (o, checked) = sink.check(&tmp.path().join("error_log"));
        assert_eq!((o.filtered, o.loaded, checked.loaded()), (1, total - 1, total - 1));
        assert!(o.error_log.is_some());
        return;
    };
    let mut rows = f.values_rows().unwrap();
    let total = rows.len() as i64;
    rows[0].as_mut().unwrap()[col] = Value::Null;
    let sink = TableSink::new(&f, vec![f.instance_id.clone()]).unwrap();
    sink.push(0, rows, &f.input_nullable()).unwrap();
    let (o, checked) = sink.check(&tmp.path().join("error_log"));
    assert_eq!((o.filtered, o.loaded, checked.loaded()), (1, total - 1, total - 1));
    assert!(o.error_log.is_some());
}

/// A queued row's bytes count every column with a null map (the queue's block is made
/// nullable): the recorded commit has `receive_bytes` 87 for three `(k, v)` rows of a unique
/// table, whose reports say 25 bytes each.
#[test]
fn queued_bytes_are_those_of_a_nullable_block() {
    let seq = sequence(doris_fixtures::group_commit::INTERVAL);
    let exec = seq
        .iter()
        .find_map(|m| if let Msg::Exec(r) = m { Some(Msg::params(r)) } else { None })
        .unwrap();
    let f = plan_write(&exec).unwrap();
    let sink = TableSink::new(&f, vec![f.instance_id.clone()]).unwrap();
    sink.push(0, f.values_rows().unwrap(), &f.input_nullable()).unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let (o, checked) = sink.check(&tmp.path().join("error_log"));
    assert_eq!(o.bytes, 25);
    assert_eq!(checked.bytes(), 29);
}
