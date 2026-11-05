//! Transactions: commit writes a pending rowset key, publish makes it visible at a version,
//! clear discards it; committed transactions survive a restart.
mod common;

use common::{headers, loaded_root, manager};
use doris_proto::doris::{PUniqueId, RowsetMetaPb, TabletMetaPb};
use doris_segment::writer::Datum;
use doris_storage::rowset::write_rowset;
use doris_storage::{StorageError, TabletManager};

/// m4.buckets: `k INT NOT NULL, v VARCHAR(16)`, 8 tablets in one partition.
fn buckets() -> Vec<TabletMetaPb> {
    headers()
        .into_iter()
        .filter(|h| {
            let cols: Vec<_> = h.schema.as_ref().unwrap().column.iter().filter_map(|c| c.name.clone()).collect();
            cols == ["k", "v"]
        })
        .collect()
}

fn rows(n: i128) -> Vec<Vec<Datum>> {
    (0..n).map(|i| vec![Datum::Int(100 + i), Datum::Bytes(format!("t{i}").into_bytes())]).collect()
}

const LOAD: PUniqueId = PUniqueId { hi: 5, lo: 6 };

/// Writes and commits `n` rows into `tablet` for `txn`; returns the rowset.
fn commit(mgr: &mut TabletManager, tablet: i64, txn: i64, n: i128) -> RowsetMetaPb {
    let meta = mgr.tablet(tablet).unwrap().clone();
    let id = mgr.next_rowset_id();
    let dir = mgr.tablet_path(tablet).unwrap();
    let rs = write_rowset(&dir, &meta, &id, txn, &LOAD, rows(n)).unwrap().meta;
    mgr.commit_txn(meta.partition_id(), txn, tablet, LOAD, rs.clone()).unwrap();
    rs
}

fn info(mgr: &TabletManager, tablet: i64) -> (i64, i64) {
    let i = &mgr.tablet_infos()[&tablet].tablet_infos[0];
    (i.version, i.row_count)
}

#[test]
fn commit_then_publish_makes_rows_visible() {
    let root = loaded_root();
    let mut mgr = manager(root.path());
    let t = &buckets()[0];
    let (v0, rows0) = info(&mgr, t.tablet_id());
    commit(&mut mgr, t.tablet_id(), 900, 3);
    // committed, not yet visible
    assert_eq!(info(&mgr, t.tablet_id()), (v0, rows0));
    let res = mgr.publish_txn(t.partition_id(), 900, v0 + 1);
    assert_eq!(info(&mgr, t.tablet_id()), (v0 + 1, rows0 + 3));
    assert_eq!(res.delta_rows.get(&t.tablet_id()), Some(&3));
    assert!(res.succ.contains(&t.tablet_id()));
}

#[test]
fn committed_txns_survive_restart() {
    let root = loaded_root();
    let mut mgr = manager(root.path());
    let t = &buckets()[0];
    let (v0, rows0) = info(&mgr, t.tablet_id());
    commit(&mut mgr, t.tablet_id(), 901, 2);
    drop(mgr);
    let mut mgr = manager(root.path());
    let res = mgr.publish_txn(t.partition_id(), 901, v0 + 1);
    assert_eq!(res.delta_rows.get(&t.tablet_id()), Some(&2));
    assert_eq!(info(&mgr, t.tablet_id()), (v0 + 1, rows0 + 2));
}

#[test]
fn a_second_commit_of_another_rowset_conflicts() {
    let root = loaded_root();
    let mut mgr = manager(root.path());
    let t = &buckets()[0];
    let rs = commit(&mut mgr, t.tablet_id(), 902, 1);
    // the same rowset again: OK (TxnManager::commit_txn is idempotent)
    mgr.commit_txn(t.partition_id(), 902, t.tablet_id(), LOAD, rs.clone()).unwrap();
    let mut other = rs;
    other.rowset_id_v2 = Some(mgr.next_rowset_id());
    let err = mgr.commit_txn(t.partition_id(), 902, t.tablet_id(), LOAD, other).unwrap_err();
    assert!(matches!(err, StorageError::TxnConflict { .. }), "{err:?}");
}

#[test]
fn publish_is_idempotent() {
    let root = loaded_root();
    let mut mgr = manager(root.path());
    let t = &buckets()[0];
    let (v0, _) = info(&mgr, t.tablet_id());
    commit(&mut mgr, t.tablet_id(), 903, 4);
    let first = mgr.publish_txn(t.partition_id(), 903, v0 + 1);
    let header = |m: &TabletManager| m.data_dirs()[0].meta().get_tablet_meta(t.tablet_id(), t.schema_hash()).unwrap().unwrap();
    let saved = header(&mgr);
    let again = mgr.publish_txn(t.partition_id(), 903, v0 + 1);
    // the same tablets succeed (and the 7 untouched ones lack the version both times); the
    // rows were already counted
    assert_eq!((&again.succ, &again.errors), (&first.succ, &first.errors));
    assert!(again.delta_rows.is_empty());
    assert_eq!(header(&mgr), saved);
}

#[test]
fn every_tablet_of_the_partition_reports_success() {
    let root = loaded_root();
    let mut mgr = manager(root.path());
    let tablets = buckets();
    let (v0, _) = info(&mgr, tablets[0].tablet_id());
    // rows in one tablet, empty rowsets in the others, as a sink commits them
    for (i, t) in tablets.iter().enumerate() {
        commit(&mut mgr, t.tablet_id(), 904, if i == 0 { 2 } else { 0 });
    }
    let res = mgr.publish_txn(tablets[0].partition_id(), 904, v0 + 1);
    let mut succ = res.succ.clone();
    succ.sort_unstable();
    let mut all: Vec<i64> = tablets.iter().map(|t| t.tablet_id()).collect();
    all.sort_unstable();
    assert_eq!(succ, all);
    // delta rows only where rows were loaded
    assert_eq!(res.delta_rows.len(), 1);
}

#[test]
fn clear_removes_pending_rowsets() {
    let root = loaded_root();
    let mut mgr = manager(root.path());
    let t = &buckets()[0];
    let (v0, _) = info(&mgr, t.tablet_id());
    let rs = commit(&mut mgr, t.tablet_id(), 905, 2);
    let file = mgr.tablet_path(t.tablet_id()).unwrap().join(format!("{}_0.dat", rs.rowset_id_v2()));
    assert!(file.is_file());
    mgr.clear_txn(905, &[]).unwrap();
    assert!(!file.exists());
    assert!(!mgr.data_dirs()[0].meta().rowset_metas().unwrap().iter().any(|r| r.rowset_id_v2 == rs.rowset_id_v2));
    // the txn is gone: a publish finds no rowset for the tablet
    let res = mgr.publish_txn(t.partition_id(), 905, v0 + 1);
    assert!(res.errors.contains(&t.tablet_id()), "{res:?}");
}

#[test]
fn a_publish_without_rowsets_reports_the_tablets_as_errors() {
    // EnginePublishVersionTask: a tablet without the version is an error tablet, not a failure
    let root = loaded_root();
    let mut mgr = manager(root.path());
    let t = &buckets()[0];
    let (v0, _) = info(&mgr, t.tablet_id());
    let res = mgr.publish_txn(t.partition_id(), 906, v0 + 1);
    assert!(res.succ.is_empty());
    assert_eq!(res.errors.len(), buckets().len());
}
