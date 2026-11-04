use std::collections::BTreeSet;
use std::path::Path;

use doris_fixtures::Fixtures;
use doris_proto::doris::TabletMetaPb;
use doris_storage::{DataDir, StorageError, TabletManager};
use doris_thrift::gen::agent_service::TDropTabletReq;
use doris_thrift::gen::types::TTaskType;

fn manager(root: &Path) -> TabletManager {
    TabletManager::open(vec![DataDir::open(root, "127.0.0.1").unwrap()]).unwrap()
}

fn stored_header(mgr: &TabletManager, tablet_id: i64) -> TabletMetaPb {
    mgr.data_dirs()[0].headers().load_all().unwrap().into_iter().find(|m| m.tablet_id() == tablet_id).expect("stored header")
}

/// Overwrites the fields that depend on time, randomness or creation order with the expected
/// values; every other field must match the reference BE's.
fn mask(got: &mut TabletMetaPb, expected: &TabletMetaPb) {
    // TabletMeta ctor: creation_time = time(nullptr) (tablet_meta.cpp:174)
    got.creation_time = expected.creation_time;
    // TabletUid::gen_uid() is random (tablet_manager.cpp, _create_tablet_meta_unlocked)
    got.tablet_uid = expected.tablet_uid;
    // DataDir::get_shard(): round robin in the order tablets are created (data_dir.h:84)
    got.shard_id = expected.shard_id;
    assert_eq!(got.rs_metas.len(), expected.rs_metas.len(), "rowset count");
    for (g, e) in got.rs_metas.iter_mut().zip(&expected.rs_metas) {
        // RowsetId v2 comes from the BE's random uid and a counter (RowsetIdGenerator)
        g.rowset_id_v2 = e.rowset_id_v2.clone();
        // the initial rowset's times are time(nullptr) at creation (Tablet::create_initial_rowset)
        g.creation_time = e.creation_time;
        g.newest_write_timestamp = e.newest_write_timestamp;
        g.tablet_uid = e.tablet_uid;
    }
}

#[test]
fn created_headers_match_the_reference() {
    let fx = Fixtures::m2();
    let tmp = tempfile::tempdir().unwrap();
    let mut mgr = manager(tmp.path());
    let with_headers = fx.header_tablets();
    assert!(with_headers.len() >= 7);
    for req in fx.create_requests() {
        mgr.create_tablet(&req).unwrap();
        let Some(json) = fx.header_json(req.tablet_id) else { continue };
        let expected: TabletMetaPb = serde_json::from_str(&json).unwrap();
        let mut got = stored_header(&mgr, req.tablet_id);
        mask(&mut got, &expected);
        assert_eq!(got, expected, "tablet {}", req.tablet_id);
    }
}

#[test]
fn finish_info_matches_the_reference() {
    let fx = Fixtures::m2();
    let finishes = fx.finish_requests();
    let tmp = tempfile::tempdir().unwrap();
    let mut mgr = manager(tmp.path());
    for req in fx.create_requests() {
        let got = mgr.create_tablet(&req).unwrap();
        let finish = finishes
            .iter()
            .filter(|f| f.task_type == TTaskType::CREATE)
            .find(|f| f.finish_tablet_infos.iter().flatten().any(|i| i.tablet_id == req.tablet_id))
            .expect("reference finishTask");
        let mut expected = finish.finish_tablet_infos.as_ref().unwrap()[0].clone();
        // path_hash is a deployment fact: hash_of_path(host, storage root)
        expected.path_hash = Some(mgr.data_dirs()[0].path_hash());
        assert_eq!(got, expected);
    }
}

#[test]
fn tablet_dir_follows_the_layout() {
    let fx = Fixtures::m2();
    let tmp = tempfile::tempdir().unwrap();
    let mut mgr = manager(tmp.path());
    for req in fx.create_requests() {
        mgr.create_tablet(&req).unwrap();
    }
    // (tablet_id, schema_hash) of every data/<shard>/<tablet>/<schema_hash> in the reference
    let reference: BTreeSet<(String, String)> = fx
        .layout()
        .0
        .iter()
        .filter_map(|l| l.strip_prefix("$ROOT/data/"))
        .filter_map(|rest| match rest.split('/').collect::<Vec<_>>()[..] {
            [_, t, h] => Some((t.to_string(), h.to_string())),
            _ => None,
        })
        .collect();
    assert!(reference.len() >= 20);
    for (t, h) in &reference {
        let found = (0..1024).any(|s| tmp.path().join(format!("data/{s}/{t}/{h}")).is_dir());
        assert!(found, "data/<shard>/{t}/{h}");
    }
}

#[test]
fn create_is_idempotent() {
    let fx = Fixtures::m2();
    let req = &fx.create_requests()[0];
    let tmp = tempfile::tempdir().unwrap();
    let mut mgr = manager(tmp.path());
    let first = mgr.create_tablet(req).unwrap();
    let second = mgr.create_tablet(req).unwrap();
    assert_eq!(first, second);
    assert_eq!(mgr.tablet_infos().len(), 1);
    assert_eq!(mgr.data_dirs()[0].headers().load_all().unwrap().len(), 1);
}

#[test]
fn dropping_an_unknown_tablet_is_not_found() {
    let tmp = tempfile::tempdir().unwrap();
    let mut mgr = manager(tmp.path());
    let req = TDropTabletReq { tablet_id: 42, schema_hash: Some(1), replica_id: None, is_drop_table_or_partition: None };
    let err = mgr.drop_tablet(&req).unwrap_err();
    assert!(matches!(err, StorageError::TabletNotFound { tablet_id: 42 }), "{err}");
    // drop_tablet_callback: Status::NotFound("could not find tablet {}")
    assert_eq!(err.to_string(), "could not find tablet 42");
}

#[test]
fn drop_moves_the_tablet_to_trash() {
    let fx = Fixtures::m2();
    let req = &fx.create_requests()[0];
    let tmp = tempfile::tempdir().unwrap();
    let mut mgr = manager(tmp.path());
    mgr.create_tablet(req).unwrap();
    let hash = req.tablet_schema.schema_hash;
    let drop = TDropTabletReq { tablet_id: req.tablet_id, schema_hash: Some(hash), replica_id: req.replica_id, is_drop_table_or_partition: Some(true) };
    mgr.drop_tablet(&drop).unwrap();
    assert!(mgr.tablet_infos().is_empty());
    assert!(mgr.data_dirs()[0].headers().load_all().unwrap().is_empty());
    let in_data = (0..1024).any(|s| tmp.path().join(format!("data/{s}/{}", req.tablet_id)).exists());
    assert!(!in_data);
    let label = std::fs::read_dir(tmp.path().join("trash")).unwrap().next().unwrap().unwrap().path();
    assert!(label.join(format!("{}/{hash}", req.tablet_id)).is_dir());
}

#[test]
fn tablets_survive_restart() {
    let fx = Fixtures::m2();
    let tmp = tempfile::tempdir().unwrap();
    let mut mgr = manager(tmp.path());
    for req in fx.create_requests() {
        mgr.create_tablet(&req).unwrap();
    }
    let before = mgr.tablet_infos();
    let version = mgr.report_version();
    drop(mgr);
    let mgr = manager(tmp.path());
    assert_eq!(mgr.tablet_infos(), before);
    assert!(mgr.report_version() > version);
}

#[test]
fn report_version_counts_changes() {
    let fx = Fixtures::m2();
    let tmp = tempfile::tempdir().unwrap();
    let mut mgr = manager(tmp.path());
    let start = mgr.report_version();
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() as i64;
    assert!((start / 10_000 - now).abs() <= 2, "report_version starts at unix seconds * 10000");
    mgr.create_tablet(&fx.create_requests()[0]).unwrap();
    assert_eq!(mgr.report_version(), start + 1);
}

#[test]
fn visible_versions_follow_the_partitions() {
    let fx = Fixtures::m2();
    let req = &fx.create_requests()[0];
    let tmp = tempfile::tempdir().unwrap();
    let mut mgr = manager(tmp.path());
    mgr.create_tablet(req).unwrap();
    let pid = req.partition_id.unwrap();
    // VersionWithTime starts at 0 (olap_common.h) and only moves forward
    assert_eq!(mgr.partitions_version().get(&pid), Some(&0));
    mgr.update_visible_version(pid, 3);
    mgr.update_visible_version(pid, 2);
    mgr.update_visible_version(999, 5);
    assert_eq!(mgr.partitions_version().get(&pid), Some(&3));
    assert_eq!(mgr.partitions_version().get(&999), None);
}
