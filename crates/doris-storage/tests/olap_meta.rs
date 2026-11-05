use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use doris_proto::doris::{TabletMetaPb, TabletStatePb};
use doris_rocks::db::{Batch, CfOptions, Db, DbOptions};
use doris_storage::{DataDir, StorageError, TabletManager, TabletOptions};
use prost::Message;

fn fixture(dir: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../doris-rocks/tests/fixtures").join(dir)
}

fn copy_dir(from: &Path, to: &Path) {
    fs::create_dir_all(to).unwrap();
    for e in fs::read_dir(from).unwrap().flatten() {
        let p = e.path();
        if p.is_dir() {
            copy_dir(&p, &to.join(e.file_name()));
        } else {
            fs::copy(&p, to.join(e.file_name())).unwrap();
        }
    }
}

/// The reference headers of a fixture, decoded (`GET /api/meta/header`, as M2's header test).
fn headers(dir: &str) -> Vec<TabletMetaPb> {
    let mut out: Vec<TabletMetaPb> = fs::read_dir(fixture(dir).join("headers"))
        .unwrap()
        .flatten()
        .map(|e| serde_json::from_str(&fs::read_to_string(e.path()).unwrap()).unwrap())
        .collect();
    out.sort_by_key(|m| m.tablet_id());
    out
}

/// A storage root holding a fixture's `meta/` and `stream_load/`, with a directory for every
/// live tablet, as the C++ BE left it.
fn cpp_root(dir: &str, root: &Path) {
    copy_dir(&fixture(dir).join("meta"), &root.join("meta"));
    copy_dir(&fixture(dir).join("stream_load"), &root.join("stream_load"));
    for h in headers(dir) {
        fs::create_dir_all(root.join(format!("data/{}/{}/{}", h.shard_id(), h.tablet_id(), h.schema_hash()))).unwrap();
    }
}

fn open(root: &Path) -> Result<TabletManager, StorageError> {
    TabletManager::open(vec![DataDir::open(root, "127.0.0.1")?], TabletOptions::default())
}

fn meta_db(root: &Path) -> Db {
    let cf = |name: &str, prefix| CfOptions { name: name.into(), fixed_prefix_len: prefix };
    let opts = DbOptions { column_families: vec![cf("default", None), cf("doris", None), cf("meta", Some(4))], ..DbOptions::default() };
    Db::open(&root.join("meta"), &opts).unwrap()
}

/// Every file under `root` with its size.
fn listing(root: &Path) -> Vec<(PathBuf, u64)> {
    let mut out = Vec::new();
    for e in fs::read_dir(root).unwrap().flatten() {
        let p = e.path();
        if p.is_dir() {
            out.push((p.clone(), 0));
            out.extend(listing(&p));
        } else {
            out.push((p.clone(), e.metadata().unwrap().len()));
        }
    }
    out.sort();
    out
}

fn running(metas: Vec<TabletMetaPb>) -> Vec<TabletMetaPb> {
    let mut out: Vec<_> = metas.into_iter().filter(|m| m.tablet_state() == TabletStatePb::PbRunning).collect();
    out.sort_by_key(|m| m.tablet_id());
    out
}

#[test]
fn loads_the_cpp_headers() {
    for dir in ["cpp-wal", "cpp-compacted"] {
        let tmp = tempfile::tempdir().unwrap();
        cpp_root(dir, tmp.path());
        let mgr = open(tmp.path()).unwrap();
        // the last round's dropped tablets were saved before their DROP reached the BE
        let dropped = fs::read_to_string(fixture(dir).join("dropped.txt")).unwrap_or_default();
        let want: Vec<_> = headers(dir)
            .into_iter()
            .filter(|m| !dropped.contains(&format!("tabletmeta_{}_", m.tablet_id())))
            .collect();
        assert_eq!(running(mgr.data_dirs()[0].meta().tablet_metas().unwrap()), want, "{dir}");
        let ids: BTreeSet<i64> = mgr.tablet_infos().keys().copied().collect();
        assert_eq!(ids, want.iter().map(|m| m.tablet_id()).collect(), "{dir}");
        // SHUTDOWN headers wait for the sweep instead
        let shut: Vec<i64> = dropped.lines().map(|k| k.split('_').nth(1).unwrap().parse().unwrap()).collect();
        assert_eq!(mgr.shutdown_tablets(), shut, "{dir}");
    }
}

#[test]
fn headers_land_in_meta() {
    let fx = doris_fixtures::Fixtures::m2();
    let tmp = tempfile::tempdir().unwrap();
    let mut mgr = open(tmp.path()).unwrap();
    let reqs: Vec<_> = fx.create_requests().into_iter().take(4).collect();
    for req in &reqs {
        mgr.create_tablet(req).unwrap();
    }
    drop(mgr);
    let db = meta_db(tmp.path());
    for req in &reqs {
        let key = format!("tabletmeta_{}_{}", req.tablet_id, req.tablet_schema.schema_hash);
        let meta = TabletMetaPb::decode(db.get("meta", key.as_bytes()).expect("header in meta/")).unwrap();
        assert_eq!(meta.tablet_id(), req.tablet_id);
    }
    assert!(!tmp.path().join("doris-rs-meta").exists());
}

#[test]
fn moves_the_temporary_store() {
    let tmp = tempfile::tempdir().unwrap();
    let h = &headers("cpp-wal")[0];
    let old = tmp.path().join("doris-rs-meta");
    fs::create_dir_all(&old).unwrap();
    fs::write(old.join(format!("tabletmeta_{}_{}", h.tablet_id(), h.schema_hash())), h.encode_to_vec()).unwrap();
    fs::create_dir_all(tmp.path().join(format!("data/{}/{}/{}", h.shard_id(), h.tablet_id(), h.schema_hash()))).unwrap();
    let mgr = open(tmp.path()).unwrap();
    assert!(mgr.tablet_infos().contains_key(&h.tablet_id()));
    assert!(!old.exists());
    drop(mgr);
    let key = format!("tabletmeta_{}_{}", h.tablet_id(), h.schema_hash());
    assert!(meta_db(tmp.path()).get("meta", key.as_bytes()).is_some());
}

#[test]
fn refuses_a_root_with_data() {
    // a header whose rowset has a segment
    let tmp = tempfile::tempdir().unwrap();
    cpp_root("cpp-wal", tmp.path());
    let mut h = headers("cpp-wal")[3].clone();
    h.rs_metas[0].num_segments = Some(1);
    {
        let mut db = meta_db(tmp.path());
        let mut b = Batch::default();
        b.put("meta", format!("tabletmeta_{}_{}", h.tablet_id(), h.schema_hash()).as_bytes(), &h.encode_to_vec());
        db.write(b, true).unwrap();
        db.close().unwrap();
    }
    let before = listing(tmp.path());
    match open(tmp.path()) {
        Err(StorageError::DataRoot { root, tablet_id, .. }) => {
            assert_eq!(root, tmp.path());
            assert_eq!(tablet_id, Some(h.tablet_id()));
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(listing(tmp.path()), before);

    // a committed rowset's key
    let tmp = tempfile::tempdir().unwrap();
    cpp_root("cpp-wal", tmp.path());
    {
        let mut db = meta_db(tmp.path());
        let mut b = Batch::default();
        b.put("meta", b"rst_0000000000000001-0000000000000002_0200000000000001", b"x");
        db.write(b, true).unwrap();
        db.close().unwrap();
    }
    let before = listing(tmp.path());
    let err = open(tmp.path()).unwrap_err();
    assert!(matches!(&err, StorageError::DataRoot { key: Some(k), .. } if k.starts_with("rst_")), "{err:?}");
    assert!(err.to_string().contains("milestone 4"), "{err}");
    assert_eq!(listing(tmp.path()), before);
}

#[test]
fn refuses_a_legacy_header() {
    let tmp = tempfile::tempdir().unwrap();
    {
        let mut db = meta_db(tmp.path());
        let mut b = Batch::default();
        b.put("meta", b"hdr_1_1", b"x");
        db.write(b, true).unwrap();
        db.close().unwrap();
    }
    assert!(matches!(open(tmp.path()), Err(StorageError::LegacyHeader { .. })));
}

#[test]
fn skips_a_header_under_the_wrong_key() {
    // DataDir::load fails the tablet and, with ignore_load_tablet_failure (default true), goes on
    let tmp = tempfile::tempdir().unwrap();
    cpp_root("cpp-wal", tmp.path());
    let h = headers("cpp-wal")[0].clone();
    let wrong = format!("tabletmeta_{}_{}", h.tablet_id() + 1, h.schema_hash());
    {
        let mut db = meta_db(tmp.path());
        let mut b = Batch::default();
        b.delete("meta", format!("tabletmeta_{}_{}", h.tablet_id(), h.schema_hash()).as_bytes());
        b.put("meta", wrong.as_bytes(), &h.encode_to_vec());
        db.write(b, true).unwrap();
        db.close().unwrap();
    }
    let mgr = open(tmp.path()).unwrap();
    assert!(!mgr.tablet_infos().contains_key(&h.tablet_id()));
    assert_eq!(mgr.tablet_infos().len(), headers("cpp-wal").len() - 1);
    assert!(mgr.data_dirs()[0].skipped_headers().iter().any(|s| s.contains(&wrong)));
}

#[test]
fn a_rowset_without_its_schema_is_saved_again() {
    let tmp = tempfile::tempdir().unwrap();
    cpp_root("cpp-wal", tmp.path());
    let mut h = headers("cpp-wal")[0].clone();
    let key = format!("tabletmeta_{}_{}", h.tablet_id(), h.schema_hash());
    h.rs_metas[0].tablet_schema = None;
    {
        let mut db = meta_db(tmp.path());
        let mut b = Batch::default();
        b.put("meta", key.as_bytes(), &h.encode_to_vec());
        db.write(b, true).unwrap();
        db.close().unwrap();
    }
    drop(open(tmp.path()).unwrap());
    let stored = TabletMetaPb::decode(meta_db(tmp.path()).get("meta", key.as_bytes()).unwrap()).unwrap();
    assert_eq!(stored.rs_metas[0].tablet_schema, h.schema);
}

#[test]
fn creates_stream_load_on_the_first_root_only() {
    let (a, b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let dirs = vec![DataDir::open(a.path(), "127.0.0.1").unwrap(), DataDir::open(b.path(), "127.0.0.1").unwrap()];
    let _mgr = TabletManager::open(dirs, TabletOptions::default()).unwrap();
    assert!(a.path().join("stream_load/CURRENT").is_file());
    assert!(!b.path().join("stream_load/CURRENT").exists());
}
