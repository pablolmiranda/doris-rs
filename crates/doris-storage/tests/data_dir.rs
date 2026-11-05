use std::collections::BTreeSet;
use std::fs;
use std::path::Path;

use doris_proto::doris::{PUniqueId, TabletMetaPb};
use doris_storage::{hash_of_path, DataDir};

const LAYOUT: &str = "../doris-be/tests/fixtures/m2/layout.txt";

/// The smallest header the C++ BE loads: ids, shard, and a nonzero tablet_uid.
fn meta(tablet_id: i64, schema_hash: i32, shard: i32) -> TabletMetaPb {
    TabletMetaPb {
        tablet_id: Some(tablet_id),
        schema_hash: Some(schema_hash),
        shard_id: Some(shard),
        tablet_uid: Some(PUniqueId { hi: 1, lo: 2 }),
        ..Default::default()
    }
}

fn top_level(root: &Path) -> BTreeSet<String> {
    fs::read_dir(root).unwrap().map(|e| e.unwrap().file_name().into_string().unwrap()).collect()
}

/// Top-level entries of the reference BE's storage root, from the fixture layout (before the drop).
fn reference_top_level() -> BTreeSet<String> {
    let text = fs::read_to_string(LAYOUT).expect("fixture layout.txt");
    text.lines()
        .take_while(|l| !l.starts_with("# after drop"))
        .filter_map(|l| l.strip_prefix("$ROOT/"))
        .filter(|rest| !rest.contains('/'))
        .map(str::to_string)
        .collect()
}

#[test]
fn path_hash_matches_the_reference() {
    // TDisk.path_hash in the wire corpus' disk report from the reference BE.
    assert_eq!(hash_of_path("127.0.0.1", "/opt/apache-doris/be/storage"), 8278071542442530434);
}

#[test]
fn a_new_root_gets_the_cpp_folders() {
    let tmp = tempfile::tempdir().unwrap();
    let _dir = DataDir::open(tmp.path(), "127.0.0.1").unwrap();
    let mut expected = reference_top_level();
    expected.remove("cluster_id"); // written on the first heartbeat, not at start
    let mut got = top_level(tmp.path());
    got.remove("cluster_id");
    assert_eq!(got, expected);
    assert!(tmp.path().join("meta/CURRENT").is_file());
    assert!(tmp.path().join("data/0").is_dir() && tmp.path().join("data/1023").is_dir());
}

#[test]
fn cluster_id_is_written_once() {
    let tmp = tempfile::tempdir().unwrap();
    let mut dir = DataDir::open(tmp.path(), "127.0.0.1").unwrap();
    assert_eq!(dir.cluster_id(), None);
    dir.set_cluster_id(7).unwrap();
    assert_eq!(fs::read_to_string(tmp.path().join("cluster_id")).unwrap(), "7");
    drop(dir);
    let mut dir = DataDir::open(tmp.path(), "127.0.0.1").unwrap();
    assert_eq!(dir.cluster_id(), Some(7));
    dir.set_cluster_id(7).unwrap();
    assert!(dir.set_cluster_id(8).is_err());
    assert_eq!(fs::read_to_string(tmp.path().join("cluster_id")).unwrap(), "7");
}

#[test]
fn headers_survive_reopen() {
    let tmp = tempfile::tempdir().unwrap();
    let mut dir = DataDir::open(tmp.path(), "127.0.0.1").unwrap();
    fs::create_dir_all(dir.tablet_dir(3, 10001, 55)).unwrap();
    dir.meta_mut().save_tablet_meta(&meta(10001, 55, 3), false).unwrap();
    drop(dir);
    let dir = DataDir::open(tmp.path(), "127.0.0.1").unwrap();
    assert_eq!(dir.meta().tablet_metas().unwrap(), vec![meta(10001, 55, 3)]);
}

#[test]
fn half_created_tablets_leave_no_trace() {
    let tmp = tempfile::tempdir().unwrap();
    let mut dir = DataDir::open(tmp.path(), "127.0.0.1").unwrap();
    // a header whose directory was never created
    dir.meta_mut().save_tablet_meta(&meta(20001, 1, 4), false).unwrap();
    // a directory whose header was never written
    fs::create_dir_all(dir.tablet_dir(5, 20002, 2)).unwrap();
    drop(dir);
    let dir = DataDir::open(tmp.path(), "127.0.0.1").unwrap();
    assert!(dir.meta().tablet_metas().unwrap().is_empty());
    assert!(!tmp.path().join("data/5/20002").exists());
    let trash: Vec<_> = fs::read_dir(tmp.path().join("trash")).unwrap().collect();
    assert_eq!(trash.len(), 1);
    let label = trash[0].as_ref().unwrap().path();
    assert!(label.join("20002/2").is_dir());
}

#[test]
fn shards_go_round_robin() {
    let tmp = tempfile::tempdir().unwrap();
    let mut dir = DataDir::open(tmp.path(), "127.0.0.1").unwrap();
    assert_eq!((dir.next_shard(), dir.next_shard()), (0, 1));
    assert_eq!(dir.tablet_dir(1, 7, 9), tmp.path().join("data/1/7/9"));
    assert_eq!(dir.path_hash(), hash_of_path("127.0.0.1", tmp.path().to_str().unwrap()) as i64);
}
