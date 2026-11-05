//! Rowsets: a root the C++ BE loaded opens with its rowsets, a rowset doris-rs writes carries
//! the C++ BE's fields, and published rowsets survive a restart.
mod common;

use common::{fixture, headers, loaded_root, manager};

use doris_proto::doris::{PUniqueId, RowsetMetaPb, RowsetStatePb, TabletMetaPb};
use doris_segment::column::read_column;
use doris_segment::writer::datums;
use doris_segment::SegmentFile;
use doris_storage::rowset::{segment_schema, write_rowset};

#[test]
fn cpp_loaded_root_opens_with_its_rowsets() {
    let root = loaded_root();
    let mgr = manager(root.path());
    let infos = mgr.tablet_infos();
    let headers = headers();
    assert_eq!(infos.len(), headers.len());
    let mut with_rows = 0;
    for h in &headers {
        let info = &infos[&h.tablet_id()].tablet_infos[0];
        let sum = |f: fn(&RowsetMetaPb) -> i64| h.rs_metas.iter().map(f).sum::<i64>();
        let version = h.rs_metas.iter().map(|r| r.end_version()).max().unwrap();
        assert_eq!(info.version, version, "tablet {}", h.tablet_id());
        assert_eq!(info.row_count, sum(|r| r.num_rows()), "tablet {}", h.tablet_id());
        assert_eq!(info.data_size, sum(|r| r.total_disk_size()), "tablet {}", h.tablet_id());
        assert_eq!(info.local_segment_size, Some(sum(|r| r.data_disk_size())), "tablet {}", h.tablet_id());
        assert_eq!(info.local_index_size, Some(sum(|r| r.index_disk_size())), "tablet {}", h.tablet_id());
        assert_eq!(info.total_version_count, Some(h.rs_metas.len() as i64), "tablet {}", h.tablet_id());
        with_rows += usize::from(info.row_count > 0);
    }
    assert!(with_rows >= 10, "{with_rows}");
}

/// The fixture rowset at `version` of the tablet whose schema has a column `name`.
fn fixture_rowset(column: &str, version: i64) -> (TabletMetaPb, RowsetMetaPb) {
    let h = headers()
        .into_iter()
        .find(|h| h.schema.as_ref().unwrap().column.iter().any(|c| c.name.as_deref() == Some(column)))
        .unwrap();
    let r = h.rs_metas.iter().find(|r| r.start_version() == version).unwrap().clone();
    (h, r)
}

/// Fields that depend on the writing process (ids, times, sizes, state before publish).
fn mask(got: &mut RowsetMetaPb, expected: &RowsetMetaPb) {
    got.rowset_id_v2 = expected.rowset_id_v2.clone();
    got.txn_id = expected.txn_id;
    got.load_id = expected.load_id;
    got.creation_time = expected.creation_time;
    got.newest_write_timestamp = expected.newest_write_timestamp;
    got.total_disk_size = expected.total_disk_size;
    got.data_disk_size = expected.data_disk_size;
    got.rowset_state = expected.rowset_state;
    got.start_version = expected.start_version;
    got.end_version = expected.end_version;
}

#[test]
fn written_rowset_matches_the_cpp_fields() {
    let (tablet, expected) = fixture_rowset("c_largeint", 2);
    assert!(expected.num_rows() > 0);
    let schema = segment_schema(tablet.schema.as_ref().unwrap()).unwrap();
    let path = fixture().join("data").join(tablet.shard_id().to_string()).join(tablet.tablet_id().to_string()).join(tablet.schema_hash().to_string());
    let seg = SegmentFile::open(&path.join(format!("{}_0.dat", expected.rowset_id_v2()))).unwrap();
    let cols: Vec<_> = schema.columns.iter().map(|c| datums(&read_column(&seg, c.unique_id).unwrap())).collect();
    let rows: Vec<Vec<_>> = (0..seg.footer.num_rows.unwrap() as usize).map(|i| cols.iter().map(|c| c[i].clone()).collect()).collect();

    let tmp = tempfile::tempdir().unwrap();
    let load_id = PUniqueId { hi: 1, lo: 2 };
    let written = write_rowset(tmp.path(), &tablet, "0200000000000001aaaaaaaaaaaaaaaabbbbbbbbbbbbbbbb", 7, &load_id, rows).unwrap();
    assert_eq!(written.meta.rowset_state(), RowsetStatePb::Committed);
    assert_eq!((written.meta.txn_id(), written.meta.load_id), (7, Some(load_id)));
    assert!(tmp.path().join("0200000000000001aaaaaaaaaaaaaaaabbbbbbbbbbbbbbbb_0.dat").is_file());
    assert_eq!(written.meta.data_disk_size() as u64, std::fs::metadata(tmp.path().join("0200000000000001aaaaaaaaaaaaaaaabbbbbbbbbbbbbbbb_0.dat")).unwrap().len());
    let mut got = written.meta.clone();
    mask(&mut got, &expected);
    assert_eq!(got, expected);
}

#[test]
fn an_empty_rowset_has_no_files() {
    // m4.ranged's p1 and p3 got rows in one bucket each; the other bucket's rowset is empty
    let (tablet, expected) = headers()
        .into_iter()
        .filter_map(|h| {
            let r = h.rs_metas.iter().find(|r| r.start_version() >= 2 && r.num_rows() == 0)?.clone();
            Some((h, r))
        })
        .next()
        .expect("an empty published rowset in the fixture");
    let tmp = tempfile::tempdir().unwrap();
    let written = write_rowset(tmp.path(), &tablet, "0200000000000002aaaaaaaaaaaaaaaabbbbbbbbbbbbbbbb", 7, &PUniqueId { hi: 1, lo: 2 }, Vec::new()).unwrap();
    assert_eq!(std::fs::read_dir(tmp.path()).unwrap().count(), 0);
    assert!(written.meta.empty());
    let mut got = written.meta.clone();
    mask(&mut got, &expected);
    assert_eq!(got, expected);
}

#[test]
fn visible_rowsets_survive_restart() {
    let root = loaded_root();
    let mut mgr = manager(root.path());
    let (tablet, _) = fixture_rowset("c_largeint", 2);
    let before = mgr.tablet_infos()[&tablet.tablet_id()].tablet_infos[0].clone();
    let meta = mgr.tablet(tablet.tablet_id()).unwrap().clone();
    let id = mgr.next_rowset_id();
    let dir = mgr.tablet_path(tablet.tablet_id()).unwrap();
    let rows = vec![vec![doris_segment::writer::Datum::Null; meta.schema.as_ref().unwrap().column.len()]];
    let mut rs = write_rowset(&dir, &meta, &id, 99, &PUniqueId { hi: 3, lo: 4 }, rows).unwrap().meta;
    rs.start_version = Some(before.version + 1);
    rs.end_version = Some(before.version + 1);
    rs.set_rowset_state(RowsetStatePb::Visible);
    mgr.add_visible_rowset(tablet.tablet_id(), rs).unwrap();
    drop(mgr);

    let mgr = manager(root.path());
    let after = &mgr.tablet_infos()[&tablet.tablet_id()].tablet_infos[0];
    assert_eq!(after.version, before.version + 1);
    assert_eq!(after.row_count, before.row_count + 1);
    // saved in the header, so no rowset key is left for it
    let keys: Vec<RowsetMetaPb> = mgr.data_dirs()[0].meta().rowset_metas().unwrap();
    assert!(!keys.iter().any(|r| r.rowset_id_v2() == id), "rst_ key left for {id}");
    let header = mgr.data_dirs()[0].meta().get_tablet_meta(tablet.tablet_id(), tablet.schema_hash()).unwrap().unwrap();
    assert!(header.rs_metas.iter().any(|r| r.rowset_id_v2() == id));
}
