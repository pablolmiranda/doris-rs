//! The tablet reader: the C++ BE's loaded tablets read back as the C++ BE answers queries over
//! them, the version path, and the reads doris-rs refuses rather than answer wrongly.
mod common;

use common::{fixture, headers, loaded_root, manager};

use doris_exec::row_text::cell_text;
use doris_exec::value::ColType;
use doris_proto::doris::{DeletePredicatePb, KeysType, RowsetMetaPb, RowsetStatePb, TabletMetaPb};
use doris_segment::pb::segment_v2::CompressionTypePb;
use doris_segment::SegmentFile;
use doris_storage::reader::{scan_columns, scan_tablet, version_path, ScanColumn};
use doris_storage::{StorageError, TabletManager};

/// The fixture's tables, by their column names, with the reply that lists all their rows.
const TABLES: &[(&str, &str)] = &[
    ("k1,k2,k3,c_bigint,c_largeint,c_bool,c_float,c_double,c_d32,c_d64,c_d128,c_date,c_dt,c_char,c_varchar,c_string", "all_types_all"),
    ("k,v", "buckets_all"),
    ("dt,k,v", "ranged_all"),
    ("k,v,s,d", "zstd_all"),
];

fn names(h: &TabletMetaPb) -> String {
    h.schema
        .as_ref()
        .unwrap()
        .column
        .iter()
        .map(|c| c.name().to_string())
        .collect::<Vec<_>>()
        .join(",")
}

fn visible_version(h: &TabletMetaPb) -> i64 {
    h.rs_metas.iter().map(|r| r.end_version()).max().unwrap()
}

/// Every row of a table's tablets, as the mysql client's TSV lines, sorted.
fn table_rows(mgr: &TabletManager, columns: &str) -> Vec<String> {
    let mut lines = Vec::new();
    for h in headers().iter().filter(|h| names(h) == columns) {
        let cols = scan_columns(h.schema.as_ref().unwrap());
        let types: Vec<ColType> = cols
            .iter()
            .map(|c| ColType {
                name: String::new(),
                ptype: c.ty.ptype,
                len: -1,
                precision: c.ty.precision,
                scale: c.ty.scale,
                nullable: true,
            })
            .collect();
        for segment in scan_tablet(mgr, h.tablet_id(), visible_version(h), &cols).unwrap() {
            for row in segment {
                let cells: Vec<String> = row
                    .iter()
                    .zip(&types)
                    .map(|(d, t)| {
                        cell_text(d, t)
                            .map_or("NULL".to_string(), |b| String::from_utf8(b).unwrap())
                    })
                    .collect();
                lines.push(cells.join("\t"));
            }
        }
    }
    lines.sort();
    lines
}

fn reply_rows(name: &str) -> Vec<String> {
    let text =
        std::fs::read_to_string(fixture().join("replies").join(format!("{name}.tsv"))).unwrap();
    let mut lines: Vec<String> = text.lines().skip(1).map(str::to_string).collect();
    lines.sort();
    lines
}

#[test]
fn cpp_loaded_tables_read_back() {
    let root = loaded_root();
    let mgr = manager(root.path());
    for (columns, reply) in TABLES.iter().filter(|(_, r)| *r != "zstd_all") {
        let expected = reply_rows(reply);
        assert!(!expected.is_empty(), "{reply}");
        assert_eq!(table_rows(&mgr, columns), expected, "{reply}");
    }
}

#[test]
fn zstd_pages_read_back() {
    let root = loaded_root();
    let mgr = manager(root.path());
    let (columns, reply) = TABLES[3];
    let expected = reply_rows(reply);
    assert_eq!(expected.len(), 20000);
    assert_eq!(table_rows(&mgr, columns), expected);
    // the columns are ZSTD-compressed: the reader decoded compressed pages
    let h = headers().into_iter().find(|h| names(h) == columns).unwrap();
    let dir = mgr.tablet_path(h.tablet_id()).unwrap();
    let rs = h.rs_metas.iter().find(|r| r.num_segments() > 0).unwrap();
    let seg = SegmentFile::open(&dir.join(format!("{}_0.dat", rs.rowset_id_v2()))).unwrap();
    assert!(
        seg.footer
            .columns
            .iter()
            .any(|c| c.compression == Some(CompressionTypePb::Zstd as i32)),
        "no ZSTD column"
    );
}

fn rowset(start: i64, end: i64, state: RowsetStatePb) -> RowsetMetaPb {
    let mut r = RowsetMetaPb {
        start_version: Some(start),
        end_version: Some(end),
        ..Default::default()
    };
    r.set_rowset_state(state);
    r
}

#[test]
fn version_path_picks_the_widest() {
    let v = RowsetStatePb::Visible;
    let rs = vec![
        rowset(0, 1, v),
        rowset(2, 2, v),
        rowset(3, 3, v),
        rowset(2, 3, v),
    ];
    let path: Vec<(i64, i64)> = version_path(&rs, 3)
        .unwrap()
        .iter()
        .map(|r| (r.start_version(), r.end_version()))
        .collect();
    assert_eq!(path, vec![(0, 1), (2, 3)]);
    let path: Vec<(i64, i64)> = version_path(&rs, 2)
        .unwrap()
        .iter()
        .map(|r| (r.start_version(), r.end_version()))
        .collect();
    assert_eq!(path, vec![(0, 1), (2, 2)]);
}

/// A DUP tablet of the fixture with rows.
fn dup_tablet() -> TabletMetaPb {
    headers()
        .into_iter()
        .find(|h| names(h) == TABLES[0].0 && h.rs_metas.iter().any(|r| r.num_rows() > 0))
        .unwrap()
}

#[test]
fn a_version_past_the_tablet_fails() {
    let root = loaded_root();
    let mgr = manager(root.path());
    let h = dup_tablet();
    let cols = scan_columns(h.schema.as_ref().unwrap());
    let v = visible_version(&h) + 1;
    match scan_tablet(&mgr, h.tablet_id(), v, &cols) {
        Err(e @ StorageError::MissedVersion { .. }) => {
            assert!(
                e.to_string().contains(&format!(
                    "fail to find path in version_graph. spec_version: 0-{v}"
                )),
                "{e}"
            )
        }
        other => panic!("expected MissedVersion, got {other:?}"),
    }
}

fn unique_tablet() -> TabletMetaPb {
    headers()
        .into_iter()
        .find(|h| h.schema.as_ref().unwrap().keys_type() == KeysType::UniqueKeys)
        .expect("a unique-key tablet in the fixture (the FE's statistics tables)")
}

#[test]
fn unique_key_without_rows_reads_empty() {
    let root = loaded_root();
    let mgr = manager(root.path());
    let h = unique_tablet();
    assert!(h.rs_metas.iter().all(|r| r.num_rows() == 0));
    let cols = scan_columns(h.schema.as_ref().unwrap());
    let segments = scan_tablet(&mgr, h.tablet_id(), visible_version(&h), &cols).unwrap();
    assert!(segments.iter().all(|s| s.is_empty()));
}

#[test]
fn unique_key_with_rows_is_refused() {
    let root = loaded_root();
    let mut mgr = manager(root.path());
    let h = unique_tablet();
    let v = visible_version(&h) + 1;
    let mut r = rowset(v, v, RowsetStatePb::Visible);
    r.rowset_id_v2 = Some(mgr.next_rowset_id());
    r.num_rows = Some(1);
    mgr.add_visible_rowset(h.tablet_id(), r).unwrap();
    let cols = scan_columns(h.schema.as_ref().unwrap());
    match scan_tablet(&mgr, h.tablet_id(), v, &cols) {
        Err(StorageError::UnsupportedRead { tablet_id, what }) => {
            assert_eq!(tablet_id, h.tablet_id());
            assert!(what.contains("UNIQUE_KEYS"), "{what}");
        }
        other => panic!("expected UnsupportedRead, got {other:?}"),
    }
}

#[test]
fn delete_predicate_is_refused() {
    let root = loaded_root();
    let mut mgr = manager(root.path());
    let h = dup_tablet();
    let v = visible_version(&h) + 1;
    let mut r = rowset(v, v, RowsetStatePb::Visible);
    r.rowset_id_v2 = Some(mgr.next_rowset_id());
    r.delete_predicate = Some(DeletePredicatePb {
        version: v as i32,
        sub_predicates: vec!["k1=1".into()],
        ..Default::default()
    });
    mgr.add_visible_rowset(h.tablet_id(), r).unwrap();
    let cols = scan_columns(h.schema.as_ref().unwrap());
    match scan_tablet(&mgr, h.tablet_id(), v, &cols) {
        Err(StorageError::UnsupportedRead { what, .. }) => {
            assert!(what.contains("delete"), "{what}")
        }
        other => panic!("expected UnsupportedRead, got {other:?}"),
    }
    // below the delete, the tablet still reads
    assert!(scan_tablet(&mgr, h.tablet_id(), v - 1, &cols).is_ok());
}

#[test]
fn a_column_missing_from_the_segment_is_refused() {
    let root = loaded_root();
    let mgr = manager(root.path());
    let h = dup_tablet();
    let mut cols = scan_columns(h.schema.as_ref().unwrap());
    cols.push(ScanColumn {
        unique_id: 999,
        ty: cols[0].ty.clone(),
    });
    match scan_tablet(&mgr, h.tablet_id(), visible_version(&h), &cols) {
        Err(StorageError::UnsupportedRead { what, .. }) => assert!(what.contains("999"), "{what}"),
        other => panic!("expected UnsupportedRead, got {other:?}"),
    }
}
