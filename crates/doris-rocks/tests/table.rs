use std::collections::BTreeSet;
use std::fs::{self, File};
use std::path::{Path, PathBuf};

use doris_rocks::table::{read_properties, read_table, InternalKey, TableBuilder, TableProps, ValueKind};
use doris_rocks::RocksError;

fn fixture(dir: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures").join(dir)
}

fn tables(dir: &Path) -> Vec<PathBuf> {
    let mut out: Vec<_> =
        fs::read_dir(dir).unwrap().flatten().map(|e| e.path()).filter(|p| p.extension().is_some_and(|e| e == "sst")).collect();
    out.sort();
    out
}

const PREFIXES: [&str; 10] =
    ["tabletmeta_", "rst_", "ppi_", "dlb_", "binlog_meta_", "binlog_data_", "pui_", "gc_", "tgc_", "hdr_"];

#[test]
fn reads_the_cpp_tables() {
    let files = tables(&fixture("cpp-compacted").join("meta"));
    assert!(files.len() >= 2);
    let mut snappy = false;
    for f in files {
        let t = read_table(&f).unwrap();
        snappy |= t.compressed_blocks > 0;
        assert!(!t.entries.is_empty(), "{}", f.display());
        assert!(t.entries.windows(2).all(|w| w[0].0 < w[1].0), "{} is out of order", f.display());
        for (k, _) in &t.entries {
            let key = String::from_utf8_lossy(&k.user_key);
            assert!(PREFIXES.iter().any(|p| key.starts_with(p)), "unexpected key {key}");
        }
    }
    assert!(snappy, "no Snappy block in the fixture tables");
}

#[test]
fn reads_the_cpp_properties() {
    let f = &tables(&fixture("cpp-compacted").join("meta"))[0];
    let props = read_properties(f).unwrap();
    assert_eq!(props.get("rocksdb.comparator").map(Vec::as_slice), Some(&b"leveldb.BytewiseComparator"[..]));
    assert_eq!(props.get("rocksdb.prefix.extractor.name").map(Vec::as_slice), Some(&b"rocksdb.FixedPrefix.4"[..]));
    assert_eq!(props.get("rocksdb.column.family.name").map(Vec::as_slice), Some(&b"meta"[..]));
}

fn key(n: usize, sequence: u64, kind: ValueKind) -> InternalKey {
    InternalKey { user_key: format!("tabletmeta_{n:08}_1234").into_bytes(), sequence, kind }
}

fn props() -> TableProps {
    TableProps { column_family_id: 2, column_family_name: "meta".into(), fixed_prefix_len: Some(4) }
}

#[test]
fn round_trips_a_table() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("000007.sst");
    let mut entries = Vec::new();
    for n in 0..5000 {
        entries.push((key(n, 20_000 - n as u64, ValueKind::Value), vec![(n % 251) as u8; 2048]));
        if n == 17 {
            entries.push((key(n, 5, ValueKind::Deletion), Vec::new()));
        }
    }
    entries.sort_by(|a, b| a.0.cmp(&b.0));
    let mut b = TableBuilder::new(File::create(&path).unwrap(), props());
    for (k, v) in &entries {
        b.add(k, v).unwrap();
    }
    let meta = b.finish().unwrap();
    assert_eq!(meta.size, fs::metadata(&path).unwrap().len());
    assert_eq!(meta.smallest, entries[0].0);
    assert_eq!(meta.largest, entries.last().unwrap().0);
    assert_eq!((meta.smallest_seqno, meta.largest_seqno), (5, 20_000));
    assert_eq!(read_table(&path).unwrap().entries, entries);
}

#[test]
fn written_properties_match_the_cpp_ones() {
    let cpp: BTreeSet<String> =
        read_properties(&tables(&fixture("cpp-compacted").join("meta"))[0]).unwrap().into_keys().collect();
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("000007.sst");
    let mut b = TableBuilder::new(File::create(&path).unwrap(), props());
    b.add(&key(1, 1, ValueKind::Value), b"v").unwrap();
    b.finish().unwrap();
    let ours: BTreeSet<String> = read_properties(&path).unwrap().into_keys().collect();
    assert_eq!(ours, cpp);
}

#[test]
fn a_flipped_byte_is_corruption() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("000007.sst");
    let mut b = TableBuilder::new(File::create(&path).unwrap(), props());
    b.add(&key(1, 1, ValueKind::Value), b"value").unwrap();
    b.finish().unwrap();
    let mut bytes = fs::read(&path).unwrap();
    bytes[3] ^= 0xff;
    fs::write(&path, bytes).unwrap();
    match read_table(&path) {
        Err(RocksError::Corrupt { path: p, .. }) => assert_eq!(p, path),
        other => panic!("{other:?}"),
    }
}
