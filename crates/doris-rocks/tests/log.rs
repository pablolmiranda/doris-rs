use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use doris_rocks::log::{read_records, LogWriter};
use doris_rocks::version_edit::{NewFile, VersionEdit};
use doris_rocks::write_batch::{BatchOp, WriteBatch};
use doris_rocks::RocksError;

fn fixture(dir: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures").join(dir)
}

fn one(dir: &Path, prefix: &str, suffix: &str) -> PathBuf {
    let mut found: Vec<_> = fs::read_dir(dir)
        .unwrap()
        .flatten()
        .map(|e| e.path())
        .filter(|p| {
            let n = p.file_name().unwrap().to_str().unwrap();
            n.starts_with(prefix) && n.ends_with(suffix)
        })
        .collect();
    found.sort();
    found.pop().unwrap_or_else(|| panic!("no {prefix}*{suffix} in {}", dir.display()))
}

fn lines(path: PathBuf) -> Vec<String> {
    fs::read_to_string(path).unwrap().lines().map(str::to_string).collect()
}

/// Column family name → id, from a manifest's edits.
fn column_families(edits: &[VersionEdit]) -> Vec<(String, u32)> {
    let mut out = vec![("default".to_string(), 0)];
    for e in edits {
        if let Some(name) = &e.column_family_add {
            out.push((name.clone(), e.column_family));
        }
    }
    out
}

#[test]
fn reads_every_record_of_the_cpp_manifest() {
    for dir in ["cpp-wal", "cpp-compacted"] {
        let meta = fixture(dir).join("meta");
        let records = read_records(&one(&meta, "MANIFEST-", "")).unwrap();
        let edits: Vec<VersionEdit> = records.iter().map(|r| VersionEdit::decode(r).unwrap()).collect();
        let cfs = column_families(&edits);
        let names: Vec<&str> = cfs.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(names, ["default", "doris", "meta"], "{dir}");
        assert!(edits.iter().any(|e| e.comparator.as_deref() == Some("leveldb.BytewiseComparator")), "{dir}");
    }
}

#[test]
fn reads_every_batch_of_the_cpp_log() {
    let meta = fixture("cpp-wal").join("meta");
    let edits: Vec<VersionEdit> =
        read_records(&one(&meta, "MANIFEST-", "")).unwrap().iter().map(|r| VersionEdit::decode(r).unwrap()).collect();
    let meta_cf = column_families(&edits).into_iter().find(|(n, _)| n == "meta").unwrap().1;
    let mut keys = std::collections::BTreeSet::new();
    for record in read_records(&one(&meta, "", ".log")).unwrap() {
        for op in WriteBatch::decode(&record).unwrap().ops {
            if let BatchOp::Put { cf, key, .. } = op {
                if cf == meta_cf {
                    keys.insert(String::from_utf8(key).unwrap());
                }
            }
        }
    }
    for k in lines(fixture("cpp-wal").join("keys.txt")) {
        assert!(keys.contains(&k), "{k} not in the log");
    }
}

fn write_records(path: &Path, records: &[Vec<u8>]) {
    let mut w = LogWriter::new(File::create(path).unwrap(), 1);
    for r in records {
        w.add_record(r).unwrap();
    }
    w.sync().unwrap();
}

#[test]
fn round_trips_records_across_blocks() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("000001.log");
    let records: Vec<Vec<u8>> =
        [10usize, 40 * 1024, 100 * 1024, 32768 - 7, 0].iter().enumerate().map(|(i, n)| vec![i as u8 + 1; *n]).collect();
    write_records(&path, &records);
    assert_eq!(read_records(&path).unwrap(), records);
}

#[test]
fn a_torn_tail_is_ignored() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("000001.log");
    let records = vec![vec![1u8; 100], vec![2u8; 100], vec![3u8; 100]];
    write_records(&path, &records);
    let len = fs::metadata(&path).unwrap().len();
    OpenOptions::new().write(true).open(&path).unwrap().set_len(len - 50).unwrap();
    assert_eq!(read_records(&path).unwrap(), records[..2].to_vec());

    write_records(&path, &records[..1]);
    OpenOptions::new().append(true).open(&path).unwrap().write_all(&[0u8; 100]).unwrap();
    assert_eq!(read_records(&path).unwrap(), records[..1].to_vec());
}

#[test]
fn a_bad_checksum_before_the_tail_is_corruption() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("000001.log");
    write_records(&path, &[vec![1u8; 100], vec![2u8; 100], vec![3u8; 100]]);
    let mut bytes = fs::read(&path).unwrap();
    bytes[20] ^= 0xff;
    fs::write(&path, bytes).unwrap();
    assert!(matches!(read_records(&path), Err(RocksError::Corrupt { .. })));
}

#[test]
fn batches_and_edits_round_trip() {
    let batch = WriteBatch {
        sequence: 42,
        ops: vec![
            BatchOp::Put { cf: 0, key: b"a".to_vec(), value: b"1".to_vec() },
            BatchOp::Put { cf: 2, key: b"tabletmeta_1_2".to_vec(), value: vec![9; 300] },
            BatchOp::Delete { cf: 2, key: b"tabletmeta_3_4".to_vec() },
            BatchOp::Delete { cf: 0, key: b"b".to_vec() },
        ],
    };
    assert_eq!(WriteBatch::decode(&batch.encode()).unwrap(), batch);

    let file = |number| NewFile {
        level: 6,
        number,
        path_id: 0,
        size: 1234,
        smallest: b"a\x01\0\0\0\0\0\0\0".to_vec(),
        largest: b"z\x01\0\0\0\0\0\0\0".to_vec(),
        smallest_seqno: 0,
        largest_seqno: 7,
        marked_for_compaction: false,
    };
    let edit = VersionEdit {
        comparator: Some("leveldb.BytewiseComparator".into()),
        log_number: Some(9),
        prev_log_number: Some(0),
        next_file_number: Some(12),
        last_sequence: Some(77),
        max_column_family: Some(2),
        min_log_number_to_keep: Some(9),
        column_family: 2,
        column_family_add: Some("meta".into()),
        column_family_drop: false,
        deleted_files: vec![(0, 3), (6, 4)],
        new_files: vec![file(10), file(11)],
    };
    assert_eq!(VersionEdit::decode(&edit.encode()).unwrap(), edit);
    let plain = VersionEdit { min_log_number_to_keep: None, column_family: 0, column_family_add: None, ..edit };
    assert_eq!(VersionEdit::decode(&plain.encode()).unwrap(), plain);
}
