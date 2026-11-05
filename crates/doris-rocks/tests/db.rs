use std::collections::BTreeSet;
use std::fs::{self, OpenOptions};
use std::path::{Path, PathBuf};

use doris_rocks::db::{Batch, CfOptions, Db, DbOptions};
use doris_rocks::RocksError;

fn fixture(dir: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures").join(dir)
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

fn lines(path: PathBuf) -> BTreeSet<String> {
    fs::read_to_string(path).map(|s| s.lines().map(str::to_string).collect()).unwrap_or_default()
}

fn meta_options() -> DbOptions {
    DbOptions {
        column_families: vec![
            CfOptions { name: "default".into(), fixed_prefix_len: None },
            CfOptions { name: "doris".into(), fixed_prefix_len: None },
            CfOptions { name: "meta".into(), fixed_prefix_len: Some(4) },
        ],
        ..DbOptions::default()
    }
}

fn keys(db: &Db, prefix: &[u8]) -> BTreeSet<String> {
    db.scan_prefix("meta", prefix).into_iter().map(|(k, _)| String::from_utf8(k.to_vec()).unwrap()).collect()
}

#[test]
fn opens_the_cpp_databases() {
    for dir in ["cpp-wal", "cpp-compacted"] {
        let tmp = tempfile::tempdir().unwrap();
        copy_dir(&fixture(dir).join("meta"), tmp.path());
        let db = Db::open(tmp.path(), &meta_options()).unwrap();
        let mut want = lines(fixture(dir).join("keys.txt"));
        want.extend(lines(fixture(dir).join("dropped.txt")));
        assert_eq!(keys(&db, b"tabletmeta_"), want, "{dir}");
    }
}

#[test]
fn opens_the_cpp_stream_load_database() {
    let tmp = tempfile::tempdir().unwrap();
    copy_dir(&fixture("cpp-wal").join("stream_load"), tmp.path());
    let opts = DbOptions {
        column_families: vec![
            CfOptions { name: "default".into(), fixed_prefix_len: None },
            CfOptions { name: "meta".into(), fixed_prefix_len: None },
        ],
        ..DbOptions::default()
    };
    let db = Db::open(tmp.path(), &opts).unwrap();
    assert!(db.scan_prefix("meta", b"").is_empty());
}

#[test]
fn a_column_family_on_disk_must_be_opened() {
    let tmp = tempfile::tempdir().unwrap();
    copy_dir(&fixture("cpp-wal").join("meta"), tmp.path());
    let opts = DbOptions { column_families: vec![CfOptions { name: "default".into(), fixed_prefix_len: None }], ..DbOptions::default() };
    assert!(Db::open(tmp.path(), &opts).is_err());
}

fn put_n(db: &mut Db, range: std::ops::Range<usize>) {
    for n in range {
        let mut b = Batch::default();
        b.put("meta", format!("tabletmeta_{n}_1").as_bytes(), format!("v{n}").as_bytes());
        db.write(b, false).unwrap();
    }
}

#[test]
fn writes_survive_reopen() {
    let tmp = tempfile::tempdir().unwrap();
    let mut db = Db::open(tmp.path(), &meta_options()).unwrap();
    put_n(&mut db, 0..100);
    for n in 0..10 {
        let mut b = Batch::default();
        b.delete("meta", format!("tabletmeta_{n}_1").as_bytes());
        db.write(b, false).unwrap();
    }
    db.close().unwrap();
    let db = Db::open(tmp.path(), &meta_options()).unwrap();
    assert_eq!(keys(&db, b"tabletmeta_").len(), 90);
    assert_eq!(db.get("meta", b"tabletmeta_42_1"), Some(&b"v42"[..]));
    assert_eq!(db.get("meta", b"tabletmeta_3_1"), None);
}

#[test]
fn writes_survive_a_kill() {
    let tmp = tempfile::tempdir().unwrap();
    let mut db = Db::open(tmp.path(), &meta_options()).unwrap();
    put_n(&mut db, 0..100);
    let copy = tempfile::tempdir().unwrap();
    copy_dir(tmp.path(), copy.path());
    drop(db);
    let db = Db::open(copy.path(), &meta_options()).unwrap();
    assert_eq!(keys(&db, b"tabletmeta_").len(), 100);
}

fn current_log(dir: &Path) -> PathBuf {
    let mut logs: Vec<_> =
        fs::read_dir(dir).unwrap().flatten().map(|e| e.path()).filter(|p| p.extension().is_some_and(|e| e == "log")).collect();
    logs.sort();
    logs.pop().unwrap()
}

#[test]
fn a_torn_log_tail_loses_only_the_last_write() {
    let tmp = tempfile::tempdir().unwrap();
    let mut db = Db::open(tmp.path(), &meta_options()).unwrap();
    put_n(&mut db, 0..3);
    let copy = tempfile::tempdir().unwrap();
    copy_dir(tmp.path(), copy.path());
    drop(db);
    let log = current_log(copy.path());
    let len = fs::metadata(&log).unwrap().len();
    OpenOptions::new().write(true).open(&log).unwrap().set_len(len - 3).unwrap();
    let db = Db::open(copy.path(), &meta_options()).unwrap();
    assert_eq!(keys(&db, b"tabletmeta_"), ["tabletmeta_0_1", "tabletmeta_1_1"].map(String::from).into());
}

#[test]
fn a_crash_mid_flush_keeps_the_old_state() {
    let tmp = tempfile::tempdir().unwrap();
    let mut db = Db::open(tmp.path(), &meta_options()).unwrap();
    put_n(&mut db, 0..10);
    db.close().unwrap();
    let failing = DbOptions { fail_before_current: true, ..meta_options() };
    assert!(Db::open(tmp.path(), &failing).is_err());
    let db = Db::open(tmp.path(), &meta_options()).unwrap();
    assert_eq!(keys(&db, b"tabletmeta_").len(), 10);
    drop(db);
    // the failed flush's files are gone: one manifest, one options file, one log, one table
    let names: Vec<String> = fs::read_dir(tmp.path()).unwrap().flatten().map(|e| e.file_name().into_string().unwrap()).collect();
    for (prefix, suffix) in [("MANIFEST-", ""), ("OPTIONS-", ""), ("", ".log"), ("", ".sst")] {
        assert_eq!(names.iter().filter(|n| n.starts_with(prefix) && n.ends_with(suffix)).count(), 1, "{prefix}*{suffix} in {names:?}");
    }
    assert!(!names.iter().any(|n| n.ends_with(".dbtmp")), "{names:?}");
}

#[test]
fn a_second_open_is_locked() {
    let tmp = tempfile::tempdir().unwrap();
    let _db = Db::open(tmp.path(), &meta_options()).unwrap();
    match Db::open(tmp.path(), &meta_options()) {
        Err(RocksError::Locked { path }) => assert_eq!(path, tmp.path().join("LOCK")),
        other => panic!("{other:?}"),
    }
}

#[test]
fn the_log_stays_bounded() {
    let tmp = tempfile::tempdir().unwrap();
    let opts = DbOptions { flush_log_bytes: 1 << 20, ..meta_options() };
    let mut db = Db::open(tmp.path(), &opts).unwrap();
    let value = vec![7u8; 2048];
    for n in 0..20_000 {
        let mut b = Batch::default();
        b.put("meta", format!("tabletmeta_{n}_1").as_bytes(), &value);
        db.write(b, false).unwrap();
        if n % 1000 == 0 {
            assert!(fs::metadata(current_log(tmp.path())).unwrap().len() < (1 << 20) + 4096);
        }
    }
    db.close().unwrap();
    let db = Db::open(tmp.path(), &opts).unwrap();
    assert_eq!(keys(&db, b"tabletmeta_").len(), 20_000);
}

#[test]
fn a_new_database_has_the_cpp_files() {
    let tmp = tempfile::tempdir().unwrap();
    let mut db = Db::open(tmp.path(), &meta_options()).unwrap();
    put_n(&mut db, 0..1);
    db.close().unwrap();
    let names: Vec<String> = fs::read_dir(tmp.path()).unwrap().flatten().map(|e| e.file_name().into_string().unwrap()).collect();
    for want in ["CURRENT", "IDENTITY", "LOCK"] {
        assert!(names.iter().any(|n| n == want), "{want} missing from {names:?}");
    }
    for (prefix, suffix) in [("MANIFEST-", ""), ("OPTIONS-", ""), ("", ".log")] {
        assert_eq!(names.iter().filter(|n| n.starts_with(prefix) && n.ends_with(suffix)).count(), 1, "{prefix}*{suffix} in {names:?}");
    }
    let current = fs::read_to_string(tmp.path().join("CURRENT")).unwrap();
    assert!(current.starts_with("MANIFEST-") && current.ends_with('\n'), "{current:?}");
    assert_eq!(fs::read_to_string(tmp.path().join("IDENTITY")).unwrap().trim().len(), 36);
}
