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
fn a_garbled_log_record_loses_only_what_follows_it() {
    // a power loss can persist a record's header but not its payload; the C++ BE still opens
    let tmp = tempfile::tempdir().unwrap();
    let mut db = Db::open(tmp.path(), &meta_options()).unwrap();
    put_n(&mut db, 0..3);
    let copy = tempfile::tempdir().unwrap();
    copy_dir(tmp.path(), copy.path());
    drop(db);
    let log = current_log(copy.path());
    let mut bytes = fs::read(&log).unwrap();
    let n = bytes.len();
    bytes[n - 5] ^= 0xff; // the last record's payload
    fs::write(&log, bytes).unwrap();
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
    let mut db = Db::open(tmp.path(), &meta_options()).unwrap();
    match Db::open(tmp.path(), &meta_options()) {
        Err(RocksError::Locked { path }) => assert_eq!(path, tmp.path().join("LOCK")),
        other => panic!("{other:?}"),
    }
    // the first keeps its lock and keeps writing
    put_n(&mut db, 0..1);
    assert!(!lockable_by_another_process(tmp.path()));
    drop(db);
    assert!(lockable_by_another_process(tmp.path()));
}

/// Whether another process can take the C++ BE's lock (`PosixEnv::LockFile`: fcntl F_SETLK,
/// F_WRLCK on the whole file); python's fcntl.lockf is that call.
fn lockable_by_another_process(dir: &Path) -> bool {
    let script = "import fcntl,sys; f=open(sys.argv[1],'a'); fcntl.lockf(f, fcntl.LOCK_EX | fcntl.LOCK_NB)";
    std::process::Command::new("python3").args(["-c", script]).arg(dir.join("LOCK")).status().unwrap().success()
}

#[test]
fn a_cpp_lock_keeps_doris_rs_out() {
    // a C++ BE running on the root holds an fcntl lock on LOCK
    let tmp = tempfile::tempdir().unwrap();
    fs::write(tmp.path().join("LOCK"), "").unwrap();
    let script = "import fcntl,sys,time; f=open(sys.argv[1],'a'); fcntl.lockf(f, fcntl.LOCK_EX); print('locked', flush=True); time.sleep(30)";
    let mut child = std::process::Command::new("python3")
        .args(["-c", script])
        .arg(tmp.path().join("LOCK"))
        .stdout(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    let mut line = String::new();
    std::io::BufRead::read_line(&mut std::io::BufReader::new(child.stdout.take().unwrap()), &mut line).unwrap();
    assert_eq!(line.trim(), "locked");
    let result = Db::open(tmp.path(), &meta_options());
    child.kill().unwrap();
    child.wait().unwrap();
    assert!(matches!(result, Err(RocksError::Locked { .. })), "{result:?}");
}

#[test]
fn the_log_stays_bounded() {
    // a flush rewrites every live key, so it waits until the log outgrows the live key set:
    // the log stays under max(flush_log_bytes, live bytes) plus one batch
    let tmp = tempfile::tempdir().unwrap();
    let opts = DbOptions { flush_log_bytes: 1 << 20, ..meta_options() };
    let mut db = Db::open(tmp.path(), &opts).unwrap();
    let value = vec![7u8; 2048];
    for n in 0..20_000 {
        let mut b = Batch::default();
        b.put("meta", format!("tabletmeta_{n}_1").as_bytes(), &value);
        db.write(b, false).unwrap();
        if n % 1000 == 0 {
            let live = (n as u64 + 1) * (2048 + 24);
            assert!(fs::metadata(current_log(tmp.path())).unwrap().len() < (1u64 << 20).max(live) + 4096);
        }
    }
    db.close().unwrap();
    let db = Db::open(tmp.path(), &opts).unwrap();
    assert_eq!(keys(&db, b"tabletmeta_").len(), 20_000);
}

fn manifest_number(dir: &Path) -> String {
    fs::read_to_string(dir.join("CURRENT")).unwrap()
}

#[test]
fn flushes_scale_with_the_key_set() {
    // 200 headers of 4 KiB, each rewritten 5 times: about 4 MB written into a 0.8 MB key set
    // should cost a handful of full rewrites, not one per flush_log_bytes
    let tmp = tempfile::tempdir().unwrap();
    let opts = DbOptions { flush_log_bytes: 4096, ..meta_options() };
    let mut db = Db::open(tmp.path(), &opts).unwrap();
    let value = vec![1u8; 4096];
    let mut flushes = 0;
    let mut last = manifest_number(tmp.path());
    for round in 0..6 {
        for n in 0..200 {
            let mut b = Batch::default();
            b.put("meta", format!("tabletmeta_{n}_1").as_bytes(), &value);
            db.write(b, false).unwrap();
            let now = manifest_number(tmp.path());
            if now != last && round > 0 {
                flushes += 1;
            }
            last = now;
        }
    }
    assert!(flushes <= 6, "{flushes} full rewrites");
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

fn listing(dir: &Path) -> Vec<(String, u64)> {
    let mut out: Vec<_> =
        fs::read_dir(dir).unwrap().flatten().map(|e| (e.file_name().into_string().unwrap(), e.metadata().unwrap().len())).collect();
    out.sort();
    out
}

#[test]
fn read_only_changes_nothing() {
    let tmp = tempfile::tempdir().unwrap();
    copy_dir(&fixture("cpp-compacted").join("meta"), tmp.path());
    let before = listing(tmp.path());
    let cfs = Db::read_only(tmp.path()).unwrap();
    assert_eq!(listing(tmp.path()), before);
    assert_eq!(cfs.keys().map(String::as_str).collect::<Vec<_>>(), ["default", "doris", "meta"]);
    let mut want = lines(fixture("cpp-compacted").join("keys.txt"));
    want.extend(lines(fixture("cpp-compacted").join("dropped.txt")));
    let got: BTreeSet<String> = cfs["meta"].keys().map(|k| String::from_utf8(k.clone()).unwrap()).collect();
    assert_eq!(got, want);
    assert!(Db::read_only(&tmp.path().join("missing")).unwrap().is_empty());
}

#[test]
fn an_injected_write_failure_fails_the_write() {
    let tmp = tempfile::tempdir().unwrap();
    let mut db = Db::open(tmp.path(), &meta_options()).unwrap();
    db.inject_write_failure(true);
    let mut b = Batch::default();
    b.put("meta", b"k", b"v");
    assert!(db.write(b, false).is_err());
    assert_eq!(db.get("meta", b"k"), None);
}

#[test]
fn a_failed_flush_keeps_every_acknowledged_write() {
    // flush_log_bytes = 1: every write tries a flush; with flushes failing, writes still land in
    // the current log and survive a reopen
    let tmp = tempfile::tempdir().unwrap();
    let opts = DbOptions { flush_log_bytes: 1, ..meta_options() };
    let mut db = Db::open(tmp.path(), &opts).unwrap();
    db.inject_flush_failure(true);
    put_n(&mut db, 0..3);
    assert_eq!(db.get("meta", b"tabletmeta_2_1"), Some(&b"v2"[..]));
    db.inject_flush_failure(false);
    put_n(&mut db, 3..4);
    drop(db);
    let db = Db::open(tmp.path(), &meta_options()).unwrap();
    assert_eq!(keys(&db, b"tabletmeta_").len(), 4);
}

#[test]
fn a_missing_current_beside_data_files_is_corruption() {
    let tmp = tempfile::tempdir().unwrap();
    let mut db = Db::open(tmp.path(), &meta_options()).unwrap();
    put_n(&mut db, 0..3);
    db.close().unwrap();
    fs::remove_file(tmp.path().join("CURRENT")).unwrap();
    assert!(matches!(Db::open(tmp.path(), &meta_options()), Err(RocksError::Corrupt { .. })));
    assert!(fs::read_dir(tmp.path()).unwrap().flatten().any(|e| e.file_name().to_string_lossy().ends_with(".log")));
}

#[test]
fn file_numbers_move_past_logs_on_disk() {
    // MarkFileNumberUsedDuringRecovery: a log can be numbered at or above the manifest's
    // next_file_number; new files must not reuse its number
    let tmp = tempfile::tempdir().unwrap();
    let db = Db::open(tmp.path(), &meta_options()).unwrap();
    db.close().unwrap();
    doris_rocks::log::LogWriter::new(fs::File::create(tmp.path().join("000050.log")).unwrap(), 50).sync().unwrap();
    drop(Db::open(tmp.path(), &meta_options()).unwrap());
    let manifest = fs::read_to_string(tmp.path().join("CURRENT")).unwrap();
    let n: u64 = manifest.trim().trim_start_matches("MANIFEST-").parse().unwrap();
    assert!(n > 50, "{manifest}");
}
