//! A small database in RocksDB 5.14.2's file layout, for metadata that fits in memory.
//!
//! The whole key set lives in memory, one ordered map per column family. Opening reads
//! `CURRENT`, the manifest it names, every live table file and every log not yet in a table
//! (`DBImpl::Recover`); writes append a write batch to the log (`DBImpl::WriteImpl`). The only
//! way files change otherwise is a **flush**, which rewrites the whole live key set: one table
//! file per non-empty column family at the bottommost level with sequence numbers zeroed (what a
//! full compaction leaves), a new manifest holding one snapshot (`VersionSet::WriteSnapshot`), a
//! new `OPTIONS` file, `CURRENT` switched by rename, a new log, and every file no longer
//! referenced deleted. A flush runs at open and whenever the log passes `flush_log_bytes`.
//!
//! RocksDB opens what this writes; this opens what RocksDB writes, including its compactions.

use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File, OpenOptions};
use std::hash::{BuildHasher, Hasher};
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::error::{IoContext, RocksError};
use crate::log::{read_records, LogWriter};
use crate::table::{read_table, InternalKey, TableBuilder, TableProps, ValueKind};
use crate::version_edit::{NewFile, VersionEdit};
use crate::write_batch::{BatchOp, WriteBatch};

const COMPARATOR: &str = "leveldb.BytewiseComparator";
/// `num_levels - 1` with the default 7 levels: where a full compaction's output goes.
const BOTTOM_LEVEL: u32 = 6;
const OPTIONS_DB: &str = include_str!("options_db.ini");
const OPTIONS_CF: &str = include_str!("options_cf.ini");

#[derive(Debug, Clone)]
pub struct CfOptions {
    pub name: String,
    /// `NewFixedPrefixTransform(n)`, recorded in table properties and the options file.
    pub fixed_prefix_len: Option<usize>,
}

#[derive(Debug, Clone)]
pub struct DbOptions {
    /// Every column family on disk must be listed (as `DB::Open` requires); listed ones that
    /// are missing are created.
    pub column_families: Vec<CfOptions>,
    /// Flush once the log passes this many bytes.
    pub flush_log_bytes: u64,
    /// Fault injection for tests: a flush stops after writing its table files, manifest and
    /// options file, before switching `CURRENT`.
    #[doc(hidden)]
    pub fail_before_current: bool,
}

impl Default for DbOptions {
    fn default() -> Self {
        DbOptions {
            column_families: vec![CfOptions { name: "default".into(), fixed_prefix_len: None }],
            flush_log_bytes: 4 << 20,
            fail_before_current: false,
        }
    }
}

/// A column family's live keys and values, in key order.
pub type KeySet = BTreeMap<Vec<u8>, Vec<u8>>;

/// Puts and deletes by column family name, applied atomically.
#[derive(Debug, Clone, Default)]
pub struct Batch {
    ops: Vec<(String, Vec<u8>, Option<Vec<u8>>)>,
}

impl Batch {
    pub fn put(&mut self, cf: &str, key: &[u8], value: &[u8]) {
        self.ops.push((cf.to_string(), key.to_vec(), Some(value.to_vec())));
    }

    pub fn delete(&mut self, cf: &str, key: &[u8]) {
        self.ops.push((cf.to_string(), key.to_vec(), None));
    }

    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }
}

#[derive(Debug)]
struct Cf {
    id: u32,
    name: String,
    fixed_prefix_len: Option<usize>,
    data: BTreeMap<Vec<u8>, Vec<u8>>,
}

#[derive(Debug)]
pub struct Db {
    dir: PathBuf,
    opts: DbOptions,
    cfs: Vec<Cf>,
    max_column_family: u32,
    last_sequence: u64,
    next_file: u64,
    log: LogWriter,
    log_number: u64,
    fail_writes: bool,
    _lock: File,
}

/// What a manifest says about one column family.
#[derive(Debug, Default)]
struct CfState {
    name: String,
    log_number: u64,
    files: BTreeMap<u64, NewFile>,
}

/// The newest (sequence, value or deletion) per user key, while recovering.
type Recovered = HashMap<Vec<u8>, (u64, Option<Vec<u8>>)>;

fn apply(map: &mut Recovered, key: Vec<u8>, sequence: u64, value: Option<Vec<u8>>) {
    match map.get(&key) {
        Some((s, _)) if *s > sequence => {}
        _ => {
            map.insert(key, (sequence, value));
        }
    }
}

fn file_number(name: &str, prefix: &str, suffix: &str) -> Option<u64> {
    name.strip_prefix(prefix)?.strip_suffix(suffix)?.parse().ok()
}

fn random_u64(salt: u64) -> u64 {
    let mut h = std::collections::hash_map::RandomState::new().build_hasher();
    h.write_u64(salt);
    h.write_u128(std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_nanos()).unwrap_or(0));
    h.finish()
}

fn sync_dir(dir: &Path) -> Result<(), RocksError> {
    File::open(dir).and_then(|d| d.sync_all()).at(dir)
}

impl Db {
    /// Every column family's live key set, read without writing anything (no lock, no flush):
    /// for checks that must leave a database untouched. A missing database reads as empty.
    pub fn read_only(dir: &Path) -> Result<BTreeMap<String, KeySet>, RocksError> {
        let r = recover(dir)?;
        Ok(r.states
            .iter()
            .map(|(id, s)| {
                let data = r.data.get(id).map(live).unwrap_or_default();
                (s.name.clone(), data)
            })
            .collect())
    }

    pub fn open(dir: &Path, opts: &DbOptions) -> Result<Db, RocksError> {
        fs::create_dir_all(dir).at(dir)?;
        let lock_path = dir.join("LOCK");
        let lock = OpenOptions::new().create(true).truncate(false).write(true).open(&lock_path).at(&lock_path)?;
        match lock.try_lock() {
            Ok(()) => {}
            Err(fs::TryLockError::WouldBlock) => return Err(RocksError::Locked { path: lock_path }),
            Err(fs::TryLockError::Error(e)) => return Err(RocksError::Io { path: lock_path, source: e }),
        }
        let identity = dir.join("IDENTITY");
        if !identity.exists() {
            let (a, b) = (random_u64(1), random_u64(2));
            let id = format!(
                "{:08x}-{:04x}-4{:03x}-{:04x}-{:012x}\n",
                a >> 32,
                (a >> 16) & 0xffff,
                a & 0xfff,
                0x8000 | ((b >> 48) & 0x3fff),
                b & 0xffff_ffff_ffff
            );
            fs::write(&identity, id).at(&identity)?;
        }

        let Recovery { states, data: mut recovered, last_sequence, next_file, mut max_column_family } = recover(dir)?;

        let mut cfs = Vec::new();
        for o in &opts.column_families {
            let found = states.iter().find(|(_, s)| s.name == o.name).map(|(id, _)| *id);
            let id = match (found, o.name.as_str()) {
                (Some(id), _) => id,
                (None, "default") => 0,
                (None, _) => {
                    max_column_family += 1;
                    max_column_family
                }
            };
            let data = recovered.remove(&id).as_ref().map(live).unwrap_or_default();
            cfs.push(Cf { id, name: o.name.clone(), fixed_prefix_len: o.fixed_prefix_len, data });
        }
        if let Some(s) = states.values().find(|s| !opts.column_families.iter().any(|o| o.name == s.name)) {
            return Err(RocksError::Unsupported {
                path: dir.to_path_buf(),
                what: format!("column family {} exists on disk but was not opened", s.name),
            });
        }
        cfs.sort_by_key(|c| c.id);
        max_column_family = max_column_family.max(cfs.iter().map(|c| c.id).max().unwrap_or(0));

        // a placeholder log until the first flush creates the real one
        let placeholder = tempfile_in(dir)?;
        let mut db = Db {
            dir: dir.to_path_buf(),
            opts: opts.clone(),
            cfs,
            max_column_family,
            last_sequence,
            next_file,
            log: LogWriter::new(placeholder, 0),
            log_number: 0,
            fail_writes: false,
            _lock: lock,
        };
        db.flush()?;
        Ok(db)
    }

    fn cf(&self, name: &str) -> Option<&Cf> {
        self.cfs.iter().find(|c| c.name == name)
    }

    pub fn get(&self, cf: &str, key: &[u8]) -> Option<&[u8]> {
        self.cf(cf)?.data.get(key).map(Vec::as_slice)
    }

    /// Every (key, value) of `cf` whose key starts with `prefix`, in key order.
    pub fn scan_prefix(&self, cf: &str, prefix: &[u8]) -> Vec<(&[u8], &[u8])> {
        let Some(cf) = self.cf(cf) else { return Vec::new() };
        cf.data
            .range(prefix.to_vec()..)
            .take_while(|(k, _)| k.starts_with(prefix))
            .map(|(k, v)| (k.as_slice(), v.as_slice()))
            .collect()
    }

    /// Fault injection for tests: while on, every write fails as a full disk would.
    #[doc(hidden)]
    pub fn inject_write_failure(&mut self, on: bool) {
        self.fail_writes = on;
    }

    /// Appends the batch to the log (synced when `sync`), then applies it.
    pub fn write(&mut self, batch: Batch, sync: bool) -> Result<(), RocksError> {
        if batch.is_empty() {
            return Ok(());
        }
        if self.fail_writes {
            return Err(RocksError::Io { path: self.log_path(), source: std::io::Error::other("injected write failure") });
        }
        let mut ops = Vec::with_capacity(batch.ops.len());
        let mut targets = Vec::with_capacity(batch.ops.len());
        for (name, key, value) in batch.ops {
            let Some(i) = self.cfs.iter().position(|c| c.name == name) else {
                return Err(RocksError::Unsupported { path: self.dir.clone(), what: format!("no column family {name}") });
            };
            let cf = self.cfs[i].id;
            ops.push(match &value {
                Some(v) => BatchOp::Put { cf, key: key.clone(), value: v.clone() },
                None => BatchOp::Delete { cf, key: key.clone() },
            });
            targets.push((i, key, value));
        }
        let n = ops.len() as u64;
        let wb = WriteBatch { sequence: self.last_sequence + 1, ops };
        let log_path = self.log_path();
        self.log.add_record(&wb.encode()).at(&log_path)?;
        if sync {
            self.log.sync().at(&log_path)?;
        }
        for (i, key, value) in targets {
            match value {
                Some(v) => self.cfs[i].data.insert(key, v),
                None => self.cfs[i].data.remove(&key),
            };
        }
        self.last_sequence += n;
        if self.log.len() > self.opts.flush_log_bytes {
            self.flush()?;
        }
        Ok(())
    }

    fn log_path(&self) -> PathBuf {
        self.dir.join(format!("{:06}.log", self.log_number))
    }

    fn alloc(&mut self) -> u64 {
        let n = self.next_file;
        self.next_file += 1;
        n
    }

    /// Rewrites the whole live key set (see the module docs).
    pub fn flush(&mut self) -> Result<(), RocksError> {
        let mut keep: Vec<String> = Vec::new();
        let mut files: BTreeMap<u32, NewFile> = BTreeMap::new();
        for i in 0..self.cfs.len() {
            if self.cfs[i].data.is_empty() {
                continue;
            }
            let number = self.alloc();
            let name = format!("{number:06}.sst");
            let path = self.dir.join(&name);
            let cf = &self.cfs[i];
            let props = TableProps {
                column_family_id: cf.id,
                column_family_name: cf.name.clone(),
                fixed_prefix_len: cf.fixed_prefix_len,
            };
            let mut b = TableBuilder::new(File::create(&path).at(&path)?, props);
            for (k, v) in &cf.data {
                b.add(&InternalKey { user_key: k.clone(), sequence: 0, kind: ValueKind::Value }, v)?;
            }
            let meta = b.finish().map_err(|e| match e {
                RocksError::Io { source, .. } => RocksError::Io { path: path.clone(), source },
                other => other,
            })?;
            files.insert(
                cf.id,
                NewFile {
                    level: BOTTOM_LEVEL,
                    number,
                    path_id: 0,
                    size: meta.size,
                    smallest: meta.smallest.encode(),
                    largest: meta.largest.encode(),
                    smallest_seqno: 0,
                    largest_seqno: 0,
                    marked_for_compaction: false,
                },
            );
            keep.push(name);
        }
        let log_number = self.alloc();
        let manifest_number = self.alloc();
        let options_number = self.alloc();

        let manifest_name = format!("MANIFEST-{manifest_number:06}");
        let manifest_path = self.dir.join(&manifest_name);
        let mut m = LogWriter::new(File::create(&manifest_path).at(&manifest_path)?, manifest_number);
        for cf in &self.cfs {
            let add = VersionEdit {
                column_family: cf.id,
                column_family_add: (cf.id != 0).then(|| cf.name.clone()),
                comparator: Some(COMPARATOR.into()),
                ..VersionEdit::default()
            };
            let content = VersionEdit {
                column_family: cf.id,
                log_number: Some(log_number),
                new_files: files.remove(&cf.id).into_iter().collect(),
                ..VersionEdit::default()
            };
            m.add_record(&add.encode()).at(&manifest_path)?;
            m.add_record(&content.encode()).at(&manifest_path)?;
        }
        let last = VersionEdit {
            log_number: Some(log_number),
            prev_log_number: Some(0),
            next_file_number: Some(self.next_file),
            last_sequence: Some(self.last_sequence),
            max_column_family: Some(self.max_column_family),
            ..VersionEdit::default()
        };
        m.add_record(&last.encode()).at(&manifest_path)?;
        m.sync().at(&manifest_path)?;
        keep.push(manifest_name.clone());

        let options_name = format!("OPTIONS-{options_number:06}");
        let options_path = self.dir.join(&options_name);
        fs::write(&options_path, self.options_text()).at(&options_path)?;
        keep.push(options_name);

        if self.opts.fail_before_current {
            return Err(RocksError::Io {
                path: self.dir.join("CURRENT"),
                source: std::io::Error::other("injected failure before CURRENT"),
            });
        }
        let tmp = self.dir.join(format!("{manifest_number:06}.dbtmp"));
        {
            let mut f = File::create(&tmp).at(&tmp)?;
            f.write_all(format!("{manifest_name}\n").as_bytes()).at(&tmp)?;
            f.sync_all().at(&tmp)?;
        }
        let current = self.dir.join("CURRENT");
        fs::rename(&tmp, &current).at(&current)?;
        sync_dir(&self.dir)?;

        let log_name = format!("{log_number:06}.log");
        let log_path = self.dir.join(&log_name);
        self.log = LogWriter::new(File::create(&log_path).at(&log_path)?, log_number);
        self.log_number = log_number;
        keep.push(log_name);

        for e in fs::read_dir(&self.dir).at(&self.dir)?.flatten() {
            let name = e.file_name().into_string().unwrap_or_default();
            let ours = name.ends_with(".sst")
                || name.ends_with(".log")
                || name.ends_with(".dbtmp")
                || name.starts_with("MANIFEST-")
                || name.starts_with("OPTIONS-")
                || name.starts_with(".doris-rocks-");
            if ours && !keep.contains(&name) {
                let p = e.path();
                fs::remove_file(&p).at(&p)?;
            }
        }
        Ok(())
    }

    fn options_text(&self) -> String {
        let cfs: String = self
            .cfs
            .iter()
            .map(|c| {
                OPTIONS_CF
                    .replace("{name}", &c.name)
                    .replace("{prefix}", &c.fixed_prefix_len.map_or("nullptr".into(), |n| format!("rocksdb.FixedPrefix.{n}")))
                    .replace("{write_buffers}", if c.fixed_prefix_len.is_some() { "5" } else { "2" })
            })
            .collect();
        OPTIONS_DB.replace("{dir}", &self.dir.display().to_string()).replace("{column_families}", &cfs)
    }

    /// Syncs the log and closes the database.
    pub fn close(mut self) -> Result<(), RocksError> {
        let p = self.log_path();
        self.log.sync().at(&p)
    }
}

/// What a database's files say: column families, their newest values, and the numbers.
struct Recovery {
    states: BTreeMap<u32, CfState>,
    data: BTreeMap<u32, Recovered>,
    last_sequence: u64,
    next_file: u64,
    max_column_family: u32,
}

fn live(map: &Recovered) -> BTreeMap<Vec<u8>, Vec<u8>> {
    map.iter().filter_map(|(k, (_, v))| v.clone().map(|v| (k.clone(), v))).collect()
}

/// Reads `CURRENT`, the manifest, the live tables and the logs (`DBImpl::Recover`).
fn recover(dir: &Path) -> Result<Recovery, RocksError> {
        let mut states: BTreeMap<u32, CfState> = BTreeMap::new();
        let mut last_sequence = 0;
        let mut next_file = 2;
        let mut max_column_family = 0;
        let current = dir.join("CURRENT");
        if current.exists() {
            let name = fs::read_to_string(&current).at(&current)?;
            let manifest = dir.join(name.trim_end_matches('\n'));
            for record in read_records(&manifest)? {
                let e = VersionEdit::decode(&record).map_err(|what| RocksError::Corrupt { path: manifest.clone(), what })?;
                if let Some(c) = &e.comparator {
                    if c != COMPARATOR {
                        return Err(RocksError::Unsupported { path: manifest.clone(), what: format!("comparator {c}") });
                    }
                }
                if let Some(name) = &e.column_family_add {
                    states.insert(e.column_family, CfState { name: name.clone(), ..CfState::default() });
                }
                if e.column_family == 0 && !states.contains_key(&0) {
                    states.insert(0, CfState { name: "default".into(), ..CfState::default() });
                }
                if e.column_family_drop {
                    states.remove(&e.column_family);
                    continue;
                }
                if let Some(s) = states.get_mut(&e.column_family) {
                    if let Some(n) = e.log_number {
                        s.log_number = n;
                    }
                    for (_, number) in &e.deleted_files {
                        s.files.remove(number);
                    }
                    for f in e.new_files {
                        s.files.insert(f.number, f);
                    }
                }
                last_sequence = e.last_sequence.unwrap_or(last_sequence);
                next_file = e.next_file_number.unwrap_or(next_file);
                max_column_family = e.max_column_family.unwrap_or(max_column_family);
            }
        }

        let mut recovered: BTreeMap<u32, Recovered> = BTreeMap::new();
        for (id, s) in &states {
            let map = recovered.entry(*id).or_default();
            // oldest first: deeper levels, then level 0 by file number
            let mut files: Vec<&NewFile> = s.files.values().collect();
            files.sort_by_key(|f| (std::cmp::Reverse(f.level), f.number));
            for f in files {
                let path = dir.join(format!("{:06}.sst", f.number));
                for (k, v) in read_table(&path)?.entries {
                    let value = (k.kind == ValueKind::Value).then_some(v);
                    apply(map, k.user_key, k.sequence, value);
                }
            }
        }
        if let Some(min_log) = states.values().map(|s| s.log_number).min() {
            let mut logs: Vec<u64> = fs::read_dir(dir)
                .at(dir)?
                .flatten()
                .filter_map(|e| file_number(e.file_name().to_str()?, "", ".log"))
                .filter(|n| *n >= min_log)
                .collect();
            logs.sort_unstable();
            for number in logs {
                let path = dir.join(format!("{number:06}.log"));
                for record in read_records(&path)? {
                    let batch = WriteBatch::decode(&record).map_err(|what| RocksError::Corrupt { path: path.clone(), what })?;
                    for (i, op) in batch.ops.into_iter().enumerate() {
                        let sequence = batch.sequence + i as u64;
                        last_sequence = last_sequence.max(sequence);
                        let Some(s) = states.get(&op.cf()) else { continue };
                        if number < s.log_number {
                            continue;
                        }
                        let map = recovered.entry(op.cf()).or_default();
                        match op {
                            BatchOp::Put { key, value, .. } => apply(map, key, sequence, Some(value)),
                            BatchOp::Delete { key, .. } => apply(map, key, sequence, None),
                        }
                    }
                }
            }
        }

        Ok(Recovery { states, data: recovered, last_sequence, next_file, max_column_family })
}

/// An unlinked scratch file in `dir`, standing in for the log before the first flush.
fn tempfile_in(dir: &Path) -> Result<File, RocksError> {
    let p = dir.join(format!(".doris-rocks-{}", random_u64(3)));
    let f = File::create(&p).at(&p)?;
    fs::remove_file(&p).at(&p)?;
    Ok(f)
}
