//! One storage root, laid out as the C++ BE lays it out (`be/src/storage/olap_define.h`,
//! `be/src/storage/data_dir.cpp`): `cluster_id`, `data/<shard>/<tablet_id>/<schema_hash>/` with
//! shards 0..1023 created at start, `trash/<time>.<n>/<tablet_id>/<schema_hash>/` for dropped
//! tablets, the other folders the C++ BE creates at start, and the tablet metadata store
//! `meta/` (`OlapMeta`).

use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use doris_proto::doris::TabletMetaPb;
use prost::Message;

use crate::error::{IoContext, StorageError};
use crate::olap_meta::{Contents, OlapMeta};
use crate::path_hash::hash_of_path;

/// `DataDir::MAX_SHARD_NUM`.
pub const MAX_SHARD_NUM: u64 = 1024;
const CLUSTER_ID: &str = "cluster_id";
/// Folders the C++ BE creates at start (taken from the reference's layout fixture).
const START_DIRS: [&str; 8] = ["data", "error_log", "spill", "spill_gc", "stream_load", "tmp", "wal", "wal/tmp"];
const DEPLOY_MODE: &str = "deploy_mode";
/// M2's temporary header store, moved into `meta/` on first start.
const OLD_HEADER_DIR: &str = "doris-rs-meta";

/// `delete_counter` in `DataDir::move_to_trash`: process-wide, so two moves in one second differ.
static TRASH_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Debug)]
pub struct DataDir {
    root: PathBuf,
    path_hash: i64,
    cluster_id: Option<i32>,
    next_shard: u64,
    meta: OlapMeta,
    skipped: Vec<String>,
}

impl DataDir {
    /// Opens a root as `DataDir::init` and `DataDir::load` do. First, reading only: a root with
    /// legacy headers or with tablet data is refused, untouched. Then the start folders, the
    /// metadata store (M2's temporary header store moved into it), headers whose rowsets lack
    /// their schema saved again, and half-created tablets cleared (a header without its
    /// directory, a directory without its header). Headers the C++ BE would fail to load are
    /// skipped (`ignore_load_tablet_failure`, default true) and listed in `skipped_headers`.
    pub fn open(root: &Path, host: &str) -> Result<DataDir, StorageError> {
        if !root.is_dir() {
            return Err(StorageError::MissingRoot { root: root.to_path_buf() });
        }
        let contents = Contents::read(root)?;
        if contents.has_legacy_header() {
            return Err(StorageError::LegacyHeader { root: root.to_path_buf() });
        }
        if let Some(key) = contents.first_data_key() {
            return Err(StorageError::DataRoot { root: root.to_path_buf(), tablet_id: None, key: Some(key) });
        }
        let (headers, skipped) = contents.headers();
        let data_root = |tablet_id| StorageError::DataRoot { root: root.to_path_buf(), tablet_id: Some(tablet_id), key: None };
        if let Some(m) = headers.iter().chain(skipped.iter().filter_map(|s| s.meta.as_ref())).find(|m| holds_data(m)) {
            return Err(data_root(m.tablet_id()));
        }
        // a tablet directory with files and no valid header may hold segments (a lost or damaged
        // meta/): refuse rather than let the cleanup below send it to trash
        let known: HashSet<(i64, i32)> = headers.iter().map(|m| (m.tablet_id(), m.schema_hash())).collect();
        let skipped_tablets: HashSet<(i64, i32)> = skipped.iter().flat_map(|s| s.tablets.iter().copied()).collect();
        for (tablet_id, schema_hash, path) in tablet_dirs(root)? {
            let unknown = !known.contains(&(tablet_id, schema_hash)) && !skipped_tablets.contains(&(tablet_id, schema_hash));
            if unknown && fs::read_dir(&path).at(&path)?.next().is_some() {
                return Err(data_root(tablet_id));
            }
        }
        let skipped_lines = skipped.iter().map(|s| s.line.clone()).collect();
        for d in START_DIRS {
            let p = root.join(d);
            fs::create_dir_all(&p).at(&p)?;
        }
        for shard in 0..MAX_SHARD_NUM {
            let p = root.join("data").join(shard.to_string());
            if !p.is_dir() {
                fs::create_dir(&p).at(&p)?;
            }
        }
        let deploy = root.join(DEPLOY_MODE);
        if !deploy.exists() {
            fs::write(&deploy, "local").at(&deploy)?;
        }
        let mut meta = OlapMeta::open(root)?;
        move_old_headers(root, &mut meta)?;
        for mut m in meta.tablet_metas()? {
            if m.rs_metas.iter().any(|r| r.tablet_schema.is_none()) {
                let schema = m.schema.clone();
                for r in m.rs_metas.iter_mut().filter(|r| r.tablet_schema.is_none()) {
                    r.tablet_schema.clone_from(&schema);
                }
                meta.save_tablet_meta(&m, true)?;
            }
        }
        let cluster_id = read_cluster_id(&root.join(CLUSTER_ID))?;
        let path_hash = hash_of_path(host, root.to_str().unwrap_or_default()) as i64;
        let mut dir = DataDir { root: root.to_path_buf(), path_hash, cluster_id, next_shard: 0, meta, skipped: skipped_lines };
        dir.clear_half_created(&skipped_tablets)?;
        Ok(dir)
    }

    /// Headers the C++ BE would fail to load, with why.
    pub fn skipped_headers(&self) -> &[String] {
        &self.skipped
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn cluster_id(&self) -> Option<i32> {
        self.cluster_id
    }

    /// As `DataDir::set_cluster_id`: an assigned root keeps its id and refuses another.
    pub fn set_cluster_id(&mut self, id: i32) -> Result<(), StorageError> {
        match self.cluster_id {
            Some(current) if current != id => Err(StorageError::ClusterIdAssigned { current, new: id }),
            Some(_) => Ok(()),
            None => {
                let path = self.root.join(CLUSTER_ID);
                fs::write(&path, id.to_string()).at(&path)?;
                self.cluster_id = Some(id);
                Ok(())
            }
        }
    }

    pub fn path_hash(&self) -> i64 {
        self.path_hash
    }

    /// `DataDir::get_shard`: round robin over the shards.
    pub fn next_shard(&mut self) -> u64 {
        let shard = self.next_shard % MAX_SHARD_NUM;
        self.next_shard += 1;
        shard
    }

    pub fn tablet_dir(&self, shard: u64, tablet_id: i64, schema_hash: i32) -> PathBuf {
        self.root.join("data").join(shard.to_string()).join(tablet_id.to_string()).join(schema_hash.to_string())
    }

    /// `DataDir::move_to_trash` for a swept tablet: with `trash_file_expire_time_sec` <= 0 (the
    /// default) the directory is deleted at once, as the C++ BE does; otherwise it goes to trash.
    pub fn remove_tablet_dir(&self, dir: &Path, trash_expire_secs: i64) -> Result<(), StorageError> {
        if trash_expire_secs > 0 {
            return self.move_to_trash(dir);
        }
        fs::remove_dir_all(dir).at(dir)?;
        let parent = dir.parent().unwrap_or(dir);
        if fs::read_dir(parent).at(parent)?.next().is_none() {
            fs::remove_dir(parent).at(parent)?;
        }
        Ok(())
    }

    /// `trash/<YYYYmmddHHMMSS>.<n>/<tablet_id>/<schema_hash>`, then the emptied `<tablet_id>`
    /// parent is removed.
    pub fn move_to_trash(&self, dir: &Path) -> Result<(), StorageError> {
        let schema_hash = dir.file_name().unwrap_or_default();
        let parent = dir.parent().unwrap_or(dir);
        let tablet_id = parent.file_name().unwrap_or_default();
        let label = format!("{}.{}", timestamp(), TRASH_COUNTER.fetch_add(1, Ordering::Relaxed));
        let target_parent = self.root.join("trash").join(label).join(tablet_id);
        fs::create_dir_all(&target_parent).at(&target_parent)?;
        fs::rename(dir, target_parent.join(schema_hash)).at(dir)?;
        if fs::read_dir(parent).at(parent)?.next().is_none() {
            fs::remove_dir(parent).at(parent)?;
        }
        Ok(())
    }

    /// (total, available) bytes of the file system holding the root.
    pub fn capacity(&self) -> Result<(i64, i64), StorageError> {
        let total = fs4::total_space(&self.root).at(&self.root)?;
        let available = fs4::available_space(&self.root).at(&self.root)?;
        Ok((total as i64, available as i64))
    }

    /// Bytes under `trash/`.
    pub fn trash_used(&self) -> i64 {
        fn size(p: &Path) -> u64 {
            match fs::symlink_metadata(p) {
                Ok(m) if m.is_dir() => fs::read_dir(p).map(|d| d.flatten().map(|e| size(&e.path())).sum()).unwrap_or(0),
                Ok(m) => m.len(),
                Err(_) => 0,
            }
        }
        size(&self.root.join("trash")) as i64
    }

    /// `StorageEngine::_do_sweep` on `snapshot/` and `trash/`: deletes each entry whose label
    /// time (`<YYYYmmddHHMMSS>.<n>[.<timeout>]`) is at least its expiry old; a third label part
    /// is that entry's own timeout in seconds. Trash expires at once when the disk is above the
    /// guard (`storage_flood_stage_usage_percent` 90 × 0.9).
    pub fn sweep_expired(&self, trash_expire_secs: i64, snapshot_expire_secs: i64) -> Result<(), StorageError> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs() as i64).unwrap_or(0);
        let usage = self.capacity().map(|(t, a)| if t > 0 { (t - a) as f64 / t as f64 } else { 0.0 }).unwrap_or(0.0);
        let trash_expire = if usage > 0.9 * 0.9 { 0 } else { trash_expire_secs };
        for (sub, expire) in [("snapshot", snapshot_expire_secs), ("trash", trash_expire)] {
            let dir = self.root.join(sub);
            let Ok(entries) = fs::read_dir(&dir) else { continue };
            let mut names: Vec<_> = entries.flatten().map(|e| e.path()).collect();
            names.sort();
            for path in names {
                let name = path.file_name().and_then(|n| n.to_str()).unwrap_or_default();
                let mut parts = name.split('.');
                let Some(created) = parts.next().and_then(parse_timestamp) else { continue };
                let expire = parts.nth(1).and_then(|t| t.parse().ok()).unwrap_or(expire);
                if now - created >= expire {
                    if path.is_dir() { fs::remove_dir_all(&path) } else { fs::remove_file(&path) }.at(&path)?;
                }
            }
        }
        Ok(())
    }

    pub fn meta(&self) -> &OlapMeta {
        &self.meta
    }

    pub fn meta_mut(&mut self) -> &mut OlapMeta {
        &mut self.meta
    }

    /// Clears tablets half created by a crash; the directories of skipped headers are left alone.
    fn clear_half_created(&mut self, skipped: &HashSet<(i64, i32)>) -> Result<(), StorageError> {
        let mut known = skipped.clone();
        for meta in self.meta.tablet_metas()? {
            let (id, hash) = (meta.tablet_id(), meta.schema_hash());
            if self.tablet_dir(meta.shard_id() as u64, id, hash).is_dir() {
                known.insert((id, hash));
            } else {
                self.meta.remove_tablet_meta(id, hash, true)?;
            }
        }
        for (tablet_id, schema_hash, path) in tablet_dirs(&self.root)? {
            if !known.contains(&(tablet_id, schema_hash)) {
                self.move_to_trash(&path)?;
            }
        }
        Ok(())
    }
}

/// A tablet holds data when one of its rowsets has a segment.
fn holds_data(m: &TabletMetaPb) -> bool {
    m.rs_metas.iter().chain(&m.inc_rs_metas).chain(&m.stale_rs_metas).any(|r| r.num_segments() > 0)
}

/// Moves M2's `doris-rs-meta/tabletmeta_<id>_<hash>` files into `meta/`, then removes the folder.
fn move_old_headers(root: &Path, meta: &mut OlapMeta) -> Result<(), StorageError> {
    let old = root.join(OLD_HEADER_DIR);
    if !old.is_dir() {
        return Ok(());
    }
    for entry in fs::read_dir(&old).at(&old)? {
        let path = entry.at(&old)?.path();
        let name = path.file_name().and_then(|n| n.to_str()).unwrap_or_default();
        if !name.starts_with("tabletmeta_") || name.ends_with(".tmp") {
            continue;
        }
        let bytes = fs::read(&path).at(&path)?;
        let m = TabletMetaPb::decode(bytes.as_slice()).map_err(|source| StorageError::BadHeader { path: path.clone(), source })?;
        meta.save_tablet_meta(&m, true)?;
    }
    fs::remove_dir_all(&old).at(&old)
}

/// Every `data/<shard>/<tablet_id>/<schema_hash>` directory.
fn tablet_dirs(root: &Path) -> Result<Vec<(i64, i32, PathBuf)>, StorageError> {
    let mut out = Vec::new();
    let data = root.join("data");
    if !data.is_dir() {
        return Ok(out);
    }
    for shard in fs::read_dir(&data).at(&data)? {
        let shard = shard.at(&data)?.path();
        if !shard.is_dir() {
            continue;
        }
        for tablet in fs::read_dir(&shard).at(&shard)? {
            let tablet = tablet.at(&shard)?.path();
            let Some(tablet_id) = parse_name::<i64>(&tablet) else { continue };
            for hash in fs::read_dir(&tablet).at(&tablet)? {
                let hash = hash.at(&tablet)?.path();
                if let Some(schema_hash) = parse_name::<i32>(&hash) {
                    out.push((tablet_id, schema_hash, hash));
                }
            }
        }
    }
    Ok(out)
}

fn parse_name<T: std::str::FromStr>(path: &Path) -> Option<T> {
    if !path.is_dir() {
        return None;
    }
    path.file_name()?.to_str()?.parse().ok()
}

fn read_cluster_id(path: &Path) -> Result<Option<i32>, StorageError> {
    match fs::read_to_string(path) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(StorageError::Io { path: path.to_path_buf(), source: e }),
        Ok(text) if text.trim().is_empty() => Ok(None),
        Ok(text) => text
            .trim()
            .parse()
            .map(Some)
            .map_err(|_| StorageError::BadClusterId { path: path.to_path_buf(), text }),
    }
}

/// `gen_timestamp_string`'s `%Y%m%d%H%M%S`. The C++ BE uses local time; this uses UTC (the
/// reference container's local time is UTC).
fn timestamp() -> String {
    let secs = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0);
    let (days, rem) = (secs / 86_400, secs % 86_400);
    let (y, m, d) = civil_from_days(days as i64);
    format!("{y:04}{m:02}{d:02}{:02}{:02}{:02}", rem / 3600, rem % 3600 / 60, rem % 60)
}

/// `%Y%m%d%H%M%S` (UTC, as `timestamp` writes it) to seconds since the epoch.
fn parse_timestamp(s: &str) -> Option<i64> {
    if s.len() != 14 || !s.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let n = |r: std::ops::Range<usize>| s[r].parse::<i64>().ok();
    let (y, m, d, hh, mm, ss) = (n(0..4)?, n(4..6)?, n(6..8)?, n(8..10)?, n(10..12)?, n(12..14)?);
    Some(days_from_civil(y, m as u32, d as u32) * 86_400 + hh * 3600 + mm * 60 + ss)
}

/// (year, month, day) to days since 1970-01-01, Howard Hinnant's `days_from_civil`.
fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y.rem_euclid(400);
    let mp = i64::from((m + 9) % 12);
    let doy = (153 * mp + 2) / 5 + i64::from(d) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// Days since 1970-01-01 to (year, month, day), Howard Hinnant's `civil_from_days`.
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (yoe + era * 400 + i64::from(m <= 2), m, d)
}
