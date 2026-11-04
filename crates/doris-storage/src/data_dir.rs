//! One storage root, laid out as the C++ BE lays it out (`be/src/storage/olap_define.h`,
//! `be/src/storage/data_dir.cpp`): `cluster_id`, `data/<shard>/<tablet_id>/<schema_hash>/` with
//! shards 0..1023 created at start, `trash/<time>.<n>/<tablet_id>/<schema_hash>/` for dropped
//! tablets, and the other folders the C++ BE creates at start. `meta/` is never written.

use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::error::{IoContext, StorageError};
use crate::header_store::{HeaderStore, HEADER_DIR};
use crate::path_hash::hash_of_path;

/// `DataDir::MAX_SHARD_NUM`.
pub const MAX_SHARD_NUM: u64 = 1024;
const CLUSTER_ID: &str = "cluster_id";
/// Folders the C++ BE creates at start (taken from the reference's layout fixture).
const START_DIRS: [&str; 8] = ["data", "error_log", "spill", "spill_gc", "stream_load", "tmp", "wal", "wal/tmp"];
const DEPLOY_MODE: &str = "deploy_mode";

/// `delete_counter` in `DataDir::move_to_trash`: process-wide, so two moves in one second differ.
static TRASH_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Debug)]
pub struct DataDir {
    root: PathBuf,
    path_hash: i64,
    cluster_id: Option<i32>,
    next_shard: u64,
    headers: HeaderStore,
}

impl DataDir {
    /// Opens a root: refuses one holding a C++ BE's data, creates the start folders, and clears
    /// half-created tablets (a header without its directory, a directory without its header).
    pub fn open(root: &Path, host: &str) -> Result<DataDir, StorageError> {
        if !root.is_dir() {
            return Err(StorageError::MissingRoot { root: root.to_path_buf() });
        }
        refuse_foreign(root)?;
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
        let headers = HeaderStore::open(root)?;
        let cluster_id = read_cluster_id(&root.join(CLUSTER_ID))?;
        let path_hash = hash_of_path(host, root.to_str().unwrap_or_default()) as i64;
        let dir = DataDir { root: root.to_path_buf(), path_hash, cluster_id, next_shard: 0, headers };
        dir.clear_half_created()?;
        Ok(dir)
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

    /// `DataDir::move_to_trash`: `trash/<YYYYmmddHHMMSS>.<n>/<tablet_id>/<schema_hash>`, then the
    /// emptied `<tablet_id>` parent is removed.
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

    pub fn headers(&self) -> &HeaderStore {
        &self.headers
    }

    fn clear_half_created(&self) -> Result<(), StorageError> {
        let mut known = HashSet::new();
        for meta in self.headers.load_all()? {
            let (id, hash) = (meta.tablet_id(), meta.schema_hash());
            if self.tablet_dir(meta.shard_id() as u64, id, hash).is_dir() {
                known.insert((id, hash));
            } else {
                self.headers.remove(id, hash)?;
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

fn refuse_foreign(root: &Path) -> Result<(), StorageError> {
    let foreign = |reason: &str| StorageError::ForeignRoot { root: root.to_path_buf(), reason: reason.to_string() };
    if root.join("meta").exists() {
        return Err(foreign("it has a meta/ directory"));
    }
    if !root.join(HEADER_DIR).exists() && !tablet_dirs(root)?.is_empty() {
        return Err(foreign("it has tablet directories under data/ that doris-rs did not write"));
    }
    Ok(())
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
