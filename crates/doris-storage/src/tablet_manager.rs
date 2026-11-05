//! The tablets this BE holds, across its data directories: create, drop, partition visible
//! versions, and the report and stat forms the FE reads. Follows `TabletManager`
//! (`be/src/storage/tablet/tablet_manager.cpp`) and the agent callbacks that call it
//! (`be/src/agent/task_worker_pool.cpp`).

use std::collections::hash_map::RandomState;
use std::collections::BTreeMap;
use std::hash::{BuildHasher, Hasher};
use std::time::{SystemTime, UNIX_EPOCH};

use doris_proto::doris::{PUniqueId, TabletMetaPb, TabletStatePb};
use doris_rocks::db::{CfOptions, Db, DbOptions};
use doris_thrift::gen::agent_service::{TCreateTabletReq, TDropTabletReq};
use doris_thrift::gen::backend_service::TTabletStat;
use doris_thrift::gen::master_service::{TTablet, TTabletInfo};

use crate::data_dir::DataDir;
use crate::error::{IoContext, StorageError};
use crate::file_header::encode_file_header;
use crate::tablet::{build_tablet_meta, storage_medium, NewTablet};

/// Expiries for a trash sweep (`trash_file_expire_time_sec`, `snapshot_expire_time_sec`).
#[derive(Debug, Clone, Copy)]
pub struct SweepOptions {
    pub trash_expire_secs: i64,
    pub snapshot_expire_secs: i64,
}

/// How the tablet manager writes headers.
#[derive(Debug, Clone, Copy, Default)]
pub struct TabletOptions {
    /// `config::sync_tablet_meta`: sync the metadata store's log on every header change.
    pub sync_tablet_meta: bool,
}

#[derive(Debug)]
struct Tablet {
    meta: TabletMetaPb,
    dir: usize,
}

#[derive(Debug)]
pub struct TabletManager {
    dirs: Vec<DataDir>,
    tablets: BTreeMap<i64, Tablet>,
    /// Shut-down tablets (dropped, or SHUTDOWN at start) waiting for the sweep.
    shutdown: Vec<Tablet>,
    opts: TabletOptions,
    /// The stream load recorder's database on the first root (`StreamLoadRecorder`).
    _stream_load: Option<Db>,
    /// Partition id → visible version (`TabletManager::_partitions`; starts at 0, in memory only).
    partitions: BTreeMap<i64, i64>,
    report_version: i64,
    /// `RowsetIdGenerator`: a per-process random id and a counter.
    backend_uid: (u64, u64),
    next_rowset: u64,
    /// `_tablet_stat_list_cache`: filled by each tablet report, read by `get_tablet_stat`.
    stat_cache: Vec<TTabletStat>,
}

fn now_secs() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs() as i64).unwrap_or(0)
}

fn random_u64() -> u64 {
    let mut h = RandomState::new().build_hasher();
    h.write_u128(SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_nanos()).unwrap_or(0));
    h.finish()
}

impl TabletManager {
    /// Loads every tablet header from every root (`TabletManager::load_tablet_from_meta`): running
    /// tablets are held, SHUTDOWN ones wait for the sweep. Opens the stream load recorder's
    /// database on the first root, as `StorageEngine::_open` does.
    pub fn open(dirs: Vec<DataDir>, opts: TabletOptions) -> Result<TabletManager, StorageError> {
        let mut tablets = BTreeMap::new();
        let mut shutdown = Vec::new();
        let mut partitions = BTreeMap::new();
        for (i, dir) in dirs.iter().enumerate() {
            for meta in dir.meta().tablet_metas()? {
                if meta.tablet_state() == TabletStatePb::PbShutdown {
                    shutdown.push(Tablet { meta, dir: i });
                    continue;
                }
                partitions.insert(meta.partition_id(), 0);
                tablets.insert(meta.tablet_id(), Tablet { meta, dir: i });
            }
        }
        let stream_load = match dirs.first() {
            Some(d) => {
                let cf = |name: &str| CfOptions { name: name.into(), fixed_prefix_len: None };
                let o = DbOptions { column_families: vec![cf("default"), cf("meta")], ..DbOptions::default() };
                Some(Db::open(&d.root().join("stream_load"), &o)?)
            }
            None => None,
        };
        Ok(TabletManager {
            dirs,
            tablets,
            shutdown,
            opts,
            _stream_load: stream_load,
            partitions,
            // `s_report_version`: time(nullptr) * 100000 at start (task_worker_pool.cpp:116)
            report_version: now_secs() * 100_000,
            backend_uid: (random_u64(), random_u64()),
            next_rowset: 1,
            stat_cache: Vec::new(),
        })
    }

    /// Fault injection for tests: while on, every header write on every root fails.
    #[doc(hidden)]
    pub fn inject_write_failure(&mut self, on: bool) {
        for d in &mut self.dirs {
            d.meta_mut().inject_write_failure(on);
        }
    }

    /// Tablets shut down and waiting for the sweep, by id.
    pub fn shutdown_tablets(&self) -> Vec<i64> {
        let mut ids: Vec<i64> = self.shutdown.iter().map(|t| t.meta.tablet_id()).collect();
        ids.sort_unstable();
        ids
    }

    pub fn data_dirs(&self) -> &[DataDir] {
        &self.dirs
    }

    pub fn cluster_id(&self) -> Option<i32> {
        self.dirs.iter().find_map(DataDir::cluster_id)
    }

    /// Assigns the cluster id to every root, as `StorageEngine::set_cluster_id` does.
    pub fn set_cluster_id(&mut self, id: i32) -> Result<(), StorageError> {
        for dir in &self.dirs {
            if let Some(current) = dir.cluster_id().filter(|c| *c != id) {
                return Err(StorageError::ClusterIdAssigned { current, new: id });
            }
        }
        self.dirs.iter_mut().try_for_each(|d| d.set_cluster_id(id))
    }

    pub fn report_version(&self) -> i64 {
        self.report_version
    }

    /// `increase_report_version`.
    pub fn increase_report_version(&mut self) {
        self.report_version += 1;
    }

    /// `TabletManager::create_tablet` plus the finish info `create_tablet_callback` sends.
    /// A tablet that already exists is not created again (an FE retry).
    pub fn create_tablet(&mut self, req: &TCreateTabletReq) -> Result<TTabletInfo, StorageError> {
        if req.base_tablet_id.unwrap_or(0) > 0 {
            return Err(StorageError::Unsupported { what: "creating a tablet from a base tablet (schema change)".into() });
        }
        if !self.tablets.contains_key(&req.tablet_id) {
            let dir = (0..self.dirs.len())
                .min_by_key(|i| self.tablets.values().filter(|t| t.dir == *i).count())
                .ok_or(StorageError::NoDataDir)?;
            let new = NewTablet {
                shard_id: self.dirs[dir].next_shard() as i32,
                tablet_uid: PUniqueId { hi: random_u64() as i64, lo: random_u64() as i64 },
                rowset_id_v2: self.next_rowset_id(),
                creation_time: now_secs(),
            };
            let meta = build_tablet_meta(req, &new);
            let path = self.dirs[dir].tablet_dir(new.shard_id as u64, req.tablet_id, req.tablet_schema.schema_hash);
            std::fs::create_dir_all(&path).at(&path)?;
            let sync = self.opts.sync_tablet_meta;
            if let Err(e) = self.dirs[dir].meta_mut().save_tablet_meta(&meta, sync) {
                // no header, no tablet: take the directory back out (its parent too, if empty)
                let _ = std::fs::remove_dir_all(&path);
                if let Some(parent) = path.parent() {
                    let _ = std::fs::remove_dir(parent);
                }
                return Err(e);
            }
            self.partitions.entry(meta.partition_id()).or_insert(0);
            self.tablets.insert(req.tablet_id, Tablet { meta, dir });
        }
        // create_tablet_callback increases it on every OK, an existing tablet's included
        self.increase_report_version();
        let t = &self.tablets[&req.tablet_id];
        Ok(TTabletInfo {
            path_hash: Some(self.dirs[t.dir].path_hash()),
            replica_id: Some(t.meta.replica_id()),
            ..tablet_info(req.tablet_id, t.meta.schema_hash(), req.version.unwrap_or(0), 0, 0)
        })
    }

    /// `RowsetId::init(2, ...)`: version 2 in the top byte, a counter, then the backend uid.
    fn next_rowset_id(&mut self) -> String {
        let hi = (2u64 << 56) | (self.next_rowset & ((1 << 56) - 1));
        self.next_rowset += 1;
        format!("{hi:016x}{:016x}{:016x}", self.backend_uid.0, self.backend_uid.1)
    }

    /// `drop_tablet_callback` / `TabletManager::_drop_tablet`: the header is saved as SHUTDOWN, the
    /// tablet leaves the tablet map (and so reports and stats), and it waits for the sweep, which
    /// moves its directory to trash.
    pub fn drop_tablet(&mut self, req: &TDropTabletReq) -> Result<(), StorageError> {
        let Some(tablet) = self.tablets.get(&req.tablet_id) else {
            return Err(StorageError::TabletNotFound { tablet_id: req.tablet_id });
        };
        let replica_id = req.replica_id.unwrap_or(0);
        if replica_id != 0 && tablet.meta.replica_id() != replica_id {
            return Err(StorageError::ReplicaMismatch { current: tablet.meta.replica_id(), requested: replica_id });
        }
        let mut meta = tablet.meta.clone();
        meta.set_tablet_state(TabletStatePb::PbShutdown);
        let sync = self.opts.sync_tablet_meta;
        // disk first: if it fails, the tablet stays known and a retried DROP can finish it
        self.dirs[tablet.dir].meta_mut().save_tablet_meta(&meta, sync)?;
        let mut tablet = self.tablets.remove(&req.tablet_id).expect("present");
        tablet.meta = meta;
        let pid = tablet.meta.partition_id();
        if !self.tablets.values().any(|t| t.meta.partition_id() == pid) {
            self.partitions.remove(&pid);
        }
        self.shutdown.push(tablet);
        Ok(())
    }

    /// `StorageEngine::start_trash_sweep`: expired `snapshot/` and `trash/` entries on every
    /// root, then the shut-down tablets. Returns how many shut-down tablets were swept.
    pub fn start_trash_sweep(&mut self, opts: SweepOptions) -> Result<usize, StorageError> {
        for d in &self.dirs {
            d.sweep_expired(opts.trash_expire_secs, opts.snapshot_expire_secs)?;
        }
        self.sweep_shutdown()
    }

    /// `TabletManager::start_trash_sweep`: each shut-down tablet whose move succeeds leaves the
    /// queue; one that fails stays for the next sweep. Returns how many left.
    pub fn sweep_shutdown(&mut self) -> Result<usize, StorageError> {
        let queue = std::mem::take(&mut self.shutdown);
        let mut swept = 0;
        for t in queue {
            if self.move_tablet_to_trash(&t).unwrap_or(false) {
                swept += 1;
            } else {
                self.shutdown.push(t);
            }
        }
        Ok(swept)
    }

    /// `TabletManager::_move_tablet_to_trash`; `Ok(true)` when the tablet can leave the queue.
    fn move_tablet_to_trash(&mut self, t: &Tablet) -> Result<bool, StorageError> {
        let (id, hash) = (t.meta.tablet_id(), t.meta.schema_hash());
        let path = self.dirs[t.dir].tablet_dir(t.meta.shard_id() as u64, id, hash);
        // a tablet created again with the same id: only an old directory elsewhere goes
        if let Some(live) = self.tablets.get(&id) {
            if live.meta.schema_hash() == hash && live.dir == t.dir {
                let live_path = self.dirs[t.dir].tablet_dir(live.meta.shard_id() as u64, id, hash);
                if live_path != path && path.is_dir() {
                    self.dirs[t.dir].move_to_trash(&path)?;
                }
                return Ok(true);
            }
        }
        let sync = self.opts.sync_tablet_meta;
        let dir = &mut self.dirs[t.dir];
        match dir.meta().get_tablet_meta(id, hash) {
            Ok(Some(stored)) => {
                if stored.tablet_state() != TabletStatePb::PbShutdown || stored.tablet_uid != t.meta.tablet_uid {
                    return Ok(true);
                }
                if path.is_dir() {
                    let hdr = path.join(format!("{id}.hdr"));
                    std::fs::write(&hdr, encode_file_header(&t.meta)).at(&hdr)?;
                    dir.move_to_trash(&path)?;
                }
                dir.meta_mut().remove_tablet_meta(id, hash, sync)?;
                Ok(true)
            }
            Ok(None) => {
                if path.is_dir() {
                    std::fs::remove_dir_all(&path).at(&path)?;
                    if let Some(parent) = path.parent() {
                        let _ = std::fs::remove_dir(parent);
                    }
                }
                Ok(true)
            }
            Err(_) => Ok(false),
        }
    }

    /// `update_partitions_visible_version`: known partitions only, never backwards.
    pub fn update_visible_version(&mut self, partition_id: i64, version: i64) {
        if let Some(v) = self.partitions.get_mut(&partition_id) {
            *v = (*v).max(version);
        }
    }

    pub fn partitions_version(&self) -> BTreeMap<i64, i64> {
        self.partitions.clone()
    }

    /// `build_all_report_tablets_info`: one `TTablet` per tablet with its report info
    /// (`Tablet::build_tablet_report_info`), for tablets holding only their initial rowset.
    pub fn tablet_infos(&self) -> BTreeMap<i64, TTablet> {
        self.tablets.iter().map(|(id, t)| (*id, TTablet { tablet_infos: vec![self.report_info(t)] })).collect()
    }

    fn report_info(&self, t: &Tablet) -> TTabletInfo {
        let version = t.meta.rs_metas.iter().map(|r| r.end_version()).max().unwrap_or(0);
        let versions = t.meta.rs_metas.len() as i64;
        let rows: i64 = t.meta.rs_metas.iter().map(|r| r.num_rows()).sum();
        let size: i64 = t.meta.rs_metas.iter().map(|r| r.total_disk_size()).sum();
        TTabletInfo {
            storage_medium: Some(storage_medium()),
            total_version_count: Some(versions),
            path_hash: Some(self.dirs[t.dir].path_hash()),
            partition_id: Some(t.meta.partition_id()),
            is_in_memory: Some(false),
            replica_id: Some(t.meta.replica_id()),
            remote_data_size: Some(0),
            visible_version_count: Some(versions),
            local_index_size: Some(0),
            local_segment_size: Some(size),
            remote_index_size: Some(0),
            remote_segment_size: Some(0),
            ..tablet_info(t.meta.tablet_id(), t.meta.schema_hash(), version, rows, size)
        }
    }

    /// `build_all_report_tablets_info`: the tablets for a tablet report, refreshing the stat
    /// cache `get_tablet_stat` answers from.
    pub fn report_tablets(&mut self) -> BTreeMap<i64, TTablet> {
        let tablets = self.tablet_infos();
        self.stat_cache = tablets
            .values()
            .flat_map(|t| &t.tablet_infos)
            .map(|i| TTabletStat {
                tablet_id: i.tablet_id,
                data_size: Some(i.data_size),
                row_count: Some(i.row_count),
                total_version_count: i.total_version_count,
                remote_data_size: i.remote_data_size,
                visible_version_count: i.visible_version_count,
                visible_version: Some(i.version),
                local_index_size: i.local_index_size,
                local_segment_size: i.local_segment_size,
                remote_index_size: i.remote_index_size,
                remote_segment_size: i.remote_segment_size,
            })
            .collect();
        tablets
    }

    /// `TabletManager::get_tablet_stat`: the stats as of the last tablet report.
    pub fn tablet_stats(&self) -> Vec<TTabletStat> {
        self.stat_cache.clone()
    }

    /// Bytes of tablet data on a root (`DataDir` local used capacity).
    pub fn data_used(&self, dir: usize) -> i64 {
        self.tablets.values().filter(|t| t.dir == dir).flat_map(|t| &t.meta.rs_metas).map(|r| r.total_disk_size()).sum()
    }
}

/// A `TTabletInfo` with its required fields; the optional ones the C++ BE always writes because
/// they have IDL defaults (`local_index_size` and the other sizes, `= 0`) are set to 0.
pub fn tablet_info(tablet_id: i64, schema_hash: i32, version: i64, row_count: i64, data_size: i64) -> TTabletInfo {
    TTabletInfo {
        tablet_id,
        schema_hash,
        version,
        version_hash: 0,
        row_count,
        data_size,
        storage_medium: None,
        transaction_ids: None,
        total_version_count: None,
        path_hash: None,
        version_miss: None,
        used: None,
        partition_id: None,
        is_in_memory: None,
        replica_id: None,
        remote_data_size: None,
        cooldown_term: None,
        cooldown_meta_id: None,
        visible_version_count: None,
        local_index_size: Some(0),
        local_segment_size: Some(0),
        remote_index_size: Some(0),
        remote_segment_size: Some(0),
        is_persistent: None,
    }
}
