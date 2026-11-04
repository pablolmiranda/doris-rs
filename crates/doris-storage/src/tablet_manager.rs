//! The tablets this BE holds, across its data directories: create, drop, partition visible
//! versions, and the report and stat forms the FE reads. Follows `TabletManager`
//! (`be/src/storage/tablet/tablet_manager.cpp`) and the agent callbacks that call it
//! (`be/src/agent/task_worker_pool.cpp`).

use std::collections::hash_map::RandomState;
use std::collections::BTreeMap;
use std::hash::{BuildHasher, Hasher};
use std::time::{SystemTime, UNIX_EPOCH};

use doris_proto::doris::{PUniqueId, TabletMetaPb};
use doris_thrift::gen::agent_service::{TCreateTabletReq, TDropTabletReq};
use doris_thrift::gen::backend_service::TTabletStat;
use doris_thrift::gen::master_service::{TTablet, TTabletInfo};

use crate::data_dir::DataDir;
use crate::error::{IoContext, StorageError};
use crate::tablet::{build_tablet_meta, storage_medium, NewTablet};

#[derive(Debug)]
struct Tablet {
    meta: TabletMetaPb,
    dir: usize,
}

#[derive(Debug)]
pub struct TabletManager {
    dirs: Vec<DataDir>,
    tablets: BTreeMap<i64, Tablet>,
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
    /// Loads every tablet header from every root.
    pub fn open(dirs: Vec<DataDir>) -> Result<TabletManager, StorageError> {
        let mut tablets = BTreeMap::new();
        let mut partitions = BTreeMap::new();
        for (i, dir) in dirs.iter().enumerate() {
            for meta in dir.headers().load_all()? {
                partitions.insert(meta.partition_id(), 0);
                tablets.insert(meta.tablet_id(), Tablet { meta, dir: i });
            }
        }
        Ok(TabletManager {
            dirs,
            tablets,
            partitions,
            // `s_report_version`: time(nullptr) * 10000 at start (task_worker_pool.cpp)
            report_version: now_secs() * 10_000,
            backend_uid: (random_u64(), random_u64()),
            next_rowset: 1,
            stat_cache: Vec::new(),
        })
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
            self.dirs[dir].headers().put(req.tablet_id, req.tablet_schema.schema_hash, &meta)?;
            self.partitions.entry(meta.partition_id()).or_insert(0);
            self.tablets.insert(req.tablet_id, Tablet { meta, dir });
            self.increase_report_version();
        }
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

    /// `drop_tablet_callback` / `TabletManager::_drop_tablet`. The C++ BE marks the tablet
    /// shut down and a later sweep moves its directory to `trash/`; this moves it at once.
    pub fn drop_tablet(&mut self, req: &TDropTabletReq) -> Result<(), StorageError> {
        let Some(tablet) = self.tablets.get(&req.tablet_id) else {
            return Err(StorageError::TabletNotFound { tablet_id: req.tablet_id });
        };
        let replica_id = req.replica_id.unwrap_or(0);
        if replica_id != 0 && tablet.meta.replica_id() != replica_id {
            return Err(StorageError::ReplicaMismatch { current: tablet.meta.replica_id(), requested: replica_id });
        }
        let tablet = self.tablets.remove(&req.tablet_id).expect("present");
        let dir = &self.dirs[tablet.dir];
        let path = dir.tablet_dir(tablet.meta.shard_id() as u64, tablet.meta.tablet_id(), tablet.meta.schema_hash());
        if path.is_dir() {
            dir.move_to_trash(&path)?;
        }
        dir.headers().remove(tablet.meta.tablet_id(), tablet.meta.schema_hash())?;
        let pid = tablet.meta.partition_id();
        if !self.tablets.values().any(|t| t.meta.partition_id() == pid) {
            self.partitions.remove(&pid);
        }
        Ok(())
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
