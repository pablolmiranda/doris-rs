//! The transactions a BE holds between a load's commit and its publish (`TxnManager`): per
//! (partition, transaction), each tablet's load id and pending rowset. Only the rowsets are
//! persisted (as COMMITTED `rst_` keys); the map is rebuilt from them at start.
use std::collections::BTreeMap;

use doris_proto::doris::{PUniqueId, RowsetMetaPb};

#[derive(Debug, Default)]
pub(crate) struct Txns {
    map: BTreeMap<(i64, i64), BTreeMap<i64, (PUniqueId, RowsetMetaPb)>>,
}

impl Txns {
    pub fn get(&self, partition_id: i64, txn_id: i64, tablet_id: i64) -> Option<&(PUniqueId, RowsetMetaPb)> {
        self.map.get(&(partition_id, txn_id))?.get(&tablet_id)
    }

    pub fn insert(&mut self, partition_id: i64, txn_id: i64, tablet_id: i64, load_id: PUniqueId, rowset: RowsetMetaPb) {
        self.map.entry((partition_id, txn_id)).or_default().insert(tablet_id, (load_id, rowset));
    }

    /// Takes a transaction's tablets in one partition out of the map.
    pub fn take(&mut self, partition_id: i64, txn_id: i64) -> BTreeMap<i64, (PUniqueId, RowsetMetaPb)> {
        self.map.remove(&(partition_id, txn_id)).unwrap_or_default()
    }

    /// The partitions a transaction has tablets in.
    pub fn partitions(&self, txn_id: i64) -> Vec<i64> {
        self.map.keys().filter(|(_, t)| *t == txn_id).map(|(p, _)| *p).collect()
    }
}

/// What a publish did (`EnginePublishVersionTask`): the partition's tablets that hold the
/// version, those that do not, and the rows each tablet's rowset brought (non-zero only).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PublishResult {
    pub succ: Vec<i64>,
    pub errors: Vec<i64>,
    pub delta_rows: BTreeMap<i64, i64>,
}
