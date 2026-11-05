use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("{path}: {source}")]
    Io { path: PathBuf, source: std::io::Error },
    #[error("storage root {root} does not exist")]
    MissingRoot { root: PathBuf },
    #[error(
        "storage root {root} holds data doris-rs cannot keep yet ({}): merge-on-write, binlog or remote-storage keys, or a tablet directory with files and no valid header",
        match (tablet_id, key) { (Some(t), _) => format!("tablet {t}"), (_, Some(k)) => format!("key {k}"), _ => "rowsets".into() }
    )]
    DataRoot { root: PathBuf, tablet_id: Option<i64>, key: Option<String> },
    /// `DataDir::_check_incompatible_old_format_tablet`.
    #[error("storage root {root} holds tablet headers in the old hdr_ format, which the BE no longer reads")]
    LegacyHeader { root: PathBuf },
    /// The C++ BE's message for this case (`DataDir::set_cluster_id`).
    #[error("going to set cluster id to already assigned store")]
    ClusterIdAssigned { current: i32, new: i32 },
    #[error("{path}: not a cluster id: {text:?}")]
    BadClusterId { path: PathBuf, text: String },
    /// `drop_tablet_callback`'s `Status::NotFound`.
    #[error("could not find tablet {tablet_id}")]
    TabletNotFound { tablet_id: i64 },
    /// `TabletManager::_drop_tablet`'s `Status::Aborted`.
    #[error("replica_id not match({current} vs {requested})")]
    ReplicaMismatch { current: i64, requested: i64 },
    /// `TabletMeta::add_rs_meta`: another rowset already holds the version.
    #[error("tablet {tablet_id} already has a rowset at version {version}")]
    VersionExists { tablet_id: i64, version: i64 },
    #[error("doris-rs does not support {what} yet")]
    Unsupported { what: String },
    #[error("no storage root is configured")]
    NoDataDir,
    #[error("{path}: bad tablet header: {source}")]
    BadHeader { path: PathBuf, source: prost::DecodeError },
    #[error(transparent)]
    Rocks(#[from] doris_rocks::RocksError),
}

pub(crate) trait IoContext<T> {
    fn at(self, path: &std::path::Path) -> Result<T, StorageError>;
}

impl<T> IoContext<T> for std::io::Result<T> {
    fn at(self, path: &std::path::Path) -> Result<T, StorageError> {
        self.map_err(|source| StorageError::Io { path: path.to_path_buf(), source })
    }
}
