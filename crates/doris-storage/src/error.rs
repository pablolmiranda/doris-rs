use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("{path}: {source}")]
    Io { path: PathBuf, source: std::io::Error },
    #[error("storage root {root} does not exist")]
    MissingRoot { root: PathBuf },
    #[error(
        "storage root {root} holds a C++ BE's data ({reason}); doris-rs cannot use it until it \
         reads the C++ BE's tablet metadata store (the meta store milestone)"
    )]
    ForeignRoot { root: PathBuf, reason: String },
    /// The C++ BE's message for this case (`DataDir::set_cluster_id`).
    #[error("going to set cluster id to already assigned store")]
    ClusterIdAssigned { current: i32, new: i32 },
    #[error("{path}: not a cluster id: {text:?}")]
    BadClusterId { path: PathBuf, text: String },
    #[error("{path}: bad tablet header: {source}")]
    BadHeader { path: PathBuf, source: prost::DecodeError },
}

pub(crate) trait IoContext<T> {
    fn at(self, path: &std::path::Path) -> Result<T, StorageError>;
}

impl<T> IoContext<T> for std::io::Result<T> {
    fn at(self, path: &std::path::Path) -> Result<T, StorageError> {
        self.map_err(|source| StorageError::Io { path: path.to_path_buf(), source })
    }
}
