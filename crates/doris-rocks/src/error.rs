use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
pub enum RocksError {
    #[error("{}: {source}", path.display())]
    Io { path: PathBuf, source: std::io::Error },
    #[error("{}: corrupt: {what}", path.display())]
    Corrupt { path: PathBuf, what: String },
    #[error("{}: not supported: {what}", path.display())]
    Unsupported { path: PathBuf, what: String },
    #[error("{} is locked by another process", path.display())]
    Locked { path: PathBuf },
}

pub(crate) trait IoContext<T> {
    fn at(self, path: &std::path::Path) -> Result<T, RocksError>;
}

impl<T> IoContext<T> for std::io::Result<T> {
    fn at(self, path: &std::path::Path) -> Result<T, RocksError> {
        self.map_err(|source| RocksError::Io { path: path.to_path_buf(), source })
    }
}
