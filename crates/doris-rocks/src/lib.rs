//! The subset of RocksDB 5.14.2's on-disk format the Doris BE uses for `<root>/meta` and
//! `<root>/stream_load`, in pure Rust: log files (write-ahead logs and manifests), write batches,
//! version edits, block-based table files, and a small database over them. Format constants cite
//! the RocksDB v5.14.2 source (`.cache/rocksdb-5.14.2`).

pub mod coding;
pub mod error;
pub mod log;
pub mod table;
pub mod version_edit;
pub mod write_batch;

pub use error::RocksError;
