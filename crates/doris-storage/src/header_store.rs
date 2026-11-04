//! Tablet headers (`TabletMetaPB`, protobuf bytes as the C++ BE serializes them), one file per
//! tablet named after the C++ BE's key, `tabletmeta_<tablet_id>_<schema_hash>`, in
//! `<root>/doris-rs-meta/`. Temporary: the meta store milestone moves headers into `<root>/meta`
//! in the C++ BE's format and deletes this store.

use std::fs;
use std::path::{Path, PathBuf};

use doris_proto::doris::TabletMetaPb;
use prost::Message;

use crate::error::{IoContext, StorageError};

pub const HEADER_DIR: &str = "doris-rs-meta";
const PREFIX: &str = "tabletmeta_";

#[derive(Debug)]
pub struct HeaderStore {
    dir: PathBuf,
}

impl HeaderStore {
    pub(crate) fn open(root: &Path) -> Result<HeaderStore, StorageError> {
        let dir = root.join(HEADER_DIR);
        fs::create_dir_all(&dir).at(&dir)?;
        Ok(HeaderStore { dir })
    }

    fn path(&self, tablet_id: i64, schema_hash: i32) -> PathBuf {
        self.dir.join(format!("{PREFIX}{tablet_id}_{schema_hash}"))
    }

    /// Writes the header to a temporary name, then renames it, so a crash leaves the old header
    /// or the new one.
    pub fn put(&self, tablet_id: i64, schema_hash: i32, meta: &TabletMetaPb) -> Result<(), StorageError> {
        let path = self.path(tablet_id, schema_hash);
        let tmp = path.with_extension("tmp");
        fs::write(&tmp, meta.encode_to_vec()).at(&tmp)?;
        fs::File::open(&tmp).and_then(|f| f.sync_all()).at(&tmp)?;
        fs::rename(&tmp, &path).at(&path)
    }

    pub fn remove(&self, tablet_id: i64, schema_hash: i32) -> Result<(), StorageError> {
        let path = self.path(tablet_id, schema_hash);
        match fs::remove_file(&path) {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            r => r.at(&path),
        }
    }

    /// Every header, ordered by tablet id and schema hash.
    pub fn load_all(&self) -> Result<Vec<TabletMetaPb>, StorageError> {
        let mut out = Vec::new();
        for entry in fs::read_dir(&self.dir).at(&self.dir)? {
            let path = entry.at(&self.dir)?.path();
            let name = path.file_name().and_then(|n| n.to_str()).unwrap_or_default();
            if !name.starts_with(PREFIX) || name.ends_with(".tmp") {
                continue;
            }
            let bytes = fs::read(&path).at(&path)?;
            let meta = TabletMetaPb::decode(bytes.as_slice())
                .map_err(|source| StorageError::BadHeader { path: path.clone(), source })?;
            out.push(meta);
        }
        out.sort_by_key(|m| (m.tablet_id(), m.schema_hash()));
        Ok(out)
    }
}
