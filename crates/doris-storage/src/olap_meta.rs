//! A root's tablet metadata store, `<root>/meta` (`be/src/storage/olap_meta.cpp`): a RocksDB
//! database with column families `default`, `doris` and `meta` (the last with a 4-byte fixed
//! prefix), of which only `meta` holds keys. Tablet headers are `TabletMetaPB` under
//! `tabletmeta_<tablet_id>_<schema_hash>` (`tablet_meta_manager.cpp`).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use doris_proto::doris::TabletMetaPb;
use doris_rocks::db::{Batch, CfOptions, Db, DbOptions};
use prost::Message;

use crate::error::StorageError;

/// `META_COLUMN_FAMILY` and its siblings (`olap_define.h:146-148`).
const CF: &str = "meta";
const HEADER_PREFIX: &str = "tabletmeta_";
/// `OLD_HEADER_PREFIX`: a header format the BE no longer reads.
const LEGACY_PREFIX: &str = "hdr_";
/// Keys that exist only once a tablet holds data (rowsets of committed transactions, pending
/// publishes, delete bitmaps, binlogs, partial updates, remote-data GC).
const DATA_PREFIXES: [&str; 7] = ["rst_", "ppi_", "dlb_", "binlog_", "pui_", "gc_", "tgc_"];

pub fn tablet_meta_key(tablet_id: i64, schema_hash: i32) -> String {
    format!("{HEADER_PREFIX}{tablet_id}_{schema_hash}")
}

pub(crate) fn options() -> DbOptions {
    let cf = |name: &str, prefix| CfOptions { name: name.into(), fixed_prefix_len: prefix };
    DbOptions { column_families: vec![cf("default", None), cf("doris", None), cf(CF, Some(4))], ..DbOptions::default() }
}

/// What a root's `meta/` holds, read without writing (for the checks that must leave a
/// refused root untouched).
#[derive(Debug, Default)]
pub(crate) struct Contents {
    pub keys: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl Contents {
    pub fn read(root: &Path) -> Result<Contents, StorageError> {
        let mut cfs = Db::read_only(&root.join("meta"))?;
        Ok(Contents { keys: cfs.remove(CF).unwrap_or_default() })
    }

    pub fn has_legacy_header(&self) -> bool {
        self.keys.keys().any(|k| k.starts_with(LEGACY_PREFIX.as_bytes()))
    }

    pub fn first_data_key(&self) -> Option<String> {
        self.keys
            .keys()
            .find(|k| DATA_PREFIXES.iter().any(|p| k.starts_with(p.as_bytes())))
            .map(|k| String::from_utf8_lossy(k).into_owned())
    }

    /// Every valid header, and each one the C++ BE would fail to load
    /// (`TabletManager::load_tablet_from_meta`).
    pub fn headers(&self) -> (Vec<TabletMetaPb>, Vec<Skipped>) {
        let (mut ok, mut skipped) = (Vec::new(), Vec::new());
        for (k, v) in self.keys.range(HEADER_PREFIX.as_bytes().to_vec()..) {
            let Some(rest) = k.strip_prefix(HEADER_PREFIX.as_bytes()) else { break };
            let key = String::from_utf8_lossy(k).into_owned();
            match check(rest, v) {
                Ok(meta) => ok.push(meta),
                Err(why) => {
                    let mut tablets = Vec::new();
                    if let Some((id, hash)) = std::str::from_utf8(rest).ok().and_then(|r| r.split_once('_')) {
                        if let (Ok(id), Ok(hash)) = (id.parse(), hash.parse()) {
                            tablets.push((id, hash));
                        }
                    }
                    let meta = TabletMetaPb::decode(v.as_slice()).ok();
                    if let Some(m) = &meta {
                        tablets.push((m.tablet_id(), m.schema_hash()));
                    }
                    skipped.push(Skipped { line: format!("{key}: {why}"), tablets, meta });
                }
            }
        }
        (ok, skipped)
    }
}

/// A header the C++ BE would fail to load: why, the tablets it may name (by its key and by its
/// contents), and its contents when they decode.
#[derive(Debug, Clone)]
pub(crate) struct Skipped {
    pub line: String,
    pub tablets: Vec<(i64, i32)>,
    pub meta: Option<TabletMetaPb>,
}

fn check(key_rest: &[u8], value: &[u8]) -> Result<TabletMetaPb, String> {
    let rest = std::str::from_utf8(key_rest).map_err(|_| "key is not text".to_string())?;
    let (id, hash) = rest.split_once('_').ok_or("key has no schema hash")?;
    let id: i64 = id.parse().map_err(|_| "bad tablet id in the key")?;
    let hash: i32 = hash.parse().map_err(|_| "bad schema hash in the key")?;
    let meta = TabletMetaPb::decode(value).map_err(|e| format!("bad header: {e}"))?;
    if meta.tablet_id() != id || meta.schema_hash() != hash {
        return Err(format!("header is for tablet {}_{}", meta.tablet_id(), meta.schema_hash()));
    }
    if meta.tablet_uid.as_ref().is_none_or(|u| u.hi == 0 && u.lo == 0) {
        return Err("tablet_uid is zero".into());
    }
    let running = meta.tablet_state() == doris_proto::doris::TabletStatePb::PbRunning;
    if running && meta.rs_metas.is_empty() {
        return Err("a running tablet with no versions".into());
    }
    Ok(meta)
}

/// The store, open for writing.
#[derive(Debug)]
pub struct OlapMeta {
    root: PathBuf,
    db: Db,
}

impl OlapMeta {
    pub fn open(root: &Path) -> Result<OlapMeta, StorageError> {
        Ok(OlapMeta { root: root.to_path_buf(), db: Db::open(&root.join("meta"), &options())? })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn save_tablet_meta(&mut self, meta: &TabletMetaPb, sync: bool) -> Result<(), StorageError> {
        let mut b = Batch::default();
        b.put(CF, tablet_meta_key(meta.tablet_id(), meta.schema_hash()).as_bytes(), &meta.encode_to_vec());
        Ok(self.db.write(b, sync)?)
    }

    pub fn remove_tablet_meta(&mut self, tablet_id: i64, schema_hash: i32, sync: bool) -> Result<(), StorageError> {
        let mut b = Batch::default();
        b.delete(CF, tablet_meta_key(tablet_id, schema_hash).as_bytes());
        Ok(self.db.write(b, sync)?)
    }

    /// One header as stored (`TabletMetaManager::get_meta`); `None` when the key is missing.
    pub fn get_tablet_meta(&self, tablet_id: i64, schema_hash: i32) -> Result<Option<TabletMetaPb>, String> {
        match self.db.get(CF, tablet_meta_key(tablet_id, schema_hash).as_bytes()) {
            None => Ok(None),
            Some(v) => TabletMetaPb::decode(v).map(Some).map_err(|e| e.to_string()),
        }
    }

    /// Every valid header, in key order.
    pub fn tablet_metas(&self) -> Result<Vec<TabletMetaPb>, StorageError> {
        let keys = self.db.scan_prefix(CF, HEADER_PREFIX.as_bytes()).into_iter().map(|(k, v)| (k.to_vec(), v.to_vec())).collect();
        Ok(Contents { keys }.headers().0)
    }

    pub fn close(self) -> Result<(), StorageError> {
        Ok(self.db.close()?)
    }

    /// Fault injection for tests: while on, every header write fails.
    #[doc(hidden)]
    pub fn inject_write_failure(&mut self, on: bool) {
        self.db.inject_write_failure(on);
    }
}
