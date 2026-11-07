//! The C++ BE's loaded fixture (`fixtures/cpp-loaded`), shared by the rowset and transaction
//! tests.
#![allow(dead_code)]
use std::path::{Path, PathBuf};

use doris_proto::doris::TabletMetaPb;
use doris_storage::{DataDir, TabletManager, TabletOptions};

pub fn fixture() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/cpp-loaded")
}

pub fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for e in std::fs::read_dir(from).unwrap() {
        let e = e.unwrap();
        let dst = to.join(e.file_name());
        if e.file_type().unwrap().is_dir() {
            copy_dir(&e.path(), &dst);
        } else {
            std::fs::copy(e.path(), dst).unwrap();
        }
    }
}

/// A root holding a copy of the fixture's databases and tablet directories.
pub fn loaded_root() -> tempfile::TempDir {
    let tmp = tempfile::tempdir().unwrap();
    for d in ["meta", "stream_load", "data"] {
        copy_dir(&fixture().join(d), &tmp.path().join(d));
    }
    tmp
}

pub fn manager(root: &Path) -> TabletManager {
    TabletManager::open(vec![DataDir::open(root, "127.0.0.1").unwrap()], TabletOptions::default()).unwrap()
}

/// The fixture's headers as the C++ BE served them (rowsets included).
pub fn headers() -> Vec<TabletMetaPb> {
    let mut out: Vec<TabletMetaPb> = std::fs::read_dir(fixture().join("headers"))
        .unwrap()
        .map(|e| serde_json::from_str(&std::fs::read_to_string(e.unwrap().path()).unwrap()).unwrap())
        .collect();
    out.sort_by_key(|m| m.tablet_id());
    out
}

