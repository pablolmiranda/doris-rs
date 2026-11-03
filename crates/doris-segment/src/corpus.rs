//! Access to the golden corpus manifest (`oracle/corpus/manifest.json`) for tests.
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Manifest {
    pub reference: Reference,
    pub tables: BTreeMap<String, TableEntry>,
    #[serde(skip)]
    root: PathBuf,
}

#[derive(Debug, Deserialize)]
pub struct Reference {
    pub image: String,
    pub version: String,
    pub commit: String,
}

#[derive(Debug, Deserialize)]
pub struct TableEntry {
    pub tablet_id: u64,
    pub schema_hash: u64,
    pub rows: u64,
    pub files: Vec<FileEntry>,
}

#[derive(Debug, Deserialize)]
pub struct FileEntry {
    pub path: PathBuf,
    pub bytes: u64,
    pub sha256: String,
}

impl Manifest {
    /// Directory holding `manifest.json`, relative to this crate in the workspace.
    pub fn corpus_dir() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../oracle/corpus")
    }

    /// Load the manifest; panics with a clear message if the corpus has not been built.
    pub fn load() -> Manifest {
        let root = Manifest::corpus_dir();
        let path = root.join("manifest.json");
        let text = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("{}: {e}; run `make corpus` in oracle/", path.display()));
        let mut m: Manifest = serde_json::from_str(&text).expect("manifest.json is valid");
        m.root = root;
        m
    }

    /// Absolute path of a manifest-relative file path.
    pub fn resolve(&self, rel: &Path) -> PathBuf {
        self.root.join(rel)
    }

    /// Read a corpus file into memory.
    pub fn read(&self, rel: &Path) -> Vec<u8> {
        let p = self.resolve(rel);
        std::fs::read(&p).unwrap_or_else(|e| panic!("{}: {e}", p.display()))
    }
}
