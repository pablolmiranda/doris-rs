//! The wire golden corpus (`oracle/wire/corpus`): every FE↔BE RPC message captured from the
//! reference, one file each, described by `manifest.json`. Integrity (sizes and sha256) is
//! checked by the oracle's Python tests; this crate's tests check the Rust codecs against it.
use std::path::{Path, PathBuf};

pub mod canonical;

use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Manifest {
    pub corpus_version: u32,
    pub reference: Reference,
    pub capture: Capture,
    messages: Vec<MessageEntry>,
    #[serde(skip)]
    root: PathBuf,
}

#[derive(Debug, Deserialize)]
pub struct Reference {
    pub image: String,
    pub version: String,
    pub src_tag: String,
}

#[derive(Debug, Deserialize)]
pub struct Capture {
    /// File name of the (untracked) pcap the messages were extracted from.
    pub pcap: String,
    pub sha256: String,
    /// TCP connections the messages came from.
    pub streams: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Protocol {
    /// Strict, unframed `TBinaryProtocol`; a file holds one whole message, header included.
    Thrift,
    /// One gRPC message payload (protobuf), without the 5-byte gRPC message prefix.
    Grpc,
}

/// Direction of a message: Thrift message types, or gRPC request and response.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Kind {
    Call,
    Reply,
    Exception,
    Oneway,
    Request,
    Response,
}

#[derive(Debug, Deserialize)]
pub struct MessageEntry {
    /// Path relative to the corpus directory.
    pub file: PathBuf,
    pub protocol: Protocol,
    /// Server port of the connection.
    pub port: u16,
    /// IDL or proto service name, such as `FrontendService` or `PBackendService`.
    pub service: String,
    pub method: String,
    pub kind: Kind,
    /// Thrift sequence id.
    pub seqid: Option<i32>,
    /// HTTP/2 stream id of a gRPC message.
    pub h2_stream: Option<u32>,
    pub tcp_stream: u32,
    /// pcap frame the message starts in.
    pub frame: u64,
    /// gRPC compressed flag.
    pub compressed: Option<bool>,
    pub bytes: u64,
    pub sha256: String,
}

impl Manifest {
    /// Directory holding `manifest.json`, relative to this crate in the workspace.
    pub fn corpus_dir() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../oracle/wire/corpus")
    }

    /// Load the manifest; panics with a clear message if the corpus is missing.
    pub fn load() -> Manifest {
        let root = Manifest::corpus_dir();
        let path = root.join("manifest.json");
        let text = std::fs::read_to_string(&path).unwrap_or_else(|e| {
            panic!(
                "{}: {e}; run `make wire-extract` in oracle/",
                path.display()
            )
        });
        let mut m: Manifest = serde_json::from_str(&text).expect("manifest.json is valid");
        m.root = root;
        m
    }

    /// Messages in capture order.
    pub fn messages(&self) -> &[MessageEntry] {
        &self.messages
    }

    /// The bytes of one message; panics if the file is missing or its size disagrees with the
    /// manifest.
    pub fn read(&self, entry: &MessageEntry) -> Vec<u8> {
        let p = self.root.join(&entry.file);
        let bytes = std::fs::read(&p).unwrap_or_else(|e| panic!("{}: {e}", p.display()));
        assert_eq!(
            bytes.len() as u64,
            entry.bytes,
            "{}: size differs from manifest",
            p.display()
        );
        bytes
    }
}
