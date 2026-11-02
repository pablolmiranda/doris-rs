use std::path::PathBuf;

/// Errors produced while reading a segment file.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("segment file too short to hold a trailer: {len} bytes")]
    TooShort { len: usize },
    #[error("bad segment magic {0:?}, expected b\"D0R1\"")]
    BadMagic([u8; 4]),
    #[error("footer length {footer_len} does not fit in a {len}-byte file")]
    FooterOutOfBounds { footer_len: usize, len: usize },
    #[error("footer checksum mismatch: stored {expected:#010x}, computed {actual:#010x}")]
    ChecksumMismatch { expected: u32, actual: u32 },
    #[error("protobuf decode: {0}")]
    Proto(#[from] prost::DecodeError),
    #[error("{path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}
