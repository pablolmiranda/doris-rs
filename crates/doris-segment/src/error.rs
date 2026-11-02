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
    #[error("page too small: {size} bytes (need footer length and checksum)")]
    PageTooSmall { size: usize },
    #[error("page checksum mismatch: stored {expected:#010x}, computed {actual:#010x}")]
    PageChecksumMismatch { expected: u32, actual: u32 },
    #[error("page decompressed to {actual} bytes but its footer declares {expected}")]
    PageSizeMismatch { expected: usize, actual: usize },
    #[error("page at offset {offset} with size {size} exceeds the {len}-byte segment")]
    PageOutOfBounds { offset: u64, size: u32, len: usize },
    #[error("unsupported page compression type {0}")]
    UnsupportedCompression(i32),
    #[error("column {column}: unsupported encoding {encoding}")]
    UnsupportedEncoding { column: u32, encoding: i32 },
    #[error("column {column}: unsupported storage field type {field_type}")]
    UnsupportedType { column: u32, field_type: i32 },
    #[error("no column with unique id {0} in this segment")]
    ColumnNotFound(u32),
    #[error("column {0} has no ordinal index")]
    NoOrdinalIndex(u32),
    #[error("decompression failed: {0}")]
    Decompress(String),
    #[error("corrupt data: {0}")]
    Corrupt(String),
    #[error("protobuf decode: {0}")]
    Proto(#[from] prost::DecodeError),
    #[error("{path}: {source}")]
    InFile {
        path: PathBuf,
        #[source]
        source: Box<Error>,
    },
    #[error("{path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}
