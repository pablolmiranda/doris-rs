//! Segment trailer and footer.
//!
//! Layout written by `VerticalSegmentWriter::_write_footer` in the C++ Backend:
//! `... | SegmentFooterPB | footer_length: u32 LE | crc32c(footer): u32 LE | b"D0R1"`.
use std::path::Path;

use prost::Message;

use crate::error::Error;
use crate::pb::segment_v2::SegmentFooterPb;

const MAGIC: &[u8; 4] = b"D0R1";
const TRAILER_LEN: usize = 12;

/// Parse and verify the footer at the end of a complete segment file.
pub fn read_footer(bytes: &[u8]) -> Result<SegmentFooterPb, Error> {
    let len = bytes.len();
    if len < TRAILER_LEN {
        return Err(Error::TooShort { len });
    }
    let mut magic = [0u8; 4];
    magic.copy_from_slice(&bytes[len - 4..]);
    if &magic != MAGIC {
        return Err(Error::BadMagic(magic));
    }
    let footer_len =
        u32::from_le_bytes(bytes[len - 12..len - 8].try_into().expect("4 bytes")) as usize;
    let expected = u32::from_le_bytes(bytes[len - 8..len - 4].try_into().expect("4 bytes"));
    if footer_len > len - TRAILER_LEN {
        return Err(Error::FooterOutOfBounds { footer_len, len });
    }
    let footer = &bytes[len - TRAILER_LEN - footer_len..len - TRAILER_LEN];
    let actual = crc32c::crc32c(footer);
    if actual != expected {
        return Err(Error::ChecksumMismatch { expected, actual });
    }
    Ok(SegmentFooterPb::decode(footer)?)
}

/// A segment file held in memory with its parsed footer.
#[derive(Debug, Clone)]
pub struct SegmentFile {
    pub bytes: Vec<u8>,
    pub footer: SegmentFooterPb,
}

impl SegmentFile {
    pub fn open(path: &Path) -> Result<SegmentFile, Error> {
        let bytes = std::fs::read(path).map_err(|source| Error::Io {
            path: path.to_path_buf(),
            source,
        })?;
        SegmentFile::from_bytes(bytes)
    }

    pub fn from_bytes(bytes: Vec<u8>) -> Result<SegmentFile, Error> {
        let footer = read_footer(&bytes)?;
        Ok(SegmentFile { bytes, footer })
    }
}
