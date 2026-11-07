//! Block-based table files (`table/format.{h,cc}`, `table/block_builder.cc`,
//! `table/block_based_table_{builder,reader}.cc`, `table/meta_blocks.cc`).
//!
//! A file is a run of blocks (data blocks, then the properties block, the metaindex block, the
//! index block), each followed by a 5-byte trailer (compression type, masked crc32c of the block
//! and that type byte), and a footer at the end pointing at the metaindex and index blocks.

mod block;
mod builder;
mod reader;

use std::cmp::Ordering;

pub use builder::{FileMeta, TableBuilder, TableProps};
pub use reader::{read_properties, read_table, Table};

use crate::coding::{put_varint64, Reader};

/// `kBlockBasedTableMagicNumber` (`table/block_based_table_builder.cc`).
pub const MAGIC: u64 = 0x88e2_41b7_85f4_cff7;
/// `kLegacyBlockBasedTableMagicNumber`, format version 0.
pub const LEGACY_MAGIC: u64 = 0xdb47_7524_8b80_fb57;
/// `Footer::kNewVersionsEncodedLength` and `kVersion0EncodedLength`.
pub const FOOTER_LEN: usize = 53;
pub const LEGACY_FOOTER_LEN: usize = 48;
pub const BLOCK_TRAILER_LEN: usize = 5;
/// `kPropertiesBlock` (`table/table_properties.cc`).
pub const PROPERTIES_BLOCK: &str = "rocksdb.properties";

/// `CompressionType` (`include/rocksdb/compression_type.h`).
pub const NO_COMPRESSION: u8 = 0;
pub const SNAPPY: u8 = 1;
pub const LZ4: u8 = 4;
pub const LZ4HC: u8 = 5;

/// The kinds an internal key in a table file can carry, of those the BE writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ValueKind {
    Deletion,
    Value,
}

impl ValueKind {
    pub fn code(self) -> u8 {
        match self {
            ValueKind::Deletion => 0x0,
            ValueKind::Value => 0x1,
        }
    }

    /// `kTypeSingleDeletion` (0x7) reads as a deletion.
    pub fn from_code(code: u8) -> Option<ValueKind> {
        match code {
            0x0 | 0x7 => Some(ValueKind::Deletion),
            0x1 => Some(ValueKind::Value),
            _ => None,
        }
    }
}

/// A user key with its sequence number and kind; encoded as the user key followed by
/// fixed64 `(sequence << 8) | kind` (`db/dbformat.h`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InternalKey {
    pub user_key: Vec<u8>,
    pub sequence: u64,
    pub kind: ValueKind,
}

impl InternalKey {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = self.user_key.clone();
        out.extend_from_slice(&((self.sequence << 8) | u64::from(self.kind.code())).to_le_bytes());
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<InternalKey, String> {
        if bytes.len() < 8 {
            return Err(format!("internal key of {} bytes", bytes.len()));
        }
        let (user, tail) = bytes.split_at(bytes.len() - 8);
        let packed = u64::from_le_bytes(tail.try_into().expect("8 bytes"));
        let code = (packed & 0xff) as u8;
        let kind = ValueKind::from_code(code).ok_or_else(|| format!("value type {code:#x} is not supported"))?;
        Ok(InternalKey { user_key: user.to_vec(), sequence: packed >> 8, kind })
    }
}

/// `InternalKeyComparator` over `BytewiseComparator`: user key ascending, then sequence and
/// kind descending (newest first).
impl Ord for InternalKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.user_key
            .cmp(&other.user_key)
            .then_with(|| other.sequence.cmp(&self.sequence))
            .then_with(|| other.kind.cmp(&self.kind))
    }
}

impl PartialOrd for InternalKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// `BlockHandle`: offset and size of a block, without its trailer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockHandle {
    pub offset: u64,
    pub size: u64,
}

impl BlockHandle {
    pub fn encode_to(&self, out: &mut Vec<u8>) {
        put_varint64(out, self.offset);
        put_varint64(out, self.size);
    }

    pub fn decode(r: &mut Reader) -> Option<BlockHandle> {
        Some(BlockHandle { offset: r.varint64()?, size: r.varint64()? })
    }
}
