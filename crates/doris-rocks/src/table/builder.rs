//! Writing a table file as `BlockBasedTableBuilder` does with `format_version = 2`, crc32c
//! checksums, no filter and no compression: 4 KiB data blocks with restart interval 16, a
//! properties block, the metaindex block, an index block with restart interval 1 (each entry the
//! last key of its data block), and the footer.

use std::fs::File;
use std::io::Write;
use std::time::{SystemTime, UNIX_EPOCH};

use super::block::BlockBuilder;
use super::{BlockHandle, InternalKey, FOOTER_LEN, MAGIC, NO_COMPRESSION, PROPERTIES_BLOCK};
use crate::coding::{mask_crc, put_fixed32, put_varint64};
use crate::error::RocksError;

const BLOCK_SIZE: usize = 4096;
const RESTART_INTERVAL: usize = 16;
const FORMAT_VERSION: u32 = 2;

/// What a table's properties block says about its column family.
#[derive(Debug, Clone)]
pub struct TableProps {
    pub column_family_id: u32,
    pub column_family_name: String,
    pub fixed_prefix_len: Option<usize>,
}

/// What a manifest records of a finished table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileMeta {
    pub size: u64,
    pub smallest: InternalKey,
    pub largest: InternalKey,
    pub smallest_seqno: u64,
    pub largest_seqno: u64,
}

#[derive(Debug)]
pub struct TableBuilder {
    file: File,
    props: TableProps,
    offset: u64,
    data: BlockBuilder,
    index: BlockBuilder,
    last_key: Option<InternalKey>,
    first_key: Option<InternalKey>,
    seqnos: Option<(u64, u64)>,
    num_entries: u64,
    num_deletions: u64,
    num_data_blocks: u64,
    raw_key_size: u64,
    raw_value_size: u64,
    error: Option<std::io::Error>,
}

impl TableBuilder {
    pub fn new(file: File, props: TableProps) -> TableBuilder {
        TableBuilder {
            file,
            props,
            offset: 0,
            data: BlockBuilder::new(RESTART_INTERVAL),
            index: BlockBuilder::new(1),
            last_key: None,
            first_key: None,
            seqnos: None,
            num_entries: 0,
            num_deletions: 0,
            num_data_blocks: 0,
            raw_key_size: 0,
            raw_value_size: 0,
            error: None,
        }
    }

    /// Adds an entry; keys must come in internal-key order.
    pub fn add(&mut self, key: &InternalKey, value: &[u8]) -> Result<(), RocksError> {
        debug_assert!(self.last_key.as_ref().is_none_or(|l| l < key), "keys out of order");
        let encoded = key.encode();
        self.data.add(&encoded, value);
        self.raw_key_size += encoded.len() as u64;
        self.raw_value_size += value.len() as u64;
        self.num_entries += 1;
        if key.kind == super::ValueKind::Deletion {
            self.num_deletions += 1;
        }
        self.seqnos = Some(match self.seqnos {
            None => (key.sequence, key.sequence),
            Some((lo, hi)) => (lo.min(key.sequence), hi.max(key.sequence)),
        });
        if self.first_key.is_none() {
            self.first_key = Some(key.clone());
        }
        self.last_key = Some(key.clone());
        if self.data.estimated_size() >= BLOCK_SIZE {
            self.flush_data()?;
        }
        Ok(())
    }

    fn flush_data(&mut self) -> Result<(), RocksError> {
        if self.data.is_empty() {
            return Ok(());
        }
        let block = self.data.finish();
        let h = self.write_block(&block)?;
        let mut v = Vec::new();
        h.encode_to(&mut v);
        let last = self.last_key.as_ref().expect("a data block has keys").encode();
        self.index.add(&last, &v);
        self.num_data_blocks += 1;
        Ok(())
    }

    fn write_block(&mut self, block: &[u8]) -> Result<BlockHandle, RocksError> {
        let h = BlockHandle { offset: self.offset, size: block.len() as u64 };
        let crc = crc32c::crc32c_append(crc32c::crc32c(block), &[NO_COMPRESSION]);
        let mut out = Vec::with_capacity(block.len() + 5);
        out.extend_from_slice(block);
        out.push(NO_COMPRESSION);
        out.extend_from_slice(&mask_crc(crc).to_le_bytes());
        self.append(&out)?;
        Ok(h)
    }

    fn append(&mut self, bytes: &[u8]) -> Result<(), RocksError> {
        if let Err(e) = self.file.write_all(bytes) {
            self.error = Some(e);
        }
        if let Some(e) = self.error.take() {
            return Err(RocksError::Io { path: "<table>".into(), source: e });
        }
        self.offset += bytes.len() as u64;
        Ok(())
    }

    /// The properties a 5.14.2 table of the BE's holds (`PropertyBlockBuilder::AddTableProperty`,
    /// the internal-key and block-based collectors), sorted by name.
    fn properties(&self, data_size: u64, index_size: u64) -> Vec<(String, Vec<u8>)> {
        let num = |v: u64| {
            let mut b = Vec::new();
            put_varint64(&mut b, v);
            b
        };
        let now = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0);
        let mut p: Vec<(String, Vec<u8>)> = vec![
            ("rocksdb.raw.key.size".into(), num(self.raw_key_size)),
            ("rocksdb.raw.value.size".into(), num(self.raw_value_size)),
            ("rocksdb.data.size".into(), num(data_size)),
            ("rocksdb.index.size".into(), num(index_size)),
            ("rocksdb.num.entries".into(), num(self.num_entries)),
            ("rocksdb.num.data.blocks".into(), num(self.num_data_blocks)),
            ("rocksdb.filter.size".into(), num(0)),
            // TableProperties::format_version stays 0 for block-based tables; the footer has 2
            ("rocksdb.format.version".into(), num(0)),
            ("rocksdb.fixed.key.length".into(), num(0)),
            ("rocksdb.column.family.id".into(), num(u64::from(self.props.column_family_id))),
            ("rocksdb.creation.time".into(), num(now)),
            ("rocksdb.oldest.key.time".into(), num(now)),
            ("rocksdb.comparator".into(), b"leveldb.BytewiseComparator".to_vec()),
            (
                "rocksdb.prefix.extractor.name".into(),
                match self.props.fixed_prefix_len {
                    Some(n) => format!("rocksdb.FixedPrefix.{n}").into_bytes(),
                    None => b"nullptr".to_vec(),
                },
            ),
            ("rocksdb.property.collectors".into(), b"[]".to_vec()),
            ("rocksdb.column.family.name".into(), self.props.column_family_name.clone().into_bytes()),
            ("rocksdb.compression".into(), b"NoCompression".to_vec()),
            ("rocksdb.merge.operator".into(), b"nullptr".to_vec()),
            ("rocksdb.deleted.keys".into(), num(self.num_deletions)),
            ("rocksdb.merge.operands".into(), num(0)),
            // BlockBasedTablePropertiesCollector: kBinarySearch, and both filterings on
            ("rocksdb.block.based.table.index.type".into(), 0u32.to_le_bytes().to_vec()),
            ("rocksdb.block.based.table.whole.key.filtering".into(), b"1".to_vec()),
            ("rocksdb.block.based.table.prefix.filtering".into(), b"1".to_vec()),
        ];
        p.sort();
        p
    }

    pub fn finish(mut self) -> Result<FileMeta, RocksError> {
        self.flush_data()?;
        let data_size = self.offset;
        let index = self.index.finish();
        let props = self.properties(data_size, index.len() as u64);
        let mut pb = BlockBuilder::new(RESTART_INTERVAL);
        for (k, v) in &props {
            pb.add(k.as_bytes(), v);
        }
        let props_handle = self.write_block(&pb.finish())?;
        let mut meta = BlockBuilder::new(1);
        let mut v = Vec::new();
        props_handle.encode_to(&mut v);
        meta.add(PROPERTIES_BLOCK.as_bytes(), &v);
        let meta_handle = self.write_block(&meta.finish())?;
        let index_handle = self.write_block(&index)?;
        let mut footer = vec![1u8]; // kCRC32c
        meta_handle.encode_to(&mut footer);
        index_handle.encode_to(&mut footer);
        footer.resize(FOOTER_LEN - 12, 0);
        put_fixed32(&mut footer, FORMAT_VERSION);
        footer.extend_from_slice(&MAGIC.to_le_bytes());
        self.append(&footer)?;
        self.file.sync_all().map_err(|e| RocksError::Io { path: "<table>".into(), source: e })?;
        let (smallest_seqno, largest_seqno) = self.seqnos.unwrap_or((0, 0));
        let empty = || InternalKey { user_key: Vec::new(), sequence: 0, kind: super::ValueKind::Value };
        Ok(FileMeta {
            size: self.offset,
            smallest: self.first_key.unwrap_or_else(empty),
            largest: self.last_key.unwrap_or_else(empty),
            smallest_seqno,
            largest_seqno,
        })
    }
}
