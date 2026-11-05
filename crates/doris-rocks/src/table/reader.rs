//! Reading a whole table file (`BlockBasedTable::Open` and its iterators, simplified: every
//! data block is read in order).

use std::collections::BTreeMap;
use std::path::Path;

use super::block::entries;
use super::{BlockHandle, InternalKey, FOOTER_LEN, LEGACY_FOOTER_LEN, LEGACY_MAGIC, LZ4, LZ4HC, MAGIC, NO_COMPRESSION, PROPERTIES_BLOCK, SNAPPY};
use crate::coding::{unmask_crc, Reader};
use crate::error::{IoContext, RocksError};

/// A table file's entries in file order, and how many of its blocks were compressed.
#[derive(Debug)]
pub struct Table {
    pub entries: Vec<(InternalKey, Vec<u8>)>,
    pub compressed_blocks: usize,
}

struct File<'a> {
    path: &'a Path,
    bytes: Vec<u8>,
    compressed_blocks: usize,
}

impl File<'_> {
    fn corrupt(&self, what: impl Into<String>) -> RocksError {
        RocksError::Corrupt { path: self.path.to_path_buf(), what: what.into() }
    }

    /// (metaindex, index) from the footer.
    fn footer(&self) -> Result<(BlockHandle, BlockHandle), RocksError> {
        let b = &self.bytes;
        if b.len() < LEGACY_FOOTER_LEN {
            return Err(self.corrupt("file shorter than a footer"));
        }
        let magic = u64::from_le_bytes(b[b.len() - 8..].try_into().expect("8 bytes"));
        let handles = if magic == MAGIC {
            if b.len() < FOOTER_LEN {
                return Err(self.corrupt("file shorter than a footer"));
            }
            let f = &b[b.len() - FOOTER_LEN..];
            let version = u32::from_le_bytes(f[FOOTER_LEN - 12..FOOTER_LEN - 8].try_into().expect("4 bytes"));
            if version > 2 {
                return Err(RocksError::Unsupported { path: self.path.to_path_buf(), what: format!("table format version {version}") });
            }
            if f[0] != 1 {
                return Err(RocksError::Unsupported { path: self.path.to_path_buf(), what: format!("checksum type {}", f[0]) });
            }
            &f[1..]
        } else if magic == LEGACY_MAGIC {
            &b[b.len() - LEGACY_FOOTER_LEN..]
        } else {
            return Err(self.corrupt(format!("bad table magic number {magic:#x}")));
        };
        let mut r = Reader::new(handles);
        let meta = BlockHandle::decode(&mut r).ok_or_else(|| self.corrupt("bad footer"))?;
        let index = BlockHandle::decode(&mut r).ok_or_else(|| self.corrupt("bad footer"))?;
        Ok((meta, index))
    }

    /// A block's contents, checksum verified and decompressed.
    fn block(&mut self, h: BlockHandle) -> Result<Vec<u8>, RocksError> {
        let (start, len) = (h.offset as usize, h.size as usize);
        let end = start.checked_add(len + super::BLOCK_TRAILER_LEN).filter(|e| *e <= self.bytes.len());
        let Some(end) = end else {
            return Err(self.corrupt(format!("block at {start} runs past the end")));
        };
        let data = &self.bytes[start..start + len];
        let kind = self.bytes[start + len];
        let stored = u32::from_le_bytes(self.bytes[start + len + 1..end].try_into().expect("4 bytes"));
        let crc = crc32c::crc32c_append(crc32c::crc32c(data), &[kind]);
        if crc != unmask_crc(stored) {
            return Err(self.corrupt(format!("checksum mismatch in the block at offset {start}")));
        }
        match kind {
            NO_COMPRESSION => Ok(data.to_vec()),
            SNAPPY => {
                self.compressed_blocks += 1;
                snap::raw::Decoder::new().decompress_vec(data).map_err(|e| self.corrupt(format!("snappy: {e}")))
            }
            // format version 2: varint32 decompressed size, then the LZ4 block
            LZ4 | LZ4HC => {
                self.compressed_blocks += 1;
                let mut r = Reader::new(data);
                let size = r.varint32().ok_or_else(|| self.corrupt("lz4 block without its size"))? as usize;
                lz4_flex::block::decompress(r.buf, size).map_err(|e| self.corrupt(format!("lz4: {e}")))
            }
            other => Err(RocksError::Unsupported { path: self.path.to_path_buf(), what: format!("compression type {other}") }),
        }
    }

    fn metaindex(&mut self) -> Result<BTreeMap<String, BlockHandle>, RocksError> {
        let (meta, _) = self.footer()?;
        let block = self.block(meta)?;
        let mut out = BTreeMap::new();
        for (k, v) in entries(&block).map_err(|e| self.corrupt(e))? {
            let h = BlockHandle::decode(&mut Reader::new(&v)).ok_or_else(|| self.corrupt("bad metaindex entry"))?;
            out.insert(String::from_utf8_lossy(&k).into_owned(), h);
        }
        Ok(out)
    }
}

fn open(path: &Path) -> Result<File<'_>, RocksError> {
    Ok(File { path, bytes: std::fs::read(path).at(path)?, compressed_blocks: 0 })
}

pub fn read_table(path: &Path) -> Result<Table, RocksError> {
    let mut f = open(path)?;
    let meta = f.metaindex()?;
    if let Some(name) = meta.keys().find(|k| k.as_str() != PROPERTIES_BLOCK && !k.starts_with("rocksdb.compression_dict")) {
        return Err(RocksError::Unsupported { path: path.to_path_buf(), what: format!("meta block {name}") });
    }
    let (_, index) = f.footer()?;
    let index = f.block(index)?;
    let mut out = Vec::new();
    for (_, v) in entries(&index).map_err(|e| f.corrupt(e))? {
        let h = BlockHandle::decode(&mut Reader::new(&v)).ok_or_else(|| f.corrupt("bad index entry"))?;
        let block = f.block(h)?;
        for (k, v) in entries(&block).map_err(|e| f.corrupt(e))? {
            out.push((InternalKey::decode(&k).map_err(|e| f.corrupt(e))?, v));
        }
    }
    Ok(Table { entries: out, compressed_blocks: f.compressed_blocks })
}

/// The properties block: name → raw value (varint64 for numbers, bytes for strings).
pub fn read_properties(path: &Path) -> Result<BTreeMap<String, Vec<u8>>, RocksError> {
    let mut f = open(path)?;
    let meta = f.metaindex()?;
    let Some(h) = meta.get(PROPERTIES_BLOCK).copied() else {
        return Ok(BTreeMap::new());
    };
    let block = f.block(h)?;
    Ok(entries(&block)
        .map_err(|e| f.corrupt(e))?
        .into_iter()
        .map(|(k, v)| (String::from_utf8_lossy(&k).into_owned(), v))
        .collect())
}
