//! Blocks (`table/block_builder.cc`, `table/block.cc`): entries with keys prefix-compressed
//! against the previous key (shared length, non-shared length, value length, key delta, value),
//! full keys at every restart point, then the restart offsets and their count as fixed32s.

use crate::coding::{put_fixed32, put_varint32, Reader};

/// A block's (key, value) entries.
pub type Entries = Vec<(Vec<u8>, Vec<u8>)>;

/// Every (key, value) of a block, in order.
pub fn entries(block: &[u8]) -> Result<Entries, String> {
    if block.len() < 4 {
        return Err("block shorter than its restart count".into());
    }
    let num_restarts = u32::from_le_bytes(block[block.len() - 4..].try_into().expect("4 bytes")) as usize;
    let restarts_at = block
        .len()
        .checked_sub(4 + 4 * num_restarts)
        .ok_or_else(|| format!("block of {} bytes cannot hold {num_restarts} restarts", block.len()))?;
    let mut r = Reader::new(&block[..restarts_at]);
    let mut out: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    let mut last: Vec<u8> = Vec::new();
    let bad = || "block entry runs past its end".to_string();
    while !r.is_empty() {
        let shared = r.varint32().ok_or_else(bad)? as usize;
        let non_shared = r.varint32().ok_or_else(bad)? as usize;
        let value_len = r.varint32().ok_or_else(bad)? as usize;
        if shared > last.len() {
            return Err("block entry shares more than the previous key".into());
        }
        let mut key = last[..shared].to_vec();
        key.extend_from_slice(r.bytes(non_shared).ok_or_else(bad)?);
        let value = r.bytes(value_len).ok_or_else(bad)?.to_vec();
        last.clone_from(&key);
        out.push((key, value));
    }
    Ok(out)
}

/// `BlockBuilder`.
#[derive(Debug)]
pub struct BlockBuilder {
    buf: Vec<u8>,
    restarts: Vec<u32>,
    counter: usize,
    restart_interval: usize,
    last_key: Vec<u8>,
}

impl BlockBuilder {
    pub fn new(restart_interval: usize) -> BlockBuilder {
        BlockBuilder { buf: Vec::new(), restarts: vec![0], counter: 0, restart_interval, last_key: Vec::new() }
    }

    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    /// The size the finished block would have.
    pub fn estimated_size(&self) -> usize {
        self.buf.len() + 4 * self.restarts.len() + 4
    }

    pub fn add(&mut self, key: &[u8], value: &[u8]) {
        let shared = if self.counter < self.restart_interval {
            key.iter().zip(&self.last_key).take_while(|(a, b)| a == b).count()
        } else {
            self.restarts.push(self.buf.len() as u32);
            self.counter = 0;
            0
        };
        put_varint32(&mut self.buf, shared as u32);
        put_varint32(&mut self.buf, (key.len() - shared) as u32);
        put_varint32(&mut self.buf, value.len() as u32);
        self.buf.extend_from_slice(&key[shared..]);
        self.buf.extend_from_slice(value);
        self.last_key = key.to_vec();
        self.counter += 1;
    }

    /// The finished block; the builder is reset for the next one.
    pub fn finish(&mut self) -> Vec<u8> {
        let mut out = std::mem::take(&mut self.buf);
        for r in &self.restarts {
            put_fixed32(&mut out, *r);
        }
        put_fixed32(&mut out, self.restarts.len() as u32);
        self.restarts = vec![0];
        self.counter = 0;
        self.last_key.clear();
        out
    }
}
