//! Write batches (`db/write_batch.cc`): the payload of each write-ahead log record. A 12-byte
//! header (sequence number fixed64, count fixed32), then records: a type byte (`ValueType`,
//! `db/dbformat.h`), a varint32 column family id for the `kTypeColumnFamily*` types, the key and,
//! for puts, the value, both length-prefixed.

use crate::coding::{put_fixed32, put_fixed64, put_length_prefixed, put_varint32, Reader};

const DELETION: u8 = 0x0;
const VALUE: u8 = 0x1;
const CF_DELETION: u8 = 0x4;
const CF_VALUE: u8 = 0x5;
const SINGLE_DELETION: u8 = 0x7;
const CF_SINGLE_DELETION: u8 = 0x8;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BatchOp {
    Put { cf: u32, key: Vec<u8>, value: Vec<u8> },
    Delete { cf: u32, key: Vec<u8> },
}

impl BatchOp {
    pub fn cf(&self) -> u32 {
        match self {
            BatchOp::Put { cf, .. } | BatchOp::Delete { cf, .. } => *cf,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WriteBatch {
    /// The sequence number of the first operation; each operation takes the next one.
    pub sequence: u64,
    pub ops: Vec<BatchOp>,
}

impl WriteBatch {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        put_fixed64(&mut out, self.sequence);
        put_fixed32(&mut out, self.ops.len() as u32);
        for op in &self.ops {
            match op {
                BatchOp::Put { cf, key, value } => {
                    if *cf == 0 {
                        out.push(VALUE);
                    } else {
                        out.push(CF_VALUE);
                        put_varint32(&mut out, *cf);
                    }
                    put_length_prefixed(&mut out, key);
                    put_length_prefixed(&mut out, value);
                }
                BatchOp::Delete { cf, key } => {
                    if *cf == 0 {
                        out.push(DELETION);
                    } else {
                        out.push(CF_DELETION);
                        put_varint32(&mut out, *cf);
                    }
                    put_length_prefixed(&mut out, key);
                }
            }
        }
        out
    }

    /// Single deletions decode as deletions; any other record type is an error naming it.
    pub fn decode(bytes: &[u8]) -> Result<WriteBatch, String> {
        let mut r = Reader::new(bytes);
        let sequence = r.fixed64().ok_or("write batch shorter than its header")?;
        let count = r.fixed32().ok_or("write batch shorter than its header")?;
        let mut ops = Vec::with_capacity(count as usize);
        let short = || "write batch record runs past its end".to_string();
        while !r.is_empty() {
            let tag = r.u8().ok_or_else(short)?;
            let cf = match tag {
                CF_VALUE | CF_DELETION | CF_SINGLE_DELETION => r.varint32().ok_or_else(short)?,
                VALUE | DELETION | SINGLE_DELETION => 0,
                other => return Err(format!("write batch record type {other:#x} is not supported")),
            };
            let key = r.length_prefixed().ok_or_else(short)?.to_vec();
            ops.push(match tag {
                VALUE | CF_VALUE => BatchOp::Put { cf, key, value: r.length_prefixed().ok_or_else(short)?.to_vec() },
                _ => BatchOp::Delete { cf, key },
            });
        }
        if ops.len() != count as usize {
            return Err(format!("write batch holds {} records, its header says {count}", ops.len()));
        }
        Ok(WriteBatch { sequence, ops })
    }
}
