//! Log files (`db/log_format.h`, `db/log_writer.cc`, `db/log_reader.cc`): the format of both
//! write-ahead logs and manifests. The file is a run of 32 KiB blocks; each record is split into
//! fragments that never cross a block, each with a 7-byte header (masked crc32c of type and
//! payload, payload length, type). A block's last bytes, when fewer than a header, are zeros.

use std::fs::File;
use std::io::Write;
use std::path::Path;

use crate::coding::{mask_crc, unmask_crc};
use crate::error::{IoContext, RocksError};

pub const BLOCK_SIZE: usize = 32768;
pub const HEADER_SIZE: usize = 7;
const ZERO: u8 = 0;
const FULL: u8 = 1;
const FIRST: u8 = 2;
const MIDDLE: u8 = 3;
const LAST: u8 = 4;
/// `kRecyclableFullType` and up: the recyclable format, which the BE does not enable.
const RECYCLABLE_FULL: u8 = 5;
const RECYCLABLE_LAST: u8 = 8;

/// Where a log's bytes go: a file, or anything that can also be cut back and synced.
pub trait LogSink: Write {
    fn truncate(&mut self, len: u64) -> std::io::Result<()>;
    fn sync(&mut self) -> std::io::Result<()>;
}

impl LogSink for File {
    fn truncate(&mut self, len: u64) -> std::io::Result<()> {
        self.set_len(len)?;
        std::io::Seek::seek(self, std::io::SeekFrom::Start(len)).map(|_| ())
    }

    fn sync(&mut self) -> std::io::Result<()> {
        self.sync_data()
    }
}

/// Appends records to a log file in the legacy (non-recyclable) format, as `log::Writer` does
/// with `recycle_log_files = false`. A record whose append fails is cut back off, so a later
/// record never follows torn bytes.
#[derive(Debug)]
pub struct LogWriter<F: LogSink = File> {
    file: F,
    block_offset: usize,
    #[allow(dead_code)]
    log_number: u64,
    bytes: u64,
}

impl<F: LogSink> LogWriter<F> {
    /// A writer at the start of `file` (a new, empty log).
    pub fn new(file: F, log_number: u64) -> LogWriter<F> {
        LogWriter { file, block_offset: 0, log_number, bytes: 0 }
    }

    /// Continues a log that already holds `len` bytes.
    pub fn resume_at(&mut self, len: u64) {
        self.bytes = len;
        self.block_offset = (len % BLOCK_SIZE as u64) as usize;
    }

    pub fn into_inner(self) -> F {
        self.file
    }

    /// Bytes written so far.
    pub fn len(&self) -> u64 {
        self.bytes
    }

    pub fn is_empty(&self) -> bool {
        self.bytes == 0
    }

    pub fn add_record(&mut self, data: &[u8]) -> std::io::Result<()> {
        let (start_offset, start_bytes) = (self.block_offset, self.bytes);
        let mut out = Vec::with_capacity(data.len() + HEADER_SIZE * (1 + data.len() / BLOCK_SIZE) + HEADER_SIZE);
        let mut rest = data;
        let mut begin = true;
        loop {
            let left = BLOCK_SIZE - self.block_offset;
            if left < HEADER_SIZE {
                out.extend(std::iter::repeat_n(0u8, left));
                self.block_offset = 0;
            }
            let avail = BLOCK_SIZE - self.block_offset - HEADER_SIZE;
            let n = rest.len().min(avail);
            let end = n == rest.len();
            let kind = match (begin, end) {
                (true, true) => FULL,
                (true, false) => FIRST,
                (false, true) => LAST,
                (false, false) => MIDDLE,
            };
            let crc = crc32c::crc32c_append(crc32c::crc32c(&[kind]), &rest[..n]);
            out.extend_from_slice(&mask_crc(crc).to_le_bytes());
            out.extend_from_slice(&(n as u16).to_le_bytes());
            out.push(kind);
            out.extend_from_slice(&rest[..n]);
            self.block_offset += HEADER_SIZE + n;
            rest = &rest[n..];
            begin = false;
            if end {
                break;
            }
        }
        if let Err(e) = self.file.write_all(&out) {
            self.block_offset = start_offset;
            self.bytes = start_bytes;
            let _ = self.file.truncate(start_bytes);
            return Err(e);
        }
        self.bytes += out.len() as u64;
        Ok(())
    }

    pub fn sync(&mut self) -> std::io::Result<()> {
        self.file.sync()
    }
}

/// Every whole record of a manifest. A torn tail (a fragment running past the end of the file,
/// or zeros where a header should be) ends it quietly; a bad checksum or fragment order before
/// that is corruption.
pub fn read_records(path: &Path) -> Result<Vec<Vec<u8>>, RocksError> {
    read(path, false).map(|(records, _)| records)
}

/// Every record of a write-ahead log up to the first bad one, and whether one was found
/// (`kPointInTimeRecovery`, 5.14.2's default `wal_recovery_mode`: replay stops at the first
/// corruption and the database opens with what came before).
pub fn read_wal_records(path: &Path) -> Result<(Vec<Vec<u8>>, bool), RocksError> {
    read(path, true)
}

fn read(path: &Path, tolerant: bool) -> Result<(Vec<Vec<u8>>, bool), RocksError> {
    let bytes = std::fs::read(path).at(path)?;
    let corrupt = |what: String| RocksError::Corrupt { path: path.to_path_buf(), what };
    let stop = |out: Vec<Vec<u8>>, e: RocksError| if tolerant { Ok((out, true)) } else { Err(e) };
    let mut out = Vec::new();
    let mut partial: Option<Vec<u8>> = None;
    let mut pos = 0;
    while pos < bytes.len() {
        let left = BLOCK_SIZE - pos % BLOCK_SIZE;
        if left < HEADER_SIZE {
            pos += left;
            continue;
        }
        if pos + HEADER_SIZE > bytes.len() {
            break;
        }
        let h = &bytes[pos..pos + HEADER_SIZE];
        let masked = u32::from_le_bytes([h[0], h[1], h[2], h[3]]);
        let len = usize::from(u16::from_le_bytes([h[4], h[5]]));
        let kind = h[6];
        if kind == ZERO {
            break;
        }
        if (RECYCLABLE_FULL..=RECYCLABLE_LAST).contains(&kind) {
            return Err(RocksError::Unsupported { path: path.to_path_buf(), what: "recyclable log records".into() });
        }
        let start = pos + HEADER_SIZE;
        if start + len > bytes.len() {
            break;
        }
        if HEADER_SIZE + len > left {
            return stop(out, corrupt(format!("a fragment at offset {pos} crosses its block")));
        }
        let payload = &bytes[start..start + len];
        let crc = crc32c::crc32c_append(crc32c::crc32c(&[kind]), payload);
        if crc != unmask_crc(masked) {
            return stop(out, corrupt(format!("checksum mismatch in the record at offset {pos}")));
        }
        pos = start + len;
        match kind {
            FULL => {
                if partial.take().is_some() {
                    return stop(out, corrupt(format!("a whole record at offset {} inside a fragmented one", start - HEADER_SIZE)));
                }
                out.push(payload.to_vec());
            }
            FIRST => {
                if partial.replace(payload.to_vec()).is_some() {
                    return stop(out, corrupt("two first fragments in a row".into()));
                }
            }
            MIDDLE | LAST => {
                let Some(mut p) = partial.take() else {
                    return stop(out, corrupt(format!("a fragment without its first part at offset {}", start - HEADER_SIZE)));
                };
                p.extend_from_slice(payload);
                if kind == LAST {
                    out.push(p);
                } else {
                    partial = Some(p);
                }
            }
            other => return stop(out, corrupt(format!("unknown record type {other}"))),
        }
    }
    Ok((out, false))
}
