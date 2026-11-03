//! The per-suite traffic corpus (`oracle/epic1/traffic`): one gzip file per upstream suite,
//! holding the FE↔BE messages the reference BE handled while the suite ran. Each record is a
//! little-endian u32 header length, a JSON header (`protocol`, `service`, `method`, `kind`,
//! `time_ms`, ...), a little-endian u32 data length and the raw message bytes; the oracle's
//! `checker/records.py` writes the same format.
use std::fs::File;
use std::io::{self, Read};
use std::path::{Path, PathBuf};

use flate2::read::GzDecoder;

#[derive(Debug, Clone)]
pub struct SuiteRecord {
    pub header: serde_json::Value,
    pub data: Vec<u8>,
}

/// `oracle/epic1/traffic` in this checkout.
pub fn corpus_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../oracle/epic1/traffic")
}

/// The suite files of a corpus directory, sorted; empty when the directory does not exist.
pub fn suite_files(dir: &Path) -> Vec<PathBuf> {
    let mut files: Vec<_> = std::fs::read_dir(dir)
        .map(|rd| {
            rd.filter_map(|e| e.ok().map(|e| e.path()))
                .filter(|p| p.to_string_lossy().ends_with(".bin.gz"))
                .collect()
        })
        .unwrap_or_default();
    files.sort();
    files
}

fn read_u32(r: &mut impl Read) -> io::Result<Option<u32>> {
    let mut buf = [0u8; 4];
    let mut got = 0;
    while got < 4 {
        let n = r.read(&mut buf[got..])?;
        if n == 0 {
            return if got == 0 {
                Ok(None)
            } else {
                Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "record length cut short",
                ))
            };
        }
        got += n;
    }
    Ok(Some(u32::from_le_bytes(buf)))
}

fn read_exact_vec(r: &mut impl Read, n: u32) -> io::Result<Vec<u8>> {
    let mut v = vec![0u8; n as usize];
    r.read_exact(&mut v)?;
    Ok(v)
}

/// Every record of one suite file, in capture order.
pub fn read_suite(path: &Path) -> io::Result<Vec<SuiteRecord>> {
    let mut r = GzDecoder::new(File::open(path)?);
    let mut out = Vec::new();
    while let Some(header_len) = read_u32(&mut r)? {
        let header = serde_json::from_slice(&read_exact_vec(&mut r, header_len)?)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let data_len = read_u32(&mut r)?.ok_or_else(|| {
            io::Error::new(io::ErrorKind::UnexpectedEof, "record without data length")
        })?;
        out.push(SuiteRecord {
            header,
            data: read_exact_vec(&mut r, data_len)?,
        });
    }
    Ok(out)
}
