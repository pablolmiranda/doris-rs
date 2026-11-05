//! `FileHeader<TabletMetaPB>` (`be/src/storage/file_header.h`): the `<tablet_id>.hdr` file the
//! C++ BE saves in a tablet's directory before moving it to trash, and in snapshots. A packed
//! `FixedFileHeaderV2` (magic number u64 `OLAP_FIX_HEADER_MAGIC_NUMBER` = 0, version u32
//! `OLAP_DATA_VERSION_APPLIED` = 1, file length u64, checksum u32 = 0, protobuf length u64,
//! protobuf checksum u32 = adler32 of the protobuf), a u32 extra header of 0, then the protobuf;
//! all little-endian.

use doris_proto::doris::TabletMetaPb;
use prost::Message;

const FIXED_LEN: usize = 36;
const EXTRA_LEN: usize = 4;
const VERSION: u32 = 1;

/// zlib's `adler32`, started from `adler32(0, Z_NULL, 0)` = 1 (`olap_adler32_init`).
pub fn adler32(bytes: &[u8]) -> u32 {
    const MOD: u32 = 65_521;
    let (mut a, mut b) = (1u32, 0u32);
    for chunk in bytes.chunks(5552) {
        for &x in chunk {
            a += u32::from(x);
            b += a;
        }
        a %= MOD;
        b %= MOD;
    }
    (b << 16) | a
}

pub fn encode_file_header(meta: &TabletMetaPb) -> Vec<u8> {
    let proto = meta.encode_to_vec();
    let total = (FIXED_LEN + EXTRA_LEN + proto.len()) as u64;
    let mut out = Vec::with_capacity(total as usize);
    out.extend_from_slice(&0u64.to_le_bytes());
    out.extend_from_slice(&VERSION.to_le_bytes());
    out.extend_from_slice(&total.to_le_bytes());
    out.extend_from_slice(&0u32.to_le_bytes());
    out.extend_from_slice(&(proto.len() as u64).to_le_bytes());
    out.extend_from_slice(&adler32(&proto).to_le_bytes());
    out.extend_from_slice(&0u32.to_le_bytes());
    out.extend_from_slice(&proto);
    out
}

/// Reads a file header back, checking its lengths and the protobuf checksum
/// (`FileHeader::deserialize`).
pub fn decode_file_header(bytes: &[u8]) -> Result<TabletMetaPb, String> {
    if bytes.len() < FIXED_LEN + EXTRA_LEN {
        return Err("file shorter than its header".into());
    }
    let u64_at = |i: usize| u64::from_le_bytes(bytes[i..i + 8].try_into().expect("8 bytes"));
    let u32_at = |i: usize| u32::from_le_bytes(bytes[i..i + 4].try_into().expect("4 bytes"));
    if u64_at(0) != 0 {
        return Err("not a FixedFileHeaderV2 (magic number)".into());
    }
    if u64_at(12) != bytes.len() as u64 {
        return Err(format!("file length {} does not match the header's {}", bytes.len(), u64_at(12)));
    }
    let proto = &bytes[FIXED_LEN + EXTRA_LEN..];
    if u64_at(24) != proto.len() as u64 || u32_at(32) != adler32(proto) {
        return Err("protobuf length or checksum mismatch".into());
    }
    TabletMetaPb::decode(proto).map_err(|e| e.to_string())
}
