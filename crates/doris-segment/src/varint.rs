//! LEB128 varints as written by `util/coding.h` (`put_varint32`, `put_varint64`).
use crate::error::Error;

/// Read an unsigned LEB128 value of at most `max_bytes` bytes at `*pos`, advancing it.
fn read_leb128(buf: &[u8], pos: &mut usize, max_bytes: usize) -> Result<u64, Error> {
    let mut value: u64 = 0;
    for i in 0..max_bytes {
        let byte = *buf
            .get(*pos)
            .ok_or_else(|| Error::Corrupt("varint runs past end of buffer".into()))?;
        *pos += 1;
        value |= u64::from(byte & 0x7F) << (7 * i);
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err(Error::Corrupt("varint longer than permitted".into()))
}

pub fn read_varint32(buf: &[u8], pos: &mut usize) -> Result<u32, Error> {
    let v = read_leb128(buf, pos, 5)?;
    u32::try_from(v).map_err(|_| Error::Corrupt("varint32 overflow".into()))
}

pub fn read_varint64(buf: &[u8], pos: &mut usize) -> Result<u64, Error> {
    read_leb128(buf, pos, 10)
}
