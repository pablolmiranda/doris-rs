//! `util/coding.h`: little-endian fixed integers, varints, length-prefixed slices.

pub fn put_fixed32(dst: &mut Vec<u8>, v: u32) {
    dst.extend_from_slice(&v.to_le_bytes());
}

pub fn put_fixed64(dst: &mut Vec<u8>, v: u64) {
    dst.extend_from_slice(&v.to_le_bytes());
}

pub fn put_varint32(dst: &mut Vec<u8>, v: u32) {
    put_varint64(dst, u64::from(v));
}

pub fn put_varint64(dst: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        dst.push((v as u8) | 0x80);
        v >>= 7;
    }
    dst.push(v as u8);
}

pub fn put_length_prefixed(dst: &mut Vec<u8>, s: &[u8]) {
    put_varint32(dst, s.len() as u32);
    dst.extend_from_slice(s);
}

/// A cursor over encoded bytes; every getter returns `None` past the end.
#[derive(Debug, Clone)]
pub struct Reader<'a> {
    pub buf: &'a [u8],
}

impl<'a> Reader<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Reader { buf }
    }

    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    pub fn fixed32(&mut self) -> Option<u32> {
        let (b, rest) = self.buf.split_first_chunk::<4>()?;
        self.buf = rest;
        Some(u32::from_le_bytes(*b))
    }

    pub fn fixed64(&mut self) -> Option<u64> {
        let (b, rest) = self.buf.split_first_chunk::<8>()?;
        self.buf = rest;
        Some(u64::from_le_bytes(*b))
    }

    pub fn varint64(&mut self) -> Option<u64> {
        let mut v = 0u64;
        for (i, &b) in self.buf.iter().enumerate().take(10) {
            v |= u64::from(b & 0x7f) << (7 * i);
            if b < 0x80 {
                self.buf = &self.buf[i + 1..];
                return Some(v);
            }
        }
        None
    }

    pub fn varint32(&mut self) -> Option<u32> {
        self.varint64().and_then(|v| u32::try_from(v).ok())
    }

    pub fn bytes(&mut self, n: usize) -> Option<&'a [u8]> {
        if self.buf.len() < n {
            return None;
        }
        let (a, b) = self.buf.split_at(n);
        self.buf = b;
        Some(a)
    }

    pub fn u8(&mut self) -> Option<u8> {
        self.bytes(1).map(|b| b[0])
    }

    pub fn length_prefixed(&mut self) -> Option<&'a [u8]> {
        let n = self.varint32()? as usize;
        self.bytes(n)
    }
}

/// `crc32c::Mask` (`util/crc32c.h`).
pub fn mask_crc(crc: u32) -> u32 {
    crc.rotate_right(15).wrapping_add(0xa282_ead8)
}

pub fn unmask_crc(masked: u32) -> u32 {
    masked.wrapping_sub(0xa282_ead8).rotate_left(15)
}
