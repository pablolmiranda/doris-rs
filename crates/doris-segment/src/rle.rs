//! Run-length / bit-packed hybrid decoder matching `util/rle_encoding.h` (`RleDecoder<T>`).
//!
//! Stream: runs, each starting at a byte boundary with a LEB128 indicator. Odd indicator:
//! literal run of `(indicator >> 1) * 8` values, each `bit_width` bits, packed LSB-first.
//! Even indicator: repeated run of `indicator >> 1` copies of one value stored in
//! `ceil(bit_width / 8)` little-endian bytes.
use crate::error::Error;

pub struct RleDecoder<'a> {
    buf: &'a [u8],
    bit_cursor: usize,
    bit_width: u32,
    repeat_count: u64,
    literal_count: u64,
    current: u64,
}

impl<'a> RleDecoder<'a> {
    pub fn new(buf: &'a [u8], bit_width: u32) -> RleDecoder<'a> {
        assert!((1..=64).contains(&bit_width), "bit_width must be 1..=64");
        RleDecoder {
            buf,
            bit_cursor: 0,
            bit_width,
            repeat_count: 0,
            literal_count: 0,
            current: 0,
        }
    }

    fn align_to_byte(&mut self) -> usize {
        self.bit_cursor = self.bit_cursor.div_ceil(8) * 8;
        self.bit_cursor / 8
    }

    /// Returns false when the stream is exhausted.
    fn read_header(&mut self) -> Result<bool, Error> {
        if self.literal_count != 0 || self.repeat_count != 0 {
            return Ok(true);
        }
        let mut pos = self.align_to_byte();
        if pos >= self.buf.len() {
            return Ok(false);
        }
        let indicator = crate::varint::read_varint32(self.buf, &mut pos)?;
        if indicator & 1 == 1 {
            self.literal_count = u64::from(indicator >> 1) * 8;
            self.bit_cursor = pos * 8;
        } else {
            self.repeat_count = u64::from(indicator >> 1);
            let nbytes = (self.bit_width as usize).div_ceil(8);
            let bytes = self
                .buf
                .get(pos..pos + nbytes)
                .ok_or_else(|| Error::Corrupt("rle repeated value runs past end".into()))?;
            let mut v: u64 = 0;
            for (i, b) in bytes.iter().enumerate() {
                v |= u64::from(*b) << (8 * i);
            }
            self.current = v;
            self.bit_cursor = (pos + nbytes) * 8;
        }
        if self.literal_count == 0 && self.repeat_count == 0 {
            return Err(Error::Corrupt("rle run of length zero".into()));
        }
        Ok(true)
    }

    fn read_bits(&mut self, n: u32) -> Result<u64, Error> {
        let end = self.bit_cursor + n as usize;
        if end > self.buf.len() * 8 {
            return Err(Error::Corrupt("rle literal run past end of buffer".into()));
        }
        let mut v: u64 = 0;
        for i in 0..n as usize {
            let bit = self.bit_cursor + i;
            let b = (self.buf[bit / 8] >> (bit % 8)) & 1;
            v |= u64::from(b) << i;
        }
        self.bit_cursor = end;
        Ok(v)
    }

    /// Next value, or None when the stream has no more runs.
    pub fn next_value(&mut self) -> Result<Option<u64>, Error> {
        if !self.read_header()? {
            return Ok(None);
        }
        if self.repeat_count > 0 {
            self.repeat_count -= 1;
            Ok(Some(self.current))
        } else {
            self.literal_count -= 1;
            Ok(Some(self.read_bits(self.bit_width)?))
        }
    }

    /// Decode exactly `n` values; errors if the stream ends early.
    pub fn take(&mut self, n: usize) -> Result<Vec<u64>, Error> {
        let mut out = Vec::with_capacity(n);
        for _ in 0..n {
            match self.next_value()? {
                Some(v) => out.push(v),
                None => {
                    return Err(Error::Corrupt(format!(
                        "rle stream ended after {} of {n} values",
                        out.len()
                    )))
                }
            }
        }
        Ok(out)
    }
}
