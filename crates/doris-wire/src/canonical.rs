//! Canonical form of a strict `TBinaryProtocol` message: every set's elements and every map's
//! entries sorted by their encoded bytes, everything else unchanged. Two encodings with equal
//! canonical forms differ only in the order of unordered collections.
//!
//! The Java FE writes sets and maps in `HashSet`/`HashMap` iteration order; the generated Rust
//! types are `BTreeSet`/`BTreeMap` and write them sorted (as the C++ BE's `std::set` does).

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CanonicalError {
    /// The input ended inside a value.
    Truncated,
    /// Not a strict (versioned) binary message header.
    BadHeader,
    /// A type id that is not a Thrift binary protocol type.
    BadType(u8),
    /// A negative length or element count.
    NegativeSize(i32),
    /// Structs or collections nested deeper than [`MAX_DEPTH`].
    TooDeep,
    /// Bytes left after the message.
    TrailingBytes(usize),
}

/// Nesting limit, as in the `thrift` crate's default configuration.
pub const MAX_DEPTH: usize = 64;

const STOP: u8 = 0;
const BOOL: u8 = 2;
const BYTE: u8 = 3;
const DOUBLE: u8 = 4;
const I16: u8 = 6;
const I32: u8 = 8;
const I64: u8 = 10;
const STRING: u8 = 11;
const STRUCT: u8 = 12;
const MAP: u8 = 13;
const SET: u8 = 14;
const LIST: u8 = 15;
const UUID: u8 = 16;

/// The canonical form of one whole message (header and body).
pub fn binary_message(bytes: &[u8]) -> Result<Vec<u8>, CanonicalError> {
    let mut r = Reader { buf: bytes, pos: 0 };
    let mut out = Vec::with_capacity(bytes.len());
    let version = r.take(4)?;
    if version[..2] != [0x80, 0x01] {
        return Err(CanonicalError::BadHeader);
    }
    out.extend_from_slice(version);
    let name_len = r.size()?;
    out.extend_from_slice(&r.buf[r.pos - 4..r.pos]);
    out.extend_from_slice(r.take(name_len)?);
    out.extend_from_slice(r.take(4)?); // sequence id
    r.value(STRUCT, 0, &mut out)?;
    match bytes.len() - r.pos {
        0 => Ok(out),
        n => Err(CanonicalError::TrailingBytes(n)),
    }
}

struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], CanonicalError> {
        let end = self
            .pos
            .checked_add(n)
            .filter(|end| *end <= self.buf.len())
            .ok_or(CanonicalError::Truncated)?;
        let s = &self.buf[self.pos..end];
        self.pos = end;
        Ok(s)
    }

    fn byte(&mut self) -> Result<u8, CanonicalError> {
        Ok(self.take(1)?[0])
    }

    /// A non-negative big-endian i32 length or count.
    fn size(&mut self) -> Result<usize, CanonicalError> {
        let n = i32::from_be_bytes(self.take(4)?.try_into().expect("4 bytes"));
        usize::try_from(n).map_err(|_| CanonicalError::NegativeSize(n))
    }

    /// One value of type `t`, appended to `out` in canonical form.
    fn value(&mut self, t: u8, depth: usize, out: &mut Vec<u8>) -> Result<(), CanonicalError> {
        let fixed = match t {
            BOOL | BYTE => Some(1),
            I16 => Some(2),
            I32 => Some(4),
            I64 | DOUBLE => Some(8),
            UUID => Some(16),
            _ => None,
        };
        if let Some(n) = fixed {
            out.extend_from_slice(self.take(n)?);
            return Ok(());
        }
        if t == STRING {
            let n = self.size()?;
            out.extend_from_slice(&self.buf[self.pos - 4..self.pos]);
            out.extend_from_slice(self.take(n)?);
            return Ok(());
        }
        if depth >= MAX_DEPTH {
            return Err(CanonicalError::TooDeep);
        }
        let start = self.pos;
        match t {
            STRUCT => loop {
                let field_type = self.byte()?;
                out.push(field_type);
                if field_type == STOP {
                    return Ok(());
                }
                out.extend_from_slice(self.take(2)?); // field id
                self.value(field_type, depth + 1, out)?;
            },
            LIST => {
                let elem = self.byte()?;
                let n = self.size()?;
                out.extend_from_slice(&self.buf[start..self.pos]);
                for _ in 0..n {
                    self.value(elem, depth + 1, out)?;
                }
                Ok(())
            }
            SET => {
                let elem = self.byte()?;
                let n = self.size()?;
                out.extend_from_slice(&self.buf[start..self.pos]);
                let mut elems = Vec::new();
                for _ in 0..n {
                    let mut e = Vec::new();
                    self.value(elem, depth + 1, &mut e)?;
                    elems.push(e);
                }
                elems.sort();
                elems.iter().for_each(|e| out.extend_from_slice(e));
                Ok(())
            }
            MAP => {
                let (key, val) = (self.byte()?, self.byte()?);
                let n = self.size()?;
                out.extend_from_slice(&self.buf[start..self.pos]);
                let mut entries = Vec::new();
                for _ in 0..n {
                    let (mut k, mut v) = (Vec::new(), Vec::new());
                    self.value(key, depth + 1, &mut k)?;
                    self.value(val, depth + 1, &mut v)?;
                    entries.push((k, v));
                }
                entries.sort();
                for (k, v) in &entries {
                    out.extend_from_slice(k);
                    out.extend_from_slice(v);
                }
                Ok(())
            }
            other => Err(CanonicalError::BadType(other)),
        }
    }
}
