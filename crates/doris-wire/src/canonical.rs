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
    /// A compact-protocol varint that does not end within ten bytes.
    VarintTooLong,
    /// A compact-protocol length or element count too large for this platform.
    SizeTooLarge(u64),
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

    /// `n` collection elements, each read by `read`, appended to `out`; sorted by their
    /// canonical encodings when the collection is a set.
    fn elements(
        &mut self,
        n: usize,
        sort: bool,
        out: &mut Vec<u8>,
        read: impl Fn(&mut Self, &mut Vec<u8>) -> Result<(), CanonicalError>,
    ) -> Result<(), CanonicalError> {
        let mut elems = Vec::new();
        for _ in 0..n {
            let mut e = Vec::new();
            read(self, &mut e)?;
            elems.push(e);
        }
        if sort {
            elems.sort();
        }
        elems.iter().for_each(|e| out.extend_from_slice(e));
        Ok(())
    }

    /// `n` map entries, sorted by their canonical (key, value) encodings, appended to `out`.
    fn entries(
        &mut self,
        n: usize,
        out: &mut Vec<u8>,
        key: impl Fn(&mut Self, &mut Vec<u8>) -> Result<(), CanonicalError>,
        val: impl Fn(&mut Self, &mut Vec<u8>) -> Result<(), CanonicalError>,
    ) -> Result<(), CanonicalError> {
        let mut entries = Vec::new();
        for _ in 0..n {
            let (mut k, mut v) = (Vec::new(), Vec::new());
            key(self, &mut k)?;
            val(self, &mut v)?;
            entries.push((k, v));
        }
        entries.sort();
        for (k, v) in &entries {
            out.extend_from_slice(k);
            out.extend_from_slice(v);
        }
        Ok(())
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
            LIST | SET => {
                let elem = self.byte()?;
                let n = self.size()?;
                out.extend_from_slice(&self.buf[start..self.pos]);
                self.elements(n, t == SET, out, |r, e| r.value(elem, depth + 1, e))
            }
            MAP => {
                let (key, val) = (self.byte()?, self.byte()?);
                let n = self.size()?;
                out.extend_from_slice(&self.buf[start..self.pos]);
                self.entries(
                    n,
                    out,
                    |r, e| r.value(key, depth + 1, e),
                    |r, e| r.value(val, depth + 1, e),
                )
            }
            other => Err(CanonicalError::BadType(other)),
        }
    }
}

/// The canonical form of one `TCompactProtocol` struct (such as the `TPipelineFragmentParamsList`
/// a fragment request carries): set elements and map entries sorted by their canonical
/// encodings, every other byte unchanged. Compact field ids are deltas that restart in every
/// struct, so reordering collection elements changes no other byte.
pub fn compact_struct(bytes: &[u8]) -> Result<Vec<u8>, CanonicalError> {
    let mut r = Reader { buf: bytes, pos: 0 };
    let mut out = Vec::with_capacity(bytes.len());
    r.compact_value(C_STRUCT, 0, &mut out)?;
    match bytes.len() - r.pos {
        0 => Ok(out),
        n => Err(CanonicalError::TrailingBytes(n)),
    }
}

const C_BOOL_TRUE: u8 = 1;
const C_BOOL_FALSE: u8 = 2;
const C_BYTE: u8 = 3;
const C_I16: u8 = 4;
const C_I32: u8 = 5;
const C_I64: u8 = 6;
const C_DOUBLE: u8 = 7;
const C_BINARY: u8 = 8;
const C_LIST: u8 = 9;
const C_SET: u8 = 10;
const C_MAP: u8 = 11;
const C_STRUCT: u8 = 12;
const C_UUID: u8 = 13;

impl Reader<'_> {
    /// An unsigned LEB128 varint, copied to `out`; returns its value.
    fn varint(&mut self, out: &mut Vec<u8>) -> Result<u64, CanonicalError> {
        let mut v = 0u64;
        for shift in (0..70).step_by(7) {
            let b = self.byte()?;
            out.push(b);
            v |= u64::from(b & 0x7f) << shift;
            if b & 0x80 == 0 {
                return Ok(v);
            }
        }
        Err(CanonicalError::VarintTooLong)
    }

    fn compact_size(&mut self, out: &mut Vec<u8>) -> Result<usize, CanonicalError> {
        let n = self.varint(out)?;
        usize::try_from(n).map_err(|_| CanonicalError::SizeTooLarge(n))
    }

    /// One compact value of type `t`, appended to `out` in canonical form.
    fn compact_value(
        &mut self,
        t: u8,
        depth: usize,
        out: &mut Vec<u8>,
    ) -> Result<(), CanonicalError> {
        match t {
            C_BOOL_TRUE | C_BOOL_FALSE | C_BYTE => out.push(self.byte()?),
            C_I16 | C_I32 | C_I64 => {
                self.varint(out)?;
            }
            C_DOUBLE => out.extend_from_slice(self.take(8)?),
            C_UUID => out.extend_from_slice(self.take(16)?),
            C_BINARY => {
                let n = self.compact_size(out)?;
                out.extend_from_slice(self.take(n)?);
            }
            C_STRUCT | C_LIST | C_SET | C_MAP if depth >= MAX_DEPTH => {
                return Err(CanonicalError::TooDeep)
            }
            C_STRUCT => loop {
                let header = self.byte()?;
                out.push(header);
                if header == STOP {
                    return Ok(());
                }
                let field_type = header & 0x0f;
                if header >> 4 == 0 {
                    self.varint(out)?; // long-form field id
                }
                match field_type {
                    C_BOOL_TRUE | C_BOOL_FALSE => {} // the value is the type nibble
                    C_BYTE..=C_UUID => self.compact_value(field_type, depth + 1, out)?,
                    other => return Err(CanonicalError::BadType(other)),
                }
            },
            C_LIST | C_SET => {
                let header = self.byte()?;
                out.push(header);
                let elem = header & 0x0f;
                let n = if header >> 4 == 0x0f {
                    self.compact_size(out)?
                } else {
                    usize::from(header >> 4)
                };
                if !(C_BOOL_TRUE..=C_UUID).contains(&elem) {
                    return Err(CanonicalError::BadType(elem));
                }
                self.elements(n, t == C_SET, out, |r, e| {
                    r.compact_value(elem, depth + 1, e)
                })?;
            }
            C_MAP => {
                let n = self.compact_size(out)?;
                if n == 0 {
                    return Ok(());
                }
                let kinds = self.byte()?;
                out.push(kinds);
                let (key, val) = (kinds >> 4, kinds & 0x0f);
                for k in [key, val] {
                    if !(C_BOOL_TRUE..=C_UUID).contains(&k) {
                        return Err(CanonicalError::BadType(k));
                    }
                }
                self.entries(
                    n,
                    out,
                    |r, e| r.compact_value(key, depth + 1, e),
                    |r, e| r.compact_value(val, depth + 1, e),
                )?;
            }
            other => return Err(CanonicalError::BadType(other)),
        }
        Ok(())
    }
}
