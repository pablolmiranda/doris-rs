//! `KeyCoder` (`storage/key_coder.h`): keys as bytes that compare like the values. Each key
//! column is `0x01` (KEY_NULL_FIRST_MARKER) for NULL, else `0x02` (KEY_NORMAL_MARKER) and the
//! value: signed integers, decimals and DATETIME (v1) sign-flipped big-endian; BOOL, DATE (v1,
//! 3 bytes), DATEV2 and DATETIMEV2 plain big-endian; CHAR the padded value; VARCHAR/STRING the
//! bytes. A short key (`_encode_keys`) covers the first `num_short_key_columns` key columns and
//! cuts strings to `index_length`; a full key (`_full_encode_keys`, segment min/max) covers
//! every key column whole.
use crate::column::field_type::*;
use crate::column::element_size;
use crate::writer::{char_padded, date_v1, datetime_v1, ColumnSpec, Datum, SegmentSchema};

const KEY_NULL_FIRST_MARKER: u8 = 0x01;
const KEY_NORMAL_MARKER: u8 = 0x02;

/// The encoded key of `row` (a full row in schema order).
pub fn encode_key(schema: &SegmentSchema, row: &[Datum], short: bool) -> Vec<u8> {
    let keys = schema.columns.iter().enumerate().filter(|(_, c)| c.is_key);
    let take = if short { schema.num_short_key_columns as usize } else { usize::MAX };
    let mut out = Vec::new();
    for (i, spec) in keys.take(take) {
        match &row[i] {
            Datum::Null => out.push(KEY_NULL_FIRST_MARKER),
            d => {
                out.push(KEY_NORMAL_MARKER);
                encode_value(spec, d, short, &mut out);
            }
        }
    }
    out
}

fn encode_value(spec: &ColumnSpec, d: &Datum, short: bool, out: &mut Vec<u8>) {
    let ft = spec.field_type;
    match (ft, d) {
        (CHAR, Datum::Bytes(b)) => {
            let padded = char_padded(b, spec.length as usize);
            let n = if short { (spec.index_length as usize).min(padded.len()) } else { padded.len() };
            out.extend_from_slice(&padded[..n]);
        }
        (VARCHAR | STRING, Datum::Bytes(b)) => {
            let n = if short { (spec.index_length as usize).min(b.len()) } else { b.len() };
            out.extend_from_slice(&b[..n]);
        }
        (BOOL, Datum::Bool(v)) => out.push(u8::from(*v)),
        (DATEV2, Datum::Int(v)) => out.extend_from_slice(&(*v as u32).to_be_bytes()),
        (DATETIMEV2, Datum::Int(v)) => out.extend_from_slice(&(*v as u64).to_be_bytes()),
        // v1 dates: the stored value, DATE unsigned in 3 bytes, DATETIME as a signed integer
        (DATE, Datum::Int(v)) => out.extend_from_slice(&date_v1(*v).to_be_bytes()[1..]),
        (DATETIME, Datum::Int(v)) => out.extend_from_slice(&((datetime_v1(*v) as u64) ^ (1 << 63)).to_be_bytes()),
        (_, Datum::Int(v)) => {
            let size = element_size(ft).unwrap_or(16);
            let bits = size * 8;
            // two's complement in `size` bytes with the sign bit flipped, big-endian
            let unsigned = (*v as u128) & (u128::MAX >> (128 - bits));
            let flipped = unsigned ^ (1u128 << (bits - 1));
            out.extend_from_slice(&flipped.to_be_bytes()[16 - size..]);
        }
        (_, Datum::Float(v)) => out.extend_from_slice(&v.to_be_bytes()),
        (_, Datum::Double(v)) => out.extend_from_slice(&v.to_be_bytes()),
        (_, Datum::Bytes(b)) => out.extend_from_slice(b),
        (_, Datum::Bool(v)) => out.push(u8::from(*v)),
        (_, Datum::Null) => unreachable!("NULL is a marker"),
        // an ARRAY is never a key column; array columns are refused before a write
        (_, Datum::Array(_) | Datum::Map(_) | Datum::Struct(_)) => unreachable!("a nested key"),
    }
}
