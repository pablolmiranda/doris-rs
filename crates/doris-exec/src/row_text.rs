//! The result sink's MySQL text rows (`vmysql_result_writer.cpp`, research note [wire] §3):
//! each cell is a length-encoded string, NULL is the byte `0xFB`, and a batch of rows is a
//! binary-protocol `TResultBatch`.
use doris_segment::writer::Datum;
use doris_thrift::gen::data::TResultBatch;
use doris_thrift::gen::types::TPrimitiveType as T;
use thrift::protocol::{TBinaryOutputProtocol, TSerializable};

use crate::block::Block;
use crate::value::ColType;

/// A float or double as fmt 7.1.3 prints `"{}"`: the shortest digits that read back as the
/// same value, in exponent form (`e±dd`) when the decimal exponent is below -4 or at least 16.
fn float_text(sci: String) -> String {
    // `sci` is Rust's shortest `{:e}`: "-1.2345678e16", "6.6e0", "-0e0"
    let (mantissa, exp) = sci.split_once('e').expect("{:e} has an exponent");
    let exp: i32 = exp.parse().expect("exponent");
    let (sign, mantissa) = mantissa
        .strip_prefix('-')
        .map_or(("", mantissa), |m| ("-", m));
    let digits: String = mantissa.chars().filter(|c| *c != '.').collect();
    if !(-4..16).contains(&exp) {
        let (first, rest) = digits.split_at(1);
        let dot = if rest.is_empty() {
            String::new()
        } else {
            format!(".{rest}")
        };
        let esign = if exp < 0 { '-' } else { '+' };
        return format!("{sign}{first}{dot}e{esign}{:02}", exp.abs());
    }
    if exp < 0 {
        return format!("{sign}0.{}{digits}", "0".repeat((-exp - 1) as usize));
    }
    let int_len = exp as usize + 1;
    if digits.len() <= int_len {
        format!("{sign}{digits}{}", "0".repeat(int_len - digits.len()))
    } else {
        format!("{sign}{}.{}", &digits[..int_len], &digits[int_len..])
    }
}

fn special(nan: bool, inf: bool, neg: bool) -> Option<String> {
    if nan {
        Some("NaN".into())
    } else if inf {
        Some(if neg { "-Infinity" } else { "Infinity" }.into())
    } else {
        None
    }
}

/// A double as the C++ BE prints it (`fmt` "{}"; NaN and the infinities by name).
pub fn double_text(v: f64) -> String {
    special(v.is_nan(), v.is_infinite(), v < 0.0).unwrap_or_else(|| float_text(format!("{v:e}")))
}

/// An unscaled decimal with exactly `scale` fraction digits.
pub fn decimal_text(v: i128, scale: i32) -> String {
    let digits = v.unsigned_abs().to_string();
    let sign = if v < 0 { "-" } else { "" };
    let scale = scale.max(0) as usize;
    if scale == 0 {
        return format!("{sign}{digits}");
    }
    let padded = format!("{digits:0>width$}", width = scale + 1);
    let (int, frac) = padded.split_at(padded.len() - scale);
    format!("{sign}{int}.{frac}")
}

pub fn datev2_text(v: u32) -> String {
    format!("{:04}-{:02}-{:02}", v >> 9, (v >> 5) & 0xF, v & 0x1F)
}

pub fn datetimev2_text(v: u64, scale: i32) -> String {
    let mut s = format!(
        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
        v >> 46,
        (v >> 42) & 0xF,
        (v >> 37) & 0x1F,
        (v >> 32) & 0x1F,
        (v >> 26) & 0x3F,
        (v >> 20) & 0x3F
    );
    if scale > 0 {
        let us = format!("{:06}", v & 0xF_FFFF);
        s.push('.');
        s.push_str(&us[..scale.min(6) as usize]);
    }
    s
}

/// A value's text in a result row; None for NULL.
pub fn cell_text(d: &Datum, t: &ColType) -> Option<Vec<u8>> {
    let text = match d {
        Datum::Null => return None,
        Datum::Bytes(b) => return Some(b.clone()),
        Datum::Bool(b) => u8::from(*b).to_string(),
        Datum::Float(f) => special(f.is_nan(), f.is_infinite(), *f < 0.0)
            .unwrap_or_else(|| float_text(format!("{f:e}"))),
        Datum::Double(f) => special(f.is_nan(), f.is_infinite(), *f < 0.0)
            .unwrap_or_else(|| float_text(format!("{f:e}"))),
        Datum::Int(v) => match t.ptype {
            T::DECIMAL32 | T::DECIMAL64 | T::DECIMAL128I | T::DECIMAL256 => {
                decimal_text(*v, t.scale)
            }
            T::DATEV2 => datev2_text(*v as u32),
            T::DATETIMEV2 => datetimev2_text(*v as u64, t.scale),
            T::BOOLEAN => u8::from(*v != 0).to_string(),
            _ => v.to_string(),
        },
    };
    Some(text.into_bytes())
}

/// Appends a MySQL length-encoded string.
pub fn put_lenenc(out: &mut Vec<u8>, b: &[u8]) {
    let n = b.len();
    if n < 251 {
        out.push(n as u8);
    } else if n < 1 << 16 {
        out.push(0xFC);
        out.extend_from_slice(&(n as u16).to_le_bytes());
    } else if n < 1 << 24 {
        out.push(0xFD);
        out.extend_from_slice(&(n as u32).to_le_bytes()[..3]);
    } else {
        out.push(0xFE);
        out.extend_from_slice(&(n as u64).to_le_bytes());
    }
    out.extend_from_slice(b);
}

/// One row as the MySQL text protocol sends it.
pub fn row_bytes(row: &[Datum], types: &[ColType]) -> Vec<u8> {
    let mut out = Vec::new();
    for (d, t) in row.iter().zip(types) {
        match cell_text(d, t) {
            None => out.push(0xFB),
            Some(b) => put_lenenc(&mut out, &b),
        }
    }
    out
}

/// A block's rows as the binary-protocol `TResultBatch` a `fetch_data` reply carries.
pub fn result_batch(block: &Block) -> Vec<u8> {
    let rows = block
        .rows
        .iter()
        .map(|r| row_bytes(r, &block.types))
        .collect();
    let batch = TResultBatch::new(rows, false, 0, None);
    let mut out = Vec::new();
    {
        let mut p = TBinaryOutputProtocol::new(&mut out, true);
        batch
            .write_to_out_protocol(&mut p)
            .expect("writing to a Vec");
    }
    out
}
