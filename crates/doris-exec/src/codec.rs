//! `encode_as_{smallint,int,bigint,largeint}` / `decode_as_varchar`
//! (`function_encode_varchar.cpp`, `function_decode_varchar.cpp`), which the FE inserts to group
//! and sort short strings as integers, and the hashes `murmur_hash3_32`, `murmur_hash3_64` (Doris's
//! own 64-bit variant), `xxhash_32` (XXH32) and `xxhash_64` (XXH3): see
//! `docs/superpowers/research/2026-11-11-m6-s7b5-strings-hashes.md` §2-3.
use doris_segment::writer::Datum;
use doris_thrift::gen::status::TStatusCode;
use doris_thrift::gen::types::TPrimitiveType as T;

use crate::value::ColType;
use crate::ExecError;

const NAMES: [&str; 9] = [
    "encode_as_smallint",
    "encode_as_int",
    "encode_as_bigint",
    "encode_as_largeint",
    "decode_as_varchar",
    "murmur_hash3_32",
    "murmur_hash3_64",
    "xxhash_32",
    "xxhash_64",
];

pub fn is_codec_function(name: &str) -> bool {
    NAMES.contains(&name)
}

fn text(t: &ColType) -> bool {
    matches!(t.ptype, T::CHAR | T::VARCHAR | T::STRING)
}

/// The integer type an encoder makes, and its width in bytes.
fn encoded(name: &str) -> Option<(T, usize)> {
    Some(match name {
        "encode_as_smallint" => (T::SMALLINT, 2),
        "encode_as_int" => (T::INT, 4),
        "encode_as_bigint" => (T::BIGINT, 8),
        "encode_as_largeint" => (T::LARGEINT, 16),
        _ => return None,
    })
}

fn width(t: T) -> Option<usize> {
    Some(match t {
        T::SMALLINT => 2,
        T::INT => 4,
        T::BIGINT => 8,
        T::LARGEINT => 16,
        _ => return None,
    })
}

pub fn supported(name: &str, args: &[ColType], to: &ColType) -> bool {
    if let Some((t, _)) = encoded(name) {
        return matches!(args, [a] if text(a)) && to.ptype == t;
    }
    match name {
        "decode_as_varchar" => matches!(args, [a] if width(a.ptype).is_some()) && text(to),
        "murmur_hash3_32" | "xxhash_32" => !args.is_empty() && args.iter().all(text) && to.ptype == T::INT,
        "murmur_hash3_64" | "xxhash_64" => !args.is_empty() && args.iter().all(text) && to.ptype == T::BIGINT,
        _ => false,
    }
}

fn bytes(d: &Datum) -> &[u8] {
    match d {
        Datum::Bytes(b) => b,
        _ => &[],
    }
}

/// `encode`: the string big-endian from the top byte, its length doubled in the low byte,
/// shifted right once (so the value is never negative).
fn encode(s: &[u8], n: usize) -> i128 {
    let mut v: u128 = 0;
    for (i, &c) in s.iter().enumerate() {
        v |= u128::from(c) << (8 * (n - 1 - i));
    }
    v |= (s.len() as u128) << 1;
    let v = v >> 1;
    match n {
        2 => i128::from(v as u16 as i16),
        4 => i128::from(v as u32 as i32),
        8 => i128::from(v as u64 as i64),
        _ => v as i128,
    }
}

/// `decode`: None when the length byte says more than the type holds.
fn decode(x: i128, n: usize) -> Option<Vec<u8>> {
    let len = ((x as u8) & 0x7f) as usize;
    if len >= n {
        return None;
    }
    let v = (x as u128).wrapping_shl(1);
    Some((0..len).map(|j| (v >> (8 * (n - 1 - j))) as u8).collect())
}

fn murmur3_32(data: &[u8], seed: u32) -> u32 {
    const C1: u32 = 0xcc9e_2d51;
    const C2: u32 = 0x1b87_3593;
    let mut h = seed;
    let mut chunks = data.chunks_exact(4);
    for b in &mut chunks {
        let mut k = u32::from_le_bytes([b[0], b[1], b[2], b[3]]);
        k = k.wrapping_mul(C1).rotate_left(15).wrapping_mul(C2);
        h ^= k;
        h = h.rotate_left(13).wrapping_mul(5).wrapping_add(0xe654_6b64);
    }
    let tail = chunks.remainder();
    if !tail.is_empty() {
        let mut k = 0u32;
        for (i, &b) in tail.iter().enumerate() {
            k |= u32::from(b) << (8 * i);
        }
        h ^= k.wrapping_mul(C1).rotate_left(15).wrapping_mul(C2);
    }
    h ^= data.len() as u32;
    h ^= h >> 16;
    h = h.wrapping_mul(0x85eb_ca6b);
    h ^= h >> 13;
    h = h.wrapping_mul(0xc2b2_ae35);
    h ^ (h >> 16)
}

/// `fmix64` as Doris has it (`murmur_hash3.cpp:97-105`): its second constant is
/// `0xc4ceb9fe1a85ec53`, not the published `0xc4ceb3fe1a85ec53`.
fn fmix64(mut k: u64) -> u64 {
    k ^= k >> 33;
    k = k.wrapping_mul(0xff51_afd7_ed55_8ccd);
    k ^= k >> 33;
    k = k.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    k ^ (k >> 33)
}

/// Doris's `murmur_hash3_x64_64` (`murmur_hash3.cpp:486-548`): one 64-bit lane over 8-byte
/// blocks, not the 128-bit algorithm cut short.
fn murmur3_64(data: &[u8], seed: u64) -> u64 {
    const C1: u64 = 0x87c3_7b91_1142_53d5;
    const C2: u64 = 0x4cf5_ad43_2745_937f;
    let mix = |k: u64| k.wrapping_mul(C1).rotate_left(31).wrapping_mul(C2);
    let mut h = seed;
    let mut chunks = data.chunks_exact(8);
    for b in &mut chunks {
        h ^= mix(u64::from_le_bytes(b.try_into().expect("8 bytes")));
        h = h.rotate_left(27).wrapping_mul(5).wrapping_add(0x52dc_e729);
    }
    let mut k = 0u64;
    for (i, &b) in chunks.remainder().iter().enumerate() {
        k |= u64::from(b) << (8 * i);
    }
    h ^= mix(k);
    h ^= data.len() as u64;
    fmix64(h)
}

fn coded(code: TStatusCode, name: &'static str, msg: String) -> ExecError {
    ExecError::Coded { code: code.0, name, msg }
}

pub fn eval(name: &str, types: &[ColType], args: &[&[Datum]], rows: usize) -> Result<Vec<Datum>, ExecError> {
    let null = |r: usize| args.iter().any(|a| a[r] == Datum::Null);
    if let Some((_, n)) = encoded(name) {
        // the column's longest value decides, before any row
        let longest = args[0].iter().map(|d| bytes(d).len()).max().unwrap_or(0);
        if longest > n - 1 {
            return Err(coded(
                TStatusCode::INTERNAL_ERROR,
                "INTERNAL_ERROR",
                format!(
                    "String is too long to encode, input string size {longest}, max valid string size for {name} is {}",
                    n - 1
                ),
            ));
        }
        return Ok((0..rows)
            .map(|r| {
                if null(r) {
                    Datum::Null
                } else {
                    Datum::Int(encode(bytes(&args[0][r]), n))
                }
            })
            .collect());
    }
    if name == "decode_as_varchar" {
        let n = width(types[0].ptype).ok_or_else(|| ExecError::Unsupported {
            what: format!("decode_as_varchar of {}", types[0].type_name()),
        })?;
        return (0..rows)
            .map(|r| {
                let Datum::Int(x) = args[0][r] else { return Ok(Datum::Null) };
                decode(x, n).map(Datum::Bytes).ok_or_else(|| {
                    coded(
                        TStatusCode::INVALID_ARGUMENT,
                        "INVALID_ARGUMENT",
                        format!(
                            "Invalid input of function decode_as_varchar, input type {} value {x}, string size {}, should not be larger than {n}",
                            types[0].type_name(),
                            (x as u8) & 0x7f
                        ),
                    )
                })
            })
            .collect();
    }
    // the hashes: each argument seeded with the previous result
    Ok((0..rows)
        .map(|r| {
            if null(r) {
                return Datum::Null;
            }
            let parts = args.iter().map(|a| bytes(&a[r]));
            match name {
                "murmur_hash3_32" => Datum::Int(i128::from(parts.fold(104_729u32, |h, b| murmur3_32(b, h)) as i32)),
                "xxhash_32" => Datum::Int(i128::from(parts.fold(0u32, twox_hash::XxHash32::oneshot) as i32)),
                "murmur_hash3_64" => Datum::Int(i128::from(parts.fold(0u64, |h, b| murmur3_64(b, h)) as i64)),
                _ => Datum::Int(i128::from(parts.fold(0u64, twox_hash::XxHash3_64::oneshot_with_seed) as i64)),
            }
        })
        .collect())
}
