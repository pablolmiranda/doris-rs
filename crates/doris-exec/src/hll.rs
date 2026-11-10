//! `ndv` / `approx_count_distinct`: C++'s HyperLogLog (`core/value/hll.{h,cpp}`), precision 14,
//! an explicit set of up to 160 hashes before the registers, and its f32 estimate.
use std::collections::HashSet;

use crate::ExecError;

const PRECISION: u32 = 14;
const REGISTERS: usize = 1 << PRECISION;
/// HLL_EXPLICIT_INT64_NUM
const EXPLICIT: usize = 160;
/// HLL_ZERO_COUNT_BITS
const ZERO_COUNT_BITS: u32 = 64 - PRECISION;

/// `HashUtil::murmur_hash64A` (`util/hash_util.hpp`).
pub fn murmur64a(key: &[u8], seed: u64) -> u64 {
    const M: u64 = 0xc6a4a7935bd1e995;
    const R: u32 = 47;
    let mut h = seed ^ (key.len() as u64).wrapping_mul(M);
    let mut blocks = key.chunks_exact(8);
    for b in &mut blocks {
        let mut k = u64::from_le_bytes(b.try_into().expect("8 bytes"));
        k = k.wrapping_mul(M);
        k ^= k >> R;
        k = k.wrapping_mul(M);
        h ^= k;
        h = h.wrapping_mul(M);
    }
    let tail = blocks.remainder();
    if !tail.is_empty() {
        for (i, b) in tail.iter().enumerate().rev() {
            h ^= u64::from(*b) << (8 * i);
        }
        h = h.wrapping_mul(M);
    }
    h ^= h >> R;
    h = h.wrapping_mul(M);
    h ^= h >> R;
    h
}

#[derive(Debug, Clone, Default, PartialEq)]
pub enum Hll {
    #[default]
    Empty,
    Explicit(HashSet<u64>),
    Full(Box<[u8; REGISTERS]>),
}

fn registers_of(set: &HashSet<u64>) -> Box<[u8; REGISTERS]> {
    let mut r = Box::new([0u8; REGISTERS]);
    for h in set {
        update_register(&mut r, *h);
    }
    r
}

fn update_register(r: &mut [u8; REGISTERS], h: u64) {
    let idx = (h % REGISTERS as u64) as usize;
    let v = (h >> PRECISION) | (1u64 << ZERO_COUNT_BITS);
    let rank = (v.trailing_zeros() + 1) as u8;
    r[idx] = r[idx].max(rank);
}

impl Hll {
    /// `HyperLogLog::update`: at 160 explicit values any further hash, even a duplicate, converts.
    pub fn update(&mut self, h: u64) {
        match self {
            Hll::Empty => *self = Hll::Explicit(HashSet::from([h])),
            Hll::Explicit(set) if set.len() < EXPLICIT => {
                set.insert(h);
            }
            Hll::Explicit(set) => {
                let mut r = registers_of(set);
                update_register(&mut r, h);
                *self = Hll::Full(r);
            }
            Hll::Full(r) => update_register(r, h),
        }
    }

    /// `HyperLogLog::merge`: explicit sets stay explicit while their union holds 160 or fewer.
    pub fn merge(&mut self, other: &Hll) {
        match (&mut *self, other) {
            (_, Hll::Empty) => {}
            (Hll::Empty, o) => *self = o.clone(),
            (Hll::Explicit(a), Hll::Explicit(b)) => {
                a.extend(b.iter().copied());
                if a.len() > EXPLICIT {
                    *self = Hll::Full(registers_of(a));
                }
            }
            (Hll::Explicit(a), Hll::Full(b)) => {
                let mut r = registers_of(a);
                r.iter_mut().zip(b.iter()).for_each(|(x, y)| *x = (*x).max(*y));
                *self = Hll::Full(r);
            }
            (Hll::Full(r), Hll::Explicit(b)) => b.iter().for_each(|h| update_register(r, *h)),
            (Hll::Full(r), Hll::Full(b)) => r.iter_mut().zip(b.iter()).for_each(|(x, y)| *x = (*x).max(*y)),
        }
    }

    /// `HyperLogLog::estimate_cardinality`, its f32 steps in C++'s order.
    pub fn estimate(&self) -> i64 {
        let r = match self {
            Hll::Empty => return 0,
            Hll::Explicit(set) => return set.len() as i64,
            Hll::Full(r) => r,
        };
        let streams = REGISTERS as f32;
        let alpha = 0.7213f32 / (1.0 + 1.079f32 / streams);
        let mut harmonic = 0f32;
        let mut zeros = 0;
        for &x in r.iter() {
            harmonic += 2f32.powi(-i32::from(x));
            if x == 0 {
                zeros += 1;
            }
        }
        harmonic = 1.0 / harmonic;
        let mut e = f64::from(alpha * streams * streams * harmonic);
        let n = REGISTERS as f64;
        if e <= n * 2.5 && zeros != 0 {
            e = n * (n / f64::from(zeros)).ln();
        } else if e < 72000.0 {
            let bias =
                5.9119 * 1.0e-18 * (e * e * e * e) - 1.4253 * 1.0e-12 * (e * e * e) + 1.2940 * 1.0e-7 * (e * e) - 5.2921 * 1.0e-3 * e + 83.3216;
            e -= e * (bias / 100.0);
        }
        (e + 0.5) as i64
    }

    /// doris-rs's own partial-state encoding (the state never leaves doris-rs).
    pub fn encode(&self) -> Vec<u8> {
        match self {
            Hll::Empty => vec![0],
            Hll::Explicit(set) => {
                let mut out = vec![1];
                out.extend((set.len() as u32).to_le_bytes());
                set.iter().for_each(|h| out.extend(h.to_le_bytes()));
                out
            }
            Hll::Full(r) => {
                let mut out = vec![2];
                out.extend_from_slice(&r[..]);
                out
            }
        }
    }

    pub fn decode(b: &[u8]) -> Result<Hll, ExecError> {
        let bad = || ExecError::Internal("a malformed ndv state".into());
        match b.split_first().ok_or_else(bad)? {
            (0, _) => Ok(Hll::Empty),
            (1, rest) => {
                let n = u32::from_le_bytes(rest.get(..4).ok_or_else(bad)?.try_into().map_err(|_| bad())?) as usize;
                let body = rest.get(4..4 + 8 * n).ok_or_else(bad)?;
                Ok(Hll::Explicit(
                    body.chunks_exact(8).map(|c| u64::from_le_bytes(c.try_into().expect("8 bytes"))).collect(),
                ))
            }
            (2, rest) => Ok(Hll::Full(Box::new(rest.try_into().map_err(|_| bad())?))),
            _ => Err(bad()),
        }
    }
}
