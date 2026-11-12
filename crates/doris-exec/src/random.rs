//! `random`/`rand` and `sleep` as the C++ BE computes them (`exprs/function/random.cpp`,
//! `function_utility.cpp`): see `docs/superpowers/research/2026-11-12-m6-s7b6-random.md`.
//!
//! C++ makes one `std::mt19937_64` per expression per fragment instance, at open, and never
//! resets it: the rows an instance evaluates take consecutive draws. Here the generators live
//! in a per-instance scope ([`with_scope`]), keyed by the expression node.
use std::cell::RefCell;
use std::collections::HashMap;

use doris_segment::writer::Datum;
use doris_thrift::gen::exprs::TExprNode;
use doris_thrift::gen::status::TStatusCode;
use doris_thrift::gen::types::TPrimitiveType as T;

use crate::value::ColType;
use crate::ExecError;

/// `std::mt19937_64`.
pub struct Mt19937_64 {
    mt: [u64; 312],
    i: usize,
}

impl Mt19937_64 {
    pub fn new(seed: u64) -> Self {
        let mut mt = [0u64; 312];
        mt[0] = seed;
        for i in 1..312 {
            mt[i] = 6364136223846793005u64.wrapping_mul(mt[i - 1] ^ (mt[i - 1] >> 62)).wrapping_add(i as u64);
        }
        Mt19937_64 { mt, i: 312 }
    }

    fn twist(&mut self) {
        const UM: u64 = 0xFFFF_FFFF_8000_0000;
        const LM: u64 = 0x7FFF_FFFF;
        for i in 0..312 {
            let x = (self.mt[i] & UM) | (self.mt[(i + 1) % 312] & LM);
            let mut xa = x >> 1;
            if x & 1 != 0 {
                xa ^= 0xB502_6F5A_A966_19E9;
            }
            self.mt[i] = self.mt[(i + 156) % 312] ^ xa;
        }
        self.i = 0;
    }

    pub fn next_u64(&mut self) -> u64 {
        if self.i >= 312 {
            self.twist();
        }
        let mut y = self.mt[self.i];
        self.i += 1;
        y ^= (y >> 29) & 0x5555_5555_5555_5555;
        y ^= (y << 17) & 0x71D6_7FFF_EDA6_0000;
        y ^= (y << 37) & 0xFFF7_EEE0_0000_0000;
        y ^ (y >> 43)
    }

    /// `uniform_real_distribution<double>(0, 1)`: one draw over 2^64, kept below 1.
    pub fn next_f64(&mut self) -> f64 {
        let v = self.next_u64() as f64 / 18446744073709551616.0;
        if v >= 1.0 {
            f64::from_bits(1.0f64.to_bits() - 1)
        } else {
            v
        }
    }
}

thread_local! {
    /// The instance's generators, and whether it stands for several C++ tasks
    static GENERATORS: RefCell<Option<(HashMap<usize, Mt19937_64>, bool)>> = const { RefCell::new(None) };
}

fn scoped<R>(shared: bool, f: impl FnOnce() -> R) -> R {
    let outer = GENERATORS.with(|g| g.borrow_mut().replace((HashMap::new(), shared)));
    let out = f();
    GENERATORS.with(|g| *g.borrow_mut() = outer);
    out
}

/// Runs `f` as one fragment instance: generators made inside it last until it returns.
pub fn with_scope<R>(f: impl FnOnce() -> R) -> R {
    scoped(false, f)
}

/// Runs `f` as a merged fragment, one run for what C++ runs as several tasks, each with its own
/// generators: a seeded generator there is refused (its draws would be one sequence, not one
/// per task).
pub fn with_shared_scope<R>(f: impl FnOnce() -> R) -> R {
    scoped(true, f)
}

fn shared() -> bool {
    GENERATORS.with(|g| g.borrow().as_ref().is_some_and(|(_, s)| *s))
}

/// An OS-random seed (`std::random_device`).
fn os_seed() -> u64 {
    use std::hash::{BuildHasher, Hasher};
    let mut h = std::collections::hash_map::RandomState::new().build_hasher();
    h.write_u128(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos()),
    );
    h.finish()
}

/// Draws from the generator of `node` in this instance, making it with `seed` first.
fn draw<R>(node: &TExprNode, seed: impl FnOnce() -> u64, f: impl FnOnce(&mut Mt19937_64) -> R) -> R {
    let key = node as *const TExprNode as usize;
    GENERATORS.with(|g| {
        let mut g = g.borrow_mut();
        match g.as_mut() {
            Some((map, _)) => f(map.entry(key).or_insert_with(|| Mt19937_64::new(seed()))),
            // outside an instance (a unit test): a fresh generator, as a fresh instance has
            None => f(&mut Mt19937_64::new(seed())),
        }
    })
}

pub fn is_random_function(name: &str) -> bool {
    matches!(name, "random" | "rand" | "sleep")
}

pub fn supported(name: &str, args: &[ColType], to: &ColType) -> bool {
    match name {
        "sleep" => matches!(args, [a] if a.ptype == T::INT) && to.ptype == T::BOOLEAN,
        _ => match args {
            [] => to.ptype == T::DOUBLE,
            [s] => s.ptype == T::BIGINT && to.ptype == T::DOUBLE,
            [a, b] => a.ptype == T::BIGINT && b.ptype == T::BIGINT && to.ptype == T::BIGINT,
            _ => false,
        },
    }
}

fn int(d: &Datum) -> Option<i128> {
    match d {
        Datum::Int(v) => Some(*v),
        _ => None,
    }
}

/// Evaluates `name` at `node` over `rows` rows. The arguments are constants (the FE requires
/// literals), read from row 0.
pub fn eval(node: &TExprNode, name: &str, args: &[&[Datum]], rows: usize) -> Result<Vec<Datum>, ExecError> {
    if name == "sleep" {
        return Ok((0..rows)
            .map(|r| match int(&args[0][r]) {
                None => Datum::Null,
                Some(n) => {
                    if n > 0 {
                        std::thread::sleep(std::time::Duration::from_secs(n as u64));
                    }
                    Datum::Bool(false)
                }
            })
            .collect());
    }
    if rows == 0 {
        return Ok(vec![]);
    }
    let first: Vec<Option<i128>> = args.iter().map(|a| int(&a[0])).collect();
    match first.as_slice() {
        [] => Ok(draw(node, os_seed, |g| (0..rows).map(|_| Datum::Double(g.next_f64())).collect())),
        // `(uint32_t)` of the seed (random.cpp)
        [Some(_)] if shared() => Err(ExecError::Unsupported {
            what: format!("a seeded {name} in a fragment run merged over several instances"),
        }),
        [Some(seed)] => Ok(draw(
            node,
            || u64::from(*seed as i64 as u32),
            |g| (0..rows).map(|_| Datum::Double(g.next_f64())).collect(),
        )),
        [Some(min), Some(max)] => {
            let (min, max) = (*min as i64, *max as i64);
            if min >= max {
                return Err(ExecError::Coded {
                    code: TStatusCode::INVALID_ARGUMENT.0,
                    name: "INVALID_ARGUMENT",
                    msg: format!("random's lower bound should less than upper bound, but got [{min}, {max})"),
                });
            }
            // unseeded, so only the range must match: [min, max], max included
            let span = (max as i128 - min as i128 + 1) as u128;
            Ok(draw(node, os_seed, |g| {
                (0..rows)
                    .map(|_| Datum::Int(i128::from(min) + (u128::from(g.next_u64()) % span) as i128))
                    .collect()
            }))
        }
        // a NULL argument: every row NULL
        _ => Ok(vec![Datum::Null; rows]),
    }
}
