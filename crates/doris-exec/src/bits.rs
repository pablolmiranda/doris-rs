//! Bit functions, as the C++ BE computes them (`be/src/exprs/function/function_bit.cpp`,
//! `function_bit_count.cpp`, `function_bit_shift.cpp`, `function_bit_test.cpp`, and `xor` in
//! `functions_logical.h`). Integers are held as `i128` at their type's value.
use doris_segment::writer::Datum;
use doris_thrift::gen::types::TPrimitiveType as T;

use crate::value::ColType;
use crate::ExecError;

/// The bit width of an integer type.
fn width(t: &ColType) -> Option<u32> {
    match t.ptype {
        T::TINYINT => Some(8),
        T::SMALLINT => Some(16),
        T::INT => Some(32),
        T::BIGINT => Some(64),
        T::LARGEINT => Some(128),
        _ => None,
    }
}

/// Whether `name` over `args` into `to` is a bit function this module evaluates.
pub fn supported(name: &str, args: &[ColType], to: &ColType) -> bool {
    let int = |t: &ColType| width(t).is_some();
    let same = |t: &ColType| t.ptype == to.ptype;
    match name {
        "bitand" | "bitor" | "bitxor" => args.len() == 2 && int(to) && args.iter().all(same),
        "bitnot" => args.len() == 1 && int(to) && same(&args[0]),
        "bit_count" => args.len() == 1 && int(&args[0]) && to.ptype == T::TINYINT,
        "bit_shift_left" | "bit_shift_right" => args.len() == 2 && args[0].ptype == T::BIGINT && args[1].ptype == T::TINYINT && to.ptype == T::BIGINT,
        "bit_test" | "bit_test_all" => args.len() >= 2 && int(&args[0]) && args.iter().all(|a| a.ptype == args[0].ptype) && to.ptype == T::TINYINT,
        "xor" => args.len() == 2 && args.iter().all(|a| a.ptype == T::BOOLEAN) && to.ptype == T::BOOLEAN,
        _ => false,
    }
}

/// Every row of `args` with any NULL is NULL; the others go through `f`.
fn rows(args: &[&[Datum]], rows: usize, f: impl Fn(&[&Datum]) -> Datum) -> Vec<Datum> {
    (0..rows)
        .map(|r| {
            let row: Vec<&Datum> = args.iter().map(|a| &a[r]).collect();
            if row.iter().any(|d| **d == Datum::Null) {
                Datum::Null
            } else {
                f(&row)
            }
        })
        .collect()
}

fn int(d: &Datum) -> i128 {
    match d {
        Datum::Int(v) => *v,
        Datum::Bool(b) => i128::from(*b),
        _ => 0,
    }
}

/// Evaluates bit function `name`; `types` are the arguments' types.
pub fn eval(name: &str, types: &[ColType], args: &[&[Datum]], n: usize) -> Result<Vec<Datum>, ExecError> {
    let w = types.first().and_then(width).unwrap_or(64);
    Ok(match name {
        "bitand" => rows(args, n, |r| Datum::Int(int(r[0]) & int(r[1]))),
        "bitor" => rows(args, n, |r| Datum::Int(int(r[0]) | int(r[1]))),
        "bitxor" => rows(args, n, |r| Datum::Int(int(r[0]) ^ int(r[1]))),
        "bitnot" => rows(args, n, |r| Datum::Int(!int(r[0]))),
        // the set bits of the two's-complement value at the type's width
        "bit_count" => rows(args, n, |r| {
            let v = int(r[0]);
            let ones = if w == 128 {
                (v as u128).count_ones()
            } else {
                ((v as u128) & ((1u128 << w) - 1)).count_ones()
            };
            Datum::Int(i128::from(ones))
        }),
        // a count outside 0..64 gives 0; otherwise a logical shift of the u64
        "bit_shift_left" | "bit_shift_right" => rows(args, n, |r| {
            let (v, by) = (int(r[0]) as i64 as u64, int(r[1]));
            let out = match by {
                0..=63 if name == "bit_shift_left" => v << by,
                0..=63 => v >> by,
                _ => 0,
            };
            Datum::Int(i128::from(out as i64))
        }),
        // 1 only if every position's bit is set; a position outside the width gives 0
        "bit_test" | "bit_test_all" => rows(args, n, |r| {
            let v = int(r[0]);
            let all = r[1..].iter().all(|p| {
                let p = int(p);
                (0..i128::from(w)).contains(&p) && (v >> p) & 1 == 1
            });
            Datum::Int(i128::from(all))
        }),
        "xor" => rows(args, n, |r| Datum::Bool((int(r[0]) != 0) != (int(r[1]) != 0))),
        _ => {
            return Err(ExecError::Unsupported {
                what: format!("function {name}"),
            })
        }
    })
}
