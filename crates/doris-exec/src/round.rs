//! `round(x[, d])` as the C++ BE computes it (`be/src/exprs/function/round.h`).
//! - DOUBLE: `round(x * 10^d) / 10^d` for d > 0, `round(x / 10^-d) * 10^-d` for d < 0, with C's
//!   `round` (half away from zero) and `int_exp10`, which saturates at `u64::MAX` past 19
//!   (`round.h:295-320`, `:464-490`; `exec/common/int_exp.h:56-65`).
//! - DECIMAL: exact integers, half away from zero, at the scale the FE planned
//!   (`round.h:111-147`, `:193-226`).

/// `int_exp10(x)` as a double.
fn exp10(x: i32) -> f64 {
    if x > 19 {
        u64::MAX as f64
    } else {
        10u64.pow(x as u32) as f64
    }
}

pub fn round_double(x: f64, d: i32) -> f64 {
    match d {
        0 => x.round(),
        d if d > 0 => {
            let s = exp10(d);
            (x * s).round() / s
        }
        d => {
            let s = exp10(-d);
            (x / s).round() * s
        }
    }
}

/// A raw decimal at `in_scale` rounded to `digits`, as a raw value at `result_scale`.
pub fn round_decimal(raw: i128, in_scale: i32, digits: i32, result_scale: i32) -> i128 {
    let scale_arg = in_scale - digits;
    if scale_arg <= 0 {
        return raw;
    }
    let Some(scale) = 10i128.checked_pow(scale_arg as u32) else { return 0 };
    let target = 10i128.checked_pow((result_scale - digits).max(0) as u32).unwrap_or(i128::MAX);
    let mut q = raw / scale;
    let rem = raw - q * scale;
    let abs_rem = rem.abs();
    if rem != 0 && abs_rem >= scale - abs_rem {
        q += if rem < 0 { -1 } else { 1 };
    }
    if target > 1 {
        q.wrapping_mul(target)
    } else {
        q
    }
}
