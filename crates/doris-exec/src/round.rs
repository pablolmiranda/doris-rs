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

/// The rounding mode of `round`, `truncate`, `floor` and `ceil` (`round.h:111-147`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// Half away from zero.
    Round,
    Trunc,
    Floor,
    Ceil,
}

impl Mode {
    /// The mode of a rounding function, by name.
    pub fn of(name: &str) -> Option<Mode> {
        Some(match name {
            "round" => Mode::Round,
            "truncate" => Mode::Trunc,
            "floor" | "dfloor" => Mode::Floor,
            "ceil" | "dceil" | "ceiling" => Mode::Ceil,
            _ => return None,
        })
    }

    fn apply(self, x: f64) -> f64 {
        match self {
            Mode::Round => x.round(),
            Mode::Trunc => x.trunc(),
            Mode::Floor => x.floor(),
            Mode::Ceil => x.ceil(),
        }
    }
}

pub fn round_double(x: f64, d: i32) -> f64 {
    round_double_mode(x, d, Mode::Round)
}

/// A double at `d` digits in `mode` (`round.h:295-320`).
pub fn round_double_mode(x: f64, d: i32, mode: Mode) -> f64 {
    match d {
        0 => mode.apply(x),
        d if d > 0 => {
            let s = exp10(d);
            mode.apply(x * s) / s
        }
        d => {
            let s = exp10(-d);
            mode.apply(x / s) * s
        }
    }
}

/// A raw decimal at `in_scale` rounded to `digits`, as a raw value at `result_scale`.
pub fn round_decimal(raw: i128, in_scale: i32, digits: i32, result_scale: i32) -> i128 {
    round_decimal_mode(raw, in_scale, digits, result_scale, Mode::Round)
}

/// As `round_decimal`, in `mode`. More digits than the scale copy the value (`round.h:212-214`).
pub fn round_decimal_mode(raw: i128, in_scale: i32, digits: i32, result_scale: i32, mode: Mode) -> i128 {
    let scale_arg = in_scale - digits;
    if scale_arg <= 0 {
        return raw;
    }
    let target = 10i128.checked_pow((result_scale - digits).max(0) as u32).unwrap_or(i128::MAX);
    // a saturated divisor exceeds every value: only floor and ceil give one target unit
    // (`round.h:157-180`)
    let Some(scale) = 10i128.checked_pow(scale_arg as u32) else {
        return match mode {
            _ if target == i128::MAX => 0,
            Mode::Floor if raw < 0 => -target,
            Mode::Ceil if raw > 0 => target,
            _ => 0,
        };
    };
    let mut q = raw / scale;
    let rem = raw - q * scale;
    match mode {
        Mode::Round => {
            let abs_rem = rem.abs();
            if rem != 0 && abs_rem >= scale - abs_rem {
                q += if rem < 0 { -1 } else { 1 };
            }
        }
        Mode::Trunc => {}
        Mode::Floor if rem < 0 => q -= 1,
        Mode::Ceil if rem > 0 => q += 1,
        _ => {}
    }
    if target > 1 {
        q.wrapping_mul(target)
    } else {
        q
    }
}
