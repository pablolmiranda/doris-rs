//! Ports of the glibc 2.35 (aarch64) math functions the reference BE calls and that are not
//! correctly rounded, so `core_math` gives other last bits: `cbrt`, `sinh`, `cosh`, `tanh`,
//! `asinh`, `log10` (and `expm1`, which they use). Each was checked against the reference
//! container's libm with 0 mismatches over ~4 000 inputs: see
//! `docs/superpowers/research/2026-11-11-m6-s7b5-math.md` ("glibc ports").
//!
//! GCC contracts `a*b+c` into an FMA on aarch64: `mul_add` stands exactly where it did, and
//! nowhere else. Do not rewrite these expressions; the constants are glibc's literals.
#![allow(clippy::excessive_precision, clippy::unreadable_literal, clippy::approx_constant, clippy::eq_op)]

fn hi(x: f64) -> u32 {
    (x.to_bits() >> 32) as u32
}

fn lo(x: f64) -> u32 {
    x.to_bits() as u32
}

fn set_hi(x: f64, h: u32) -> f64 {
    f64::from_bits((u64::from(h) << 32) | (x.to_bits() & 0xffff_ffff))
}

fn exp(x: f64) -> f64 {
    core_math::exp(x)
}

fn log(x: f64) -> f64 {
    core_math::log(x)
}

/// `sysdeps/ieee754/dbl-64/s_cbrt.c`.
pub fn cbrt(x: f64) -> f64 {
    const CBRT2: f64 = 1.2599210498948731648;
    const SQR_CBRT2: f64 = 1.5874010519681994748;
    const FACTOR: [f64; 5] = [1.0 / SQR_CBRT2, 1.0 / CBRT2, 1.0, CBRT2, SQR_CBRT2];
    if x == 0.0 || !x.is_finite() {
        return x + x;
    }
    // frexp(|x|): the mantissa in [0.5, 1)
    let (mut bits, mut adj) = (x.abs().to_bits(), 0);
    if bits >> 52 == 0 {
        bits = (x.abs() * f64::from_bits(0x4350_0000_0000_0000)).to_bits();
        adj = -54;
    }
    let xe = ((bits >> 52) as i32) - 1022 + adj;
    let xm = f64::from_bits((bits & 0x000f_ffff_ffff_ffff) | (1022u64 << 52));
    let u = (-0.145263899385486377f64).mul_add(xm, 0.784932344976639262);
    let u = u.mul_add(xm, -1.83469277483613086);
    let u = u.mul_add(xm, 2.44693122563534430);
    let u = u.mul_add(xm, -2.11499494167371287);
    let u = u.mul_add(xm, 1.50819193781584896);
    let u = u.mul_add(xm, 0.354895765043919860);
    let t2 = u * u * u;
    // C's % and / truncate toward zero, as Rust's do
    let ym = u * (t2 + 2.0 * xm) / (2.0 * t2 + xm) * FACTOR[(2 + xe % 3) as usize];
    let r = ym * 2f64.powi(xe / 3);
    if x < 0.0 {
        -r
    } else {
        r
    }
}

/// `sysdeps/ieee754/dbl-64/s_expm1.c` (fdlibm, glibc's Estrin form).
pub fn expm1(mut x: f64) -> f64 {
    const Q: [f64; 6] = [
        1.0,
        -3.33333333333331316428e-02,
        1.58730158725481460165e-03,
        -7.93650757867487942473e-05,
        4.00821782732936239552e-06,
        -2.01099218183624371326e-07,
    ];
    const LN2_HI: f64 = 6.93147180369123816490e-01;
    const LN2_LO: f64 = 1.90821492927058770002e-10;
    const INVLN2: f64 = 1.44269504088896338700e+00;
    let mut hx = hi(x);
    let xsb = hx & 0x8000_0000;
    hx &= 0x7fff_ffff;
    if hx >= 0x4043_687A {
        if hx >= 0x4086_2E42 {
            if hx >= 0x7ff0_0000 {
                return if ((hx & 0xfffff) | lo(x)) != 0 {
                    x + x
                } else if xsb == 0 {
                    x
                } else {
                    -1.0
                };
            }
            if x > 7.09782712893383973096e+02 {
                return f64::INFINITY;
            }
        }
        if xsb != 0 {
            return 1.0e-300 - 1.0;
        }
    }
    let (k, c): (i32, f64);
    if hx > 0x3fd6_2e42 {
        let (h, l);
        if hx < 0x3FF0_A2B2 {
            if xsb == 0 {
                h = x - LN2_HI;
                l = LN2_LO;
                k = 1;
            } else {
                h = x + LN2_HI;
                l = -LN2_LO;
                k = -1;
            }
        } else {
            k = INVLN2.mul_add(x, if xsb == 0 { 0.5 } else { -0.5 }) as i32;
            let t = f64::from(k);
            h = (-t).mul_add(LN2_HI, x);
            l = t * LN2_LO;
        }
        x = h - l;
        c = (h - x) - l;
    } else if hx < 0x3c90_0000 {
        return x;
    } else {
        k = 0;
        c = 0.0;
    }
    let hfx = 0.5 * x;
    let hxs = x * hfx;
    let r1 = hxs.mul_add(Q[1], 1.0);
    let h2 = hxs * hxs;
    let r2 = hxs.mul_add(Q[3], Q[2]);
    let h4 = h2 * h2;
    let r3 = hxs.mul_add(Q[5], Q[4]);
    let r1 = h4.mul_add(r3, h2.mul_add(r2, r1));
    let t = (-r1).mul_add(hfx, 3.0);
    let mut e = hxs * ((r1 - t) / (-x).mul_add(t, 6.0));
    if k == 0 {
        return x - x.mul_add(e, -hxs);
    }
    e = x.mul_add(e - c, -c);
    e -= hxs;
    if k == -1 {
        return 0.5 * (x - e) - 0.5;
    }
    if k == 1 {
        return if x < -0.25 { -2.0 * (e - (x + 0.5)) } else { 2.0f64.mul_add(x - e, 1.0) };
    }
    let add_k = |y: f64| set_hi(y, hi(y).wrapping_add((k << 20) as u32));
    if k <= -2 || k > 56 {
        return add_k(1.0 - (e - x)) - 1.0;
    }
    if k < 20 {
        // t = 1 - 2^-k
        add_k(set_hi(1.0, 0x3ff0_0000 - (0x20_0000 >> k)) - (e - x))
    } else {
        // t = 2^-k
        add_k(x - (e + f64::from_bits(((0x3ff - k) as u64) << 52)) + 1.0)
    }
}

/// `e_sinh.c` (fdlibm).
pub fn sinh(x: f64) -> f64 {
    let ix = hi(x) & 0x7fff_ffff;
    if ix >= 0x7ff0_0000 {
        return x + x;
    }
    let h = if x.is_sign_negative() { -0.5 } else { 0.5 };
    // |x| < 22
    if ix < 0x4036_0000 {
        // |x| < 2^-28
        if ix < 0x3e30_0000 {
            return x;
        }
        let t = expm1(x.abs());
        if ix < 0x3ff0_0000 {
            return h * (2.0 * t - t * t / (t + 1.0));
        }
        return h * (t + t / (t + 1.0));
    }
    if ix < 0x4086_2e42 {
        return h * exp(x.abs());
    }
    if ix < 0x4086_33ce || (ix == 0x4086_33ce && lo(x) <= 0x8fb9_f87d) {
        let w = exp(0.5 * x.abs());
        let t = h * w;
        return t * w;
    }
    x * 1.0e307
}

/// `e_cosh.c` (fdlibm).
pub fn cosh(x: f64) -> f64 {
    let ix = hi(x) & 0x7fff_ffff;
    if ix < 0x4036_0000 {
        if ix < 0x3fd6_2e43 {
            if ix < 0x3c80_0000 {
                return 1.0;
            }
            let t = expm1(x.abs());
            let w = 1.0 + t;
            return 1.0 + (t * t) / (w + w);
        }
        let t = exp(x.abs());
        return 0.5 * t + 0.5 / t;
    }
    if ix < 0x4086_2e42 {
        return 0.5 * exp(x.abs());
    }
    if ix < 0x4086_33ce || (ix == 0x4086_33ce && lo(x) <= 0x8fb9_f87d) {
        let w = exp(0.5 * x.abs());
        let t = 0.5 * w;
        return t * w;
    }
    if ix >= 0x7ff0_0000 {
        return x * x;
    }
    f64::INFINITY
}

/// `s_tanh.c` (fdlibm).
pub fn tanh(x: f64) -> f64 {
    let ix = hi(x) & 0x7fff_ffff;
    if ix >= 0x7ff0_0000 {
        return if x.is_sign_negative() { 1.0 / x - 1.0 } else { 1.0 / x + 1.0 };
    }
    let z = if ix < 0x4036_0000 {
        if x == 0.0 {
            return x;
        }
        if ix < 0x3c80_0000 {
            return x * (1.0 + x);
        }
        if ix >= 0x3ff0_0000 {
            let t = expm1(2.0 * x.abs());
            1.0 - 2.0 / (t + 2.0)
        } else {
            let t = expm1(-2.0 * x.abs());
            -t / (t + 2.0)
        }
    } else {
        1.0 - 1.0e-300
    };
    if x.is_sign_negative() {
        -z
    } else {
        z
    }
}

/// `s_asinh.c` (fdlibm).
pub fn asinh(x: f64) -> f64 {
    let ix = hi(x) & 0x7fff_ffff;
    if ix < 0x3e30_0000 {
        return x;
    }
    let w = if ix > 0x41b0_0000 {
        if ix >= 0x7ff0_0000 {
            return x + x;
        }
        log(x.abs()) + 6.93147180559945286227e-01
    } else {
        let xa = x.abs();
        if ix > 0x4000_0000 {
            log(2.0 * xa + 1.0 / ((xa * xa + 1.0).sqrt() + xa))
        } else {
            let t = xa * xa;
            libm::log1p(xa + t / (1.0 + (1.0 + t).sqrt()))
        }
    };
    w.copysign(x)
}

/// `e_log10.c` (fdlibm). The `x <= 0 → NULL` rule is the caller's (`function_math_log.h`).
pub fn log10(mut x: f64) -> f64 {
    const TWO54: f64 = 1.80143985094819840000e+16;
    const IVLN10: f64 = 4.34294481903251816668e-01;
    const LOG10_2HI: f64 = 3.01029995663611771306e-01;
    const LOG10_2LO: f64 = 3.69423907715893078616e-13;
    let mut hx = hi(x) as i32;
    let mut k = 0i32;
    if hx < 0x0010_0000 {
        if x == 0.0 {
            return -TWO54 / x.abs();
        }
        if hx < 0 {
            return (x - x) / (x - x);
        }
        k -= 54;
        x *= TWO54;
        hx = hi(x) as i32;
    }
    if hx >= 0x7ff0_0000 {
        return x + x;
    }
    k += (hx >> 20) - 1023;
    let i = ((k as u32) >> 31) as i32;
    hx = (hx & 0x000f_ffff) | ((0x3ff - i) << 20);
    let y = f64::from(k + i);
    x = set_hi(x, hx as u32);
    // matched every sample uncontracted
    let z = y * LOG10_2LO + IVLN10 * log(x);
    z + y * LOG10_2HI
}
