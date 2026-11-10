//! The moment aggregates, as the C++ BE computes them (`be/src/exprs/aggregate/`: `moments.h`,
//! `aggregate_function_statistic.h`, `aggregate_function_regr_union.h`,
//! `aggregate_function_corr.h`, `aggregate_function_covar.h`, `aggregate_function_avg_weighted.h`),
//! every formula in C++'s evaluation order. Every argument is a DOUBLE; a row with a NULL
//! argument is skipped. A partial state is doris-rs's own encoding.

/// One function's running state.
#[derive(Debug, Clone, PartialEq)]
pub enum Moments {
    /// `VarMoments`: the count and the power sums up to `order` (3 skew, 4 kurt)
    Power {
        m: [f64; 5],
        order: u8,
    },
    /// Youngs–Cramer (`regr_slope`, `regr_intercept`)
    Regr {
        sx: f64,
        sy: f64,
        sxx: f64,
        syy: f64,
        sxy: f64,
        n: u64,
        intercept: bool,
    },
    Corr {
        m0: f64,
        x1: f64,
        y1: f64,
        xy: f64,
        x2: f64,
        y2: f64,
    },
    CorrWelford {
        mean_x: f64,
        mean_y: f64,
        c2: f64,
        m2x: f64,
        m2y: f64,
        count: i64,
    },
    Covar {
        sum_x: f64,
        sum_y: f64,
        sum_xy: f64,
        count: i64,
        samp: bool,
    },
    AvgWeighted {
        data_sum: f64,
        weight_sum: f64,
    },
}

/// The functions this module computes, by name and argument count.
pub fn arity(name: &str) -> Option<usize> {
    Some(match name {
        "kurt" | "kurt_pop" | "kurtosis" | "skew" | "skew_pop" | "skewness" => 1,
        "regr_slope" | "regr_intercept" | "corr" | "corr_welford" | "covar" | "covar_pop" | "covar_samp" | "avg_weighted" => 2,
        _ => return None,
    })
}

impl Moments {
    pub fn new(name: &str) -> Option<Moments> {
        Some(match name {
            "skew" | "skew_pop" | "skewness" => Moments::Power { m: [0.0; 5], order: 3 },
            "kurt" | "kurt_pop" | "kurtosis" => Moments::Power { m: [0.0; 5], order: 4 },
            "regr_slope" | "regr_intercept" => Moments::Regr {
                sx: 0.0,
                sy: 0.0,
                sxx: 0.0,
                syy: 0.0,
                sxy: 0.0,
                n: 0,
                intercept: name == "regr_intercept",
            },
            "corr" => Moments::Corr {
                m0: 0.0,
                x1: 0.0,
                y1: 0.0,
                xy: 0.0,
                x2: 0.0,
                y2: 0.0,
            },
            "corr_welford" => Moments::CorrWelford {
                mean_x: 0.0,
                mean_y: 0.0,
                c2: 0.0,
                m2x: 0.0,
                m2y: 0.0,
                count: 0,
            },
            "covar" | "covar_pop" | "covar_samp" => Moments::Covar {
                sum_x: 0.0,
                sum_y: 0.0,
                sum_xy: 0.0,
                count: 0,
                samp: name == "covar_samp",
            },
            "avg_weighted" => Moments::AvgWeighted {
                data_sum: 0.0,
                weight_sum: 0.0,
            },
            _ => return None,
        })
    }

    /// Adds a row; `a` is the first argument (y for regr), `b` the second.
    pub fn add(&mut self, a: f64, b: f64) {
        match self {
            Moments::Power { m, .. } => {
                let x = a;
                m[0] += 1.0;
                m[1] += x;
                m[2] += x * x;
                m[3] += (x * x) * x;
                m[4] += ((x * x) * x) * x;
            }
            Moments::Regr {
                sx, sy, sxx, syy, sxy, n, ..
            } => {
                let (y, x) = (a, b);
                *sx += x;
                *sy += y;
                if *n == 0 {
                    *n = 1;
                    return;
                }
                let n_old = *n as f64;
                let n_new = n_old + 1.0;
                let scale = 1.0 / (n_new * n_old);
                *n += 1;
                let tx = x * n_new - *sx;
                let ty = y * n_new - *sy;
                *sxx += (tx * tx) * scale;
                *syy += (ty * ty) * scale;
                *sxy += (tx * ty) * scale;
            }
            Moments::Corr { m0, x1, y1, xy, x2, y2 } => {
                let (x, y) = (a, b);
                *m0 += 1.0;
                *x1 += x;
                *y1 += y;
                *xy += x * y;
                *x2 += x * x;
                *y2 += y * y;
            }
            Moments::CorrWelford {
                mean_x,
                mean_y,
                c2,
                m2x,
                m2y,
                count,
            } => {
                let (x, y) = (a, b);
                *count += 1;
                let dx = x - *mean_x;
                let dy = y - *mean_y;
                *mean_x += dx / *count as f64;
                *mean_y += dy / *count as f64;
                // the updated means
                *c2 += dx * (y - *mean_y);
                *m2x += dx * (x - *mean_x);
                *m2y += dy * (y - *mean_y);
            }
            Moments::Covar {
                sum_x, sum_y, sum_xy, count, ..
            } => {
                *sum_x += a;
                *sum_y += b;
                *sum_xy += a * b;
                *count += 1;
            }
            Moments::AvgWeighted { data_sum, weight_sum } => {
                *data_sum += a * b;
                *weight_sum += b;
            }
        }
    }

    pub fn merge(&mut self, other: &Moments) {
        match (self, other) {
            (Moments::Power { m, .. }, Moments::Power { m: r, .. }) => {
                for (a, b) in m.iter_mut().zip(r) {
                    *a += b;
                }
            }
            (me @ Moments::Regr { .. }, rhs @ Moments::Regr { .. }) => {
                let (
                    Moments::Regr {
                        sx, sy, sxx, syy, sxy, n, ..
                    },
                    Moments::Regr {
                        sx: rsx,
                        sy: rsy,
                        sxx: rsxx,
                        syy: rsyy,
                        sxy: rsxy,
                        n: rn,
                        ..
                    },
                ) = (&mut *me, rhs)
                else {
                    unreachable!()
                };
                if *rn == 0 {
                    return;
                }
                if *n == 0 {
                    *me = rhs.clone();
                    return;
                }
                let (n1, n2) = (*n as f64, *rn as f64);
                let nsum = n1 + n2;
                let dx = *sx / n1 - rsx / n2;
                let dy = *sy / n1 - rsy / n2;
                *n += rn;
                *sx += rsx;
                *sy += rsy;
                *sxx += rsxx + (((n1 * n2) * dx) * dx) / nsum;
                *syy += rsyy + (((n1 * n2) * dy) * dy) / nsum;
                *sxy += rsxy + (((n1 * n2) * dx) * dy) / nsum;
            }
            (
                Moments::Corr { m0, x1, y1, xy, x2, y2 },
                Moments::Corr {
                    m0: a,
                    x1: b,
                    y1: c,
                    xy: d,
                    x2: e,
                    y2: f,
                },
            ) => {
                *m0 += a;
                *x1 += b;
                *y1 += c;
                *xy += d;
                *x2 += e;
                *y2 += f;
            }
            (me @ Moments::CorrWelford { .. }, rhs @ Moments::CorrWelford { .. }) => {
                let (
                    Moments::CorrWelford {
                        mean_x,
                        mean_y,
                        c2,
                        m2x,
                        m2y,
                        count,
                    },
                    Moments::CorrWelford {
                        mean_x: rx,
                        mean_y: ry,
                        c2: rc2,
                        m2x: rm2x,
                        m2y: rm2y,
                        count: rc,
                    },
                ) = (&mut *me, rhs)
                else {
                    unreachable!()
                };
                if *rc == 0 {
                    return;
                }
                if *count == 0 {
                    *me = rhs.clone();
                    return;
                }
                let dx = rx - *mean_x;
                let dy = ry - *mean_y;
                let (c, r) = (*count as f64, *rc as f64);
                let tot = c + r;
                *mean_x = (*mean_x * c + rx * r) / tot;
                *mean_y = (*mean_y * c + ry * r) / tot;
                *c2 += rc2 + (((dx * dy) * c) * r) / tot;
                *m2x += rm2x + (((dx * dx) * c) * r) / tot;
                *m2y += rm2y + (((dy * dy) * c) * r) / tot;
                *count += rc;
            }
            (
                Moments::Covar {
                    sum_x, sum_y, sum_xy, count, ..
                },
                Moments::Covar {
                    sum_x: a,
                    sum_y: b,
                    sum_xy: c,
                    count: d,
                    ..
                },
            ) => {
                if *d == 0 {
                    return;
                }
                *sum_x += a;
                *sum_y += b;
                *sum_xy += c;
                *count += d;
            }
            (Moments::AvgWeighted { data_sum, weight_sum }, Moments::AvgWeighted { data_sum: a, weight_sum: b }) => {
                *data_sum += a;
                *weight_sum += b;
            }
            _ => {}
        }
    }

    /// The result, None for NULL. `accepted`: some row had no NULL argument (the variadic NULL
    /// wrapper's flag: without one, corr, covar and avg_weighted are NULL).
    pub fn finish(&self, accepted: bool) -> Option<f64> {
        let nan_null = |v: f64| (!v.is_nan()).then_some(v);
        match self {
            Moments::Power { m, order } => {
                let m0 = m[0];
                if m0 == 0.0 {
                    return None;
                }
                let var = ((m[2] - (m[1] * m[1]) / m0) / m0).max(0.0);
                let moment = |order: u8| -> f64 {
                    if m0 == 1.0 {
                        return 0.0;
                    }
                    match order {
                        3 => (m[3] - ((3.0 * m[2] - ((2.0 * m[1]) * m[1]) / m0) * m[1]) / m0) / m0,
                        _ => (m[4] - ((4.0 * m[3] - ((6.0 * m[2] - ((3.0 * m[1]) * m[1]) / m0) * m[1]) / m0) * m[1]) / m0) / m0,
                    }
                };
                let mo = moment(*order);
                if var.is_nan() || mo.is_nan() || var <= 0.0 {
                    return None;
                }
                Some(if *order == 3 { mo / var.powf(1.5) } else { mo / (var * var) - 3.0 })
            }
            Moments::Regr {
                sx,
                sy,
                sxx,
                sxy,
                n,
                intercept,
                ..
            } => {
                if *n < 1 || *sxx == 0.0 {
                    return None;
                }
                nan_null(if *intercept { (sy - (sx * sxy) / sxx) / *n as f64 } else { sxy / sxx })
            }
            _ if !accepted => None,
            Moments::Corr { m0, x1, y1, xy, x2, y2 } => {
                if m0 * x2 <= x1 * x1 || m0 * y2 <= y1 * y1 {
                    return Some(0.0);
                }
                Some((m0 * xy - x1 * y1) / ((m0 * x2 - x1 * x1) * (m0 * y2 - y1 * y1)).sqrt())
            }
            Moments::CorrWelford { c2, m2x, m2y, .. } => Some(if *m2x == 0.0 || *m2y == 0.0 { 0.0 } else { c2 / (m2x * m2y).sqrt() }),
            Moments::Covar {
                sum_x,
                sum_y,
                sum_xy,
                count,
                samp,
            } => {
                let c = *count as f64;
                Some(match (*samp, *count) {
                    (false, 1) | (true, 0 | 1) => 0.0,
                    (false, _) => sum_xy / c - (sum_x * sum_y) / (c * c),
                    (true, _) => sum_xy / (c - 1.0) - (sum_x * sum_y) / (c * (c - 1.0)),
                })
            }
            Moments::AvgWeighted { data_sum, weight_sum } => Some(if *weight_sum == 0.0 { f64::NAN } else { data_sum / weight_sum }),
        }
    }

    pub fn encode(&self, out: &mut Vec<u8>) {
        let f = |v: &[f64], out: &mut Vec<u8>| v.iter().for_each(|x| out.extend(x.to_bits().to_le_bytes()));
        match self {
            Moments::Power { m, order } => {
                out.push(*order);
                f(m, out);
            }
            Moments::Regr {
                sx,
                sy,
                sxx,
                syy,
                sxy,
                n,
                intercept,
            } => {
                out.push(if *intercept { 6 } else { 5 });
                f(&[*sx, *sy, *sxx, *syy, *sxy], out);
                out.extend(n.to_le_bytes());
            }
            Moments::Corr { m0, x1, y1, xy, x2, y2 } => {
                out.push(7);
                f(&[*m0, *x1, *y1, *xy, *x2, *y2], out);
            }
            Moments::CorrWelford {
                mean_x,
                mean_y,
                c2,
                m2x,
                m2y,
                count,
            } => {
                out.push(8);
                f(&[*mean_x, *mean_y, *c2, *m2x, *m2y], out);
                out.extend(count.to_le_bytes());
            }
            Moments::Covar {
                sum_x,
                sum_y,
                sum_xy,
                count,
                samp,
            } => {
                out.push(if *samp { 10 } else { 9 });
                f(&[*sum_x, *sum_y, *sum_xy], out);
                out.extend(count.to_le_bytes());
            }
            Moments::AvgWeighted { data_sum, weight_sum } => {
                out.push(11);
                f(&[*data_sum, *weight_sum], out);
            }
        }
    }

    /// The state `encode` wrote at the start of `b`, and the bytes it took.
    pub fn decode(b: &[u8]) -> Option<(Moments, usize)> {
        let f = |i: usize| -> Option<f64> { Some(f64::from_bits(u64::from_le_bytes(b.get(1 + 8 * i..9 + 8 * i)?.try_into().ok()?))) };
        let u = |i: usize| -> Option<u64> { Some(u64::from_le_bytes(b.get(1 + 8 * i..9 + 8 * i)?.try_into().ok()?)) };
        Some(match *b.first()? {
            order @ (3 | 4) => (
                Moments::Power {
                    m: [f(0)?, f(1)?, f(2)?, f(3)?, f(4)?],
                    order,
                },
                41,
            ),
            t @ (5 | 6) => (
                Moments::Regr {
                    sx: f(0)?,
                    sy: f(1)?,
                    sxx: f(2)?,
                    syy: f(3)?,
                    sxy: f(4)?,
                    n: u(5)?,
                    intercept: t == 6,
                },
                49,
            ),
            7 => (
                Moments::Corr {
                    m0: f(0)?,
                    x1: f(1)?,
                    y1: f(2)?,
                    xy: f(3)?,
                    x2: f(4)?,
                    y2: f(5)?,
                },
                49,
            ),
            8 => (
                Moments::CorrWelford {
                    mean_x: f(0)?,
                    mean_y: f(1)?,
                    c2: f(2)?,
                    m2x: f(3)?,
                    m2y: f(4)?,
                    count: u(5)? as i64,
                },
                49,
            ),
            t @ (9 | 10) => (
                Moments::Covar {
                    sum_x: f(0)?,
                    sum_y: f(1)?,
                    sum_xy: f(2)?,
                    count: u(3)? as i64,
                    samp: t == 10,
                },
                33,
            ),
            11 => (
                Moments::AvgWeighted {
                    data_sum: f(0)?,
                    weight_sum: f(1)?,
                },
                17,
            ),
            _ => return None,
        })
    }
}
