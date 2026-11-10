//! `st_point` and `st_astext`, as the C++ BE computes them (`exprs/function/geo/functions_geo.cpp`,
//! `geo_types.cpp`, s2geometry v0.10.0). A point is stored as `0x00`, the shape type `0x01`, then
//! its S2 unit vector as three little-endian f64.
use std::f64::consts::PI;

use doris_segment::writer::Datum;
use doris_thrift::gen::types::TPrimitiveType as T;

use crate::value::ColType;

const POINT: u8 = 1;

/// Whether `name` over `args` into `to` is a geo function this module evaluates.
pub fn supported(name: &str, args: &[ColType], to: &ColType) -> bool {
    let text = |t: &ColType| matches!(t.ptype, T::CHAR | T::VARCHAR | T::STRING);
    match name {
        "st_point" => args.len() == 2 && args.iter().all(|a| a.ptype == T::DOUBLE) && text(to),
        "st_astext" | "st_aswkt" => args.len() == 1 && text(&args[0]) && text(to),
        _ => false,
    }
}

pub fn is_geo_function(name: &str) -> bool {
    matches!(name, "st_point" | "st_astext" | "st_aswkt")
}

/// `GeoPoint::from_coord` and `encode_to`: NULL unless `|x| <= 180` and `|y| <= 90`
/// (x the longitude, y the latitude).
pub fn point(x: f64, y: f64) -> Option<Vec<u8>> {
    if !(x.abs() <= 180.0 && y.abs() <= 90.0) {
        return None;
    }
    // S2LatLng::FromDegrees(lat, lng).ToPoint(), in its operation order
    let (phi, theta) = ((PI / 180.0) * y, (PI / 180.0) * x);
    let c = phi.cos();
    let v = [theta.cos() * c, theta.sin() * c, phi.sin()];
    let mut out = vec![0, POINT];
    for f in v {
        out.extend_from_slice(&f.to_le_bytes());
    }
    Some(out)
}

/// `GeoPoint::as_wkt`: `POINT (lng lat)`, each at `std::setprecision(15)`; NULL unless the
/// bytes hold a point.
pub fn as_text(b: &[u8]) -> Option<String> {
    if b.len() != 26 || b[0] != 0 || b[1] != POINT {
        return None;
    }
    let f = |i: usize| f64::from_le_bytes(b[2 + 8 * i..10 + 8 * i].try_into().expect("8 bytes"));
    let (x, y, z) = (f(0), f(1), f(2));
    // S2LatLng(point): lat = atan2(z, sqrt(x² + y²)), lng = atan2(y, x), in degrees
    let lat = (180.0 / PI) * z.atan2((x * x + y * y).sqrt());
    let lng = (180.0 / PI) * y.atan2(x);
    Some(format!("POINT ({} {})", g15(lng), g15(lat)))
}

/// printf's `%.15g`, as `std::setprecision(15)` prints a double: 15 significant digits with
/// trailing zeros dropped, in scientific form when the exponent is below -4 or at least 15.
pub fn g15(v: f64) -> String {
    if v == 0.0 {
        return if v.is_sign_negative() { "-0" } else { "0" }.into();
    }
    let strip = |t: &str| {
        if t.contains('.') {
            t.trim_end_matches('0').trim_end_matches('.').to_string()
        } else {
            t.to_string()
        }
    };
    let sci = format!("{v:.14e}");
    let (mantissa, exp) = sci.split_once('e').expect("an exponent");
    let x: i32 = exp.parse().expect("an integer exponent");
    if !(-4..15).contains(&x) {
        format!("{}e{}{:02}", strip(mantissa), if x < 0 { '-' } else { '+' }, x.abs())
    } else {
        strip(&format!("{:.*}", (14 - x) as usize, v))
    }
}

/// Evaluates geo function `name`; a NULL argument gives NULL.
pub fn eval(name: &str, args: &[&[Datum]], rows: usize) -> Vec<Datum> {
    (0..rows)
        .map(|r| match (name, args.first().map(|a| &a[r]), args.get(1).map(|a| &a[r])) {
            ("st_point", Some(Datum::Double(x)), Some(Datum::Double(y))) => point(*x, *y).map_or(Datum::Null, Datum::Bytes),
            (_, Some(Datum::Bytes(b)), None) => as_text(b).map_or(Datum::Null, |s| Datum::Bytes(s.into_bytes())),
            _ => Datum::Null,
        })
        .collect()
}
