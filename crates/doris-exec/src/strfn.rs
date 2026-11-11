//! String functions, as the C++ BE computes them (`be/src/exprs/function/function_string.cpp`,
//! `function_string_concat.h`, `function_string_misc.cpp`, `function_hex.cpp`,
//! `function_string_replace.h`, `nullif.cpp`): see
//! `docs/superpowers/research/2026-11-10-m6-s7b3-strings.md`.
use doris_segment::writer::Datum;
use doris_thrift::gen::status::TStatusCode;
use doris_thrift::gen::types::TPrimitiveType as T;

use crate::value::ColType;
use crate::ExecError;

const NAMES: &[&str] = &[
    "upper",
    "ucase",
    "replace",
    "nullif",
    "find_in_set",
    "char",
    "lpad",
    "rpad",
    "hex",
    "unhex",
    "crc32",
    "parse_data_size",
    "trim_in",
    "ltrim_in",
    "rtrim_in",
    "url_encode",
    "url_decode",
    "top_level_domain",
    "first_significant_subdomain",
    "cut_to_first_significant_subdomain",
];

/// Whether `name` is a function this module evaluates (for some signature).
pub fn is_string_function(name: &str) -> bool {
    NAMES.contains(&name)
}

fn is_text(t: &ColType) -> bool {
    matches!(t.ptype, T::CHAR | T::VARCHAR | T::STRING)
}

/// Whether `name` over `args` into `to` is a call this module evaluates.
pub fn supported(name: &str, args: &[ColType], to: &ColType) -> bool {
    let all_text = |n: usize| args.len() == n && args.iter().all(is_text);
    match name {
        "upper"
        | "ucase"
        | "hex"
        | "unhex"
        | "url_encode"
        | "url_decode"
        | "top_level_domain"
        | "first_significant_subdomain"
        | "cut_to_first_significant_subdomain" => all_text(1) && is_text(to),
        "replace" => all_text(3) && is_text(to),
        "find_in_set" => all_text(2) && to.ptype == T::INT,
        "crc32" => all_text(1) && to.ptype == T::BIGINT,
        "parse_data_size" => all_text(1) && to.ptype == T::LARGEINT,
        "lpad" | "rpad" => args.len() == 3 && is_text(&args[0]) && args[1].ptype == T::INT && is_text(&args[2]) && is_text(to),
        "char" => args.len() >= 2 && is_text(&args[0]) && args[1..].iter().all(|a| a.ptype == T::INT) && is_text(to),
        "trim_in" | "ltrim_in" | "rtrim_in" => (all_text(1) || all_text(2)) && is_text(to),
        // `if(a = b, NULL, a)`: two values of one type, compared as stored
        "nullif" => {
            args.len() == 2
                && args.iter().all(|a| a.ptype == to.ptype && a.scale == to.scale)
                && matches!(
                    to.ptype,
                    T::BOOLEAN
                        | T::TINYINT
                        | T::SMALLINT
                        | T::INT
                        | T::BIGINT
                        | T::LARGEINT
                        | T::DOUBLE
                        | T::DECIMAL32
                        | T::DECIMAL64
                        | T::DECIMAL128I
                        | T::DATEV2
                        | T::DATETIMEV2
                        | T::CHAR
                        | T::VARCHAR
                        | T::STRING
                )
        }
        _ => false,
    }
}

fn bytes(d: &Datum) -> &[u8] {
    match d {
        Datum::Bytes(b) => b,
        _ => &[],
    }
}

fn int(d: &Datum) -> i128 {
    match d {
        Datum::Int(v) => *v,
        _ => 0,
    }
}

/// Every row with a NULL argument is NULL; the others go through `f`.
fn rows(args: &[&[Datum]], n: usize, mut f: impl FnMut(&[&Datum]) -> Result<Datum, ExecError>) -> Result<Vec<Datum>, ExecError> {
    (0..n)
        .map(|r| {
            let row: Vec<&Datum> = args.iter().map(|a| &a[r]).collect();
            if row.iter().any(|d| matches!(d, Datum::Null)) {
                Ok(Datum::Null)
            } else {
                f(&row)
            }
        })
        .collect()
}

/// A UTF-8 character's length by its lead byte (`UTF8_BYTE_LENGTH`, vstring_function.h:35-44).
fn utf8_len(b: u8) -> usize {
    match b {
        0xC0..=0xDF => 2,
        0xE0..=0xEF => 3,
        0xF0..=0xF7 => 4,
        0xF8..=0xFB => 5,
        0xFC..=0xFF => 6,
        _ => 1,
    }
}

/// The byte offsets where each character starts, by lead byte, and the end.
fn char_starts(s: &[u8]) -> Vec<usize> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < s.len() {
        out.push(i);
        i += utf8_len(s[i]);
    }
    out.push(s.len());
    out
}

/// The characters of `s`, by lead byte (the last may be cut short).
fn chars(s: &[u8]) -> Vec<&[u8]> {
    let starts = char_starts(s);
    starts.windows(2).map(|w| &s[w[0]..w[1].min(s.len())]).collect()
}

/// `upper` (fs.cpp:567-627): ASCII when the bytes are, else full Unicode case mapping.
fn upper(s: &[u8]) -> Vec<u8> {
    if s.is_ascii() {
        s.to_ascii_uppercase()
    } else {
        String::from_utf8_lossy(s).to_uppercase().into_bytes()
    }
}

/// `replace` (function_string_replace.h:57-159): an empty `old` leaves the string as it is.
fn replace(s: &[u8], old: &[u8], new: &[u8]) -> Vec<u8> {
    if old.is_empty() {
        return s.to_vec();
    }
    let mut out = Vec::with_capacity(s.len());
    let mut i = 0;
    while i < s.len() {
        if s[i..].starts_with(old) {
            out.extend_from_slice(new);
            i += old.len();
        } else {
            out.push(s[i]);
            i += 1;
        }
    }
    out
}

/// `find_in_set` (fs.cpp:307-343): 0 when the needle holds a comma.
fn find_in_set(needle: &[u8], list: &[u8]) -> i128 {
    if needle.contains(&b',') {
        return 0;
    }
    // a do-while that stops when the next token would start at the end: an empty last token
    // (after a trailing comma) is never tested
    let mut start = 0;
    let mut index = 1;
    loop {
        let end = list[start..].iter().position(|b| *b == b',').map_or(list.len(), |p| start + p);
        if &list[start..end] == needle {
            return index;
        }
        start = end + 1;
        index += 1;
        if start >= list.len() {
            return 0;
        }
    }
}

/// `char(charset, n…)` (misc.cpp:602-783): each number's big-endian bytes without leading zeros;
/// NULL numbers are skipped; invalid UTF-8 is NULL. The charset is not read.
fn char_of(nums: &[&Datum]) -> Datum {
    let mut out = Vec::new();
    for d in nums {
        let Datum::Int(v) = d else { continue };
        let b = (*v as u32).to_be_bytes();
        let first = b.iter().position(|x| *x != 0).unwrap_or(3);
        out.extend_from_slice(&b[first..]);
    }
    if std::str::from_utf8(&out).is_ok() {
        Datum::Bytes(out)
    } else {
        Datum::Null
    }
}

/// `lpad`/`rpad` (function_string_concat.h:757-826), in characters.
/// The C++ string column's limit (`ColumnString::check_chars_length`, column_string.h:68-77).
const MAX_STRING_SIZE: usize = 0xffff_ffff;

/// `lpad`/`rpad` of row `row`, whose column already holds `written` bytes.
fn pad(s: &[u8], len: i128, p: &[u8], left: bool, row: usize, written: usize) -> Result<Datum, ExecError> {
    if len < 0 {
        return Ok(Datum::Null);
    }
    // the reservation C++ checks before padding (function_string_concat.h:788-798)
    let count = char_starts(s).len() - 1;
    let pad_chars = chars(p).len();
    if count < len as usize && pad_chars > 0 {
        let pad_times = (len as usize - count) / pad_chars;
        let total = written + s.len() + (pad_times + 1) * p.len();
        if total > MAX_STRING_SIZE {
            return Err(ExecError::Coded {
                code: -3113,
                name: "E-3113",
                msg: format!("string column length is too large: total_length={total}, element_number={row}, rows=0"),
            });
        }
    }
    Ok(pad_row(s, len, p, left))
}

fn pad_row(s: &[u8], len: i128, p: &[u8], left: bool) -> Datum {
    let len = len as usize;
    let starts = char_starts(s);
    let count = starts.len() - 1;
    if count >= len {
        return Datum::Bytes(s[..starts[len].min(s.len())].to_vec());
    }
    if p.is_empty() {
        return Datum::Bytes(Vec::new());
    }
    let pad_chars = chars(p);
    let need = len - count;
    let mut fill = Vec::new();
    for _ in 0..need / pad_chars.len() {
        fill.extend_from_slice(p);
    }
    for c in &pad_chars[..need % pad_chars.len()] {
        fill.extend_from_slice(c);
    }
    Datum::Bytes(if left {
        [fill, s.to_vec()].concat()
    } else {
        [s.to_vec(), fill].concat()
    })
}

/// Uppercase hex of every byte (function_hex.cpp:54-112).
fn hex(s: &[u8]) -> Vec<u8> {
    const D: &[u8; 16] = b"0123456789ABCDEF";
    s.iter().flat_map(|b| [D[(b >> 4) as usize], D[(b & 15) as usize]]).collect()
}

/// `unhex` (fs.cpp:1006-1060): odd length or a non-hex digit gives `''`.
fn unhex(s: &[u8]) -> Vec<u8> {
    if !s.len().is_multiple_of(2) {
        return Vec::new();
    }
    let digit = |c: u8| (c as char).to_digit(16).map(|d| d as u8);
    let mut out = Vec::with_capacity(s.len() / 2);
    for pair in s.chunks(2) {
        match (digit(pair[0]), digit(pair[1])) {
            (Some(h), Some(l)) => out.push(h << 4 | l),
            _ => return Vec::new(),
        }
    }
    out
}

fn invalid(msg: String) -> ExecError {
    ExecError::Coded {
        code: TStatusCode::INVALID_ARGUMENT.0,
        name: "INVALID_ARGUMENT",
        msg,
    }
}

/// `parse_data_size` (fs.cpp:138-168): the leading run of digits and dots, read as `stod` reads
/// it, times a case-sensitive unit.
fn parse_data_size(s: &[u8]) -> Result<i128, ExecError> {
    let shown = String::from_utf8_lossy(s);
    let base = || format!("Invalid Input argument \"{shown}\" of function parse_data_size");
    let run = s.iter().take_while(|c| c.is_ascii_digit() || **c == b'.').count();
    if run == 0 {
        return Err(invalid(base()));
    }
    // stod takes the longest prefix of digits, one dot, digits, with at least one digit
    let num = &s[..run];
    let int_digits = num.iter().take_while(|c| c.is_ascii_digit()).count();
    let mut end = int_digits;
    if num.get(end) == Some(&b'.') {
        end += 1 + num[end + 1..].iter().take_while(|c| c.is_ascii_digit()).count();
    }
    let has_digit = num[..end].iter().any(u8::is_ascii_digit);
    let value: f64 = match std::str::from_utf8(&num[..end]).ok().filter(|_| has_digit).and_then(|t| t.parse().ok()) {
        Some(v) if f64::is_finite(v) => v,
        _ => return Err(invalid(format!("{}, error: stod", base()))),
    };
    let shift = match &s[run..] {
        b"B" => 0,
        b"kB" => 10,
        b"MB" => 20,
        b"GB" => 30,
        b"TB" => 40,
        b"PB" => 50,
        b"EB" => 60,
        b"ZB" => 70,
        b"YB" => 80,
        _ => return Err(invalid(base())),
    };
    // a power of two times a double is exact; the cast truncates
    Ok((value * 2f64.powi(shift)) as i128)
}

/// `trim_in` and its sides (fs.cpp:724-1004): one argument strips spaces; otherwise the second is
/// a set of characters stripped from the end(s).
fn trim_in(s: &[u8], set: Option<&[u8]>, left: bool, right: bool) -> Vec<u8> {
    let set: Vec<&[u8]> = match set {
        None => vec![b" "],
        Some(set) => chars(set),
    };
    let cs = chars(s);
    let mut a = 0;
    let mut b = cs.len();
    if left {
        while a < b && set.contains(&cs[a]) {
            a += 1;
        }
    }
    if right {
        while b > a && set.contains(&cs[b - 1]) {
            b -= 1;
        }
    }
    cs[a..b].concat()
}

/// Evaluates string function `name`; `types` are the arguments' types.
pub fn eval(name: &str, types: &[ColType], args: &[&[Datum]], n: usize) -> Result<Vec<Datum>, ExecError> {
    let text = |v: Vec<u8>| Ok(Datum::Bytes(v));
    match name {
        "upper" | "ucase" => rows(args, n, |r| text(upper(bytes(r[0])))),
        "replace" => rows(args, n, |r| text(replace(bytes(r[0]), bytes(r[1]), bytes(r[2])))),
        "find_in_set" => rows(args, n, |r| Ok(Datum::Int(find_in_set(bytes(r[0]), bytes(r[1]))))),
        "crc32" => rows(args, n, |r| Ok(Datum::Int(i128::from(crc32fast::hash(bytes(r[0])))))),
        "hex" => rows(args, n, |r| text(hex(bytes(r[0])))),
        "unhex" => rows(args, n, |r| text(unhex(bytes(r[0])))),
        "url_encode" => rows(args, n, |r| text(crate::url::url_encode(bytes(r[0])))),
        "url_decode" => rows(args, n, |r| crate::url::url_decode(bytes(r[0])).map(Datum::Bytes)),
        "top_level_domain" => rows(args, n, |r| text(crate::url::top_level_domain(bytes(r[0])))),
        "first_significant_subdomain" => rows(args, n, |r| text(crate::url::first_significant_subdomain(bytes(r[0])))),
        "cut_to_first_significant_subdomain" => rows(args, n, |r| text(crate::url::cut_to_first_significant_subdomain(bytes(r[0])))),
        "lpad" | "rpad" => {
            let mut written = 0;
            let mut row = 0;
            rows(args, n, |r| {
                let out = pad(bytes(r[0]), int(r[1]), bytes(r[2]), name == "lpad", row, written)?;
                written += bytes(&out).len();
                row += 1;
                Ok(out)
            })
        }
        "trim_in" | "ltrim_in" | "rtrim_in" => rows(args, n, |r| {
            text(trim_in(bytes(r[0]), r.get(1).map(|d| bytes(d)), name != "rtrim_in", name != "ltrim_in"))
        }),
        // these three handle their own NULLs
        "parse_data_size" => (0..n)
            .map(|r| match &args[0][r] {
                Datum::Null => Ok(Datum::Null),
                d => parse_data_size(bytes(d)).map(Datum::Int),
            })
            .collect(),
        "char" => Ok((0..n).map(|r| char_of(&args[1..].iter().map(|a| &a[r]).collect::<Vec<_>>())).collect()),
        "nullif" => Ok((0..n)
            .map(|r| {
                let (a, b) = (&args[0][r], &args[1][r]);
                // `a = b` (the `eq` function: NaN equals NaN) is NULL when either is, and NULL
                // is not true
                let equal = match (types.first(), types.get(1)) {
                    (Some(ta), Some(tb)) => crate::eval::compare(a, ta, b, tb) == Some(std::cmp::Ordering::Equal),
                    _ => false,
                };
                if *a != Datum::Null && *b != Datum::Null && equal {
                    Datum::Null
                } else {
                    a.clone()
                }
            })
            .collect()),
        other => Err(ExecError::Unsupported {
            what: format!("function {other}"),
        }),
    }
}
