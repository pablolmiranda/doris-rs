//! `like`, `concat` and `length` as the C++ BE computes them (`be/src/exprs/function/`).
//!
//! `like` follows `like.cpp`/`like.h`, including where its paths disagree:
//! - a constant pattern is classified by five regexes first (`construct_like_const_state`,
//!   `like.cpp:896-1005`, regexes `:57-61`): all-pass, equals, starts-with, ends-with,
//!   substring, else a regex (`convert_like_pattern`, `:789-843`). The equals and ends-with
//!   regexes accept a raw `\`, so `a\\_` is the literal `a\_` there, not `a\` and a wildcard;
//! - a column of patterns takes one of those kinds when every row has it
//!   (`pattern_type_recognition`, `:633-699`), else each row goes through
//!   `extract_like_fast_path` (`like.h:63-148`), which counts escapes, or the regex.
use regex::bytes::{Regex, RegexBuilder};
use std::sync::OnceLock;

use doris_segment::writer::Datum;

use crate::value::ColType;
use crate::ExecError;

#[derive(Debug, Clone)]
enum Kind {
    AllPass,
    Equals(Vec<u8>),
    StartsWith(Vec<u8>),
    EndsWith(Vec<u8>),
    Substring(Vec<u8>),
    Regex(Regex),
}

/// A compiled LIKE pattern.
#[derive(Debug, Clone)]
pub struct Like(Kind);

struct Classifiers {
    allpass: Regex,
    equals: Regex,
    starts: Regex,
    ends: Regex,
    substring: Regex,
}

fn classifiers() -> &'static Classifiers {
    static C: OnceLock<Classifiers> = OnceLock::new();
    C.get_or_init(|| {
        // like.cpp:57-61, as RE2::FullMatch reads them
        let full = |re: &str| Regex::new(&format!("^(?:{re})$")).expect("like classifier");
        Classifiers {
            allpass: full("%+"),
            equals: full(r"(((\\_)|([^%_]))+)"),
            starts: full(r"(((\\%)|(\\_)|([^%_\\]))+)(?:%+)"),
            ends: full(r"(?:%+)(((\\_)|([^%_]))+)"),
            substring: full(r"(?:%+)(((\\_)|([^%_\\]))+)(?:%+)"),
        }
    })
}

/// `remove_escape_character` (`like.cpp:845-861`): `\%`, `\_`, `\\` become their second byte.
fn remove_escapes(s: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(s.len());
    let mut i = 0;
    while i < s.len() {
        if s[i] == b'\\' && i + 1 < s.len() && matches!(s[i + 1], b'%' | b'_' | b'\\') {
            out.push(s[i + 1]);
            i += 2;
        } else {
            out.push(s[i]);
            i += 1;
        }
    }
    out
}

/// The kind the constant classifiers give a pattern; None for the regex path.
fn classify(p: &[u8]) -> Option<Kind> {
    let c = classifiers();
    let group = |re: &Regex| re.captures(p).and_then(|m| m.get(1)).map(|g| remove_escapes(g.as_bytes()));
    if !p.is_empty() && c.allpass.is_match(p) {
        Some(Kind::AllPass)
    } else if p.is_empty() {
        Some(Kind::Equals(Vec::new()))
    } else if let Some(s) = group(&c.equals) {
        Some(Kind::Equals(s))
    } else if let Some(s) = group(&c.starts) {
        Some(Kind::StartsWith(s))
    } else if let Some(s) = group(&c.ends) {
        Some(Kind::EndsWith(s))
    } else {
        group(&c.substring).map(Kind::Substring)
    }
}

/// `convert_like_pattern` (`like.cpp:789-843`): the regex a LIKE pattern searches with.
pub fn like_regex(pattern: &[u8]) -> String {
    if pattern.is_empty() {
        return r"^\z".into();
    }
    let mut re = Vec::with_capacity(pattern.len() * 2);
    if pattern[0] != b'%' {
        re.push(b'^');
    }
    let mut i = 0;
    while i < pattern.len() {
        let c = pattern[i];
        if c == b'\\' && i + 1 < pattern.len() {
            let next = pattern[i + 1];
            if next == b'%' || next == b'_' {
                re.push(next);
                i += 2;
                continue;
            } else if next == b'\\' {
                re.extend_from_slice(br"\\");
                i += 2;
                continue;
            }
        }
        match c {
            b'%' if i + 1 == pattern.len() => return String::from_utf8_lossy(&re).into_owned(),
            b'%' => re.extend_from_slice(b".*"),
            b'_' => re.push(b'.'),
            b'[' | b']' | b'(' | b')' | b'{' | b'}' | b'-' | b'*' | b'+' | b'\\' | b'|' | b'/' | b':' | b'^' | b'.' | b'$' | b'?' => {
                re.push(b'\\');
                re.push(c);
            }
            _ => re.push(c),
        }
        i += 1;
    }
    re.extend_from_slice(br"\z");
    String::from_utf8_lossy(&re).into_owned()
}

fn compile(pattern: &[u8]) -> Result<Regex, ExecError> {
    if std::str::from_utf8(pattern).is_err() {
        return Err(ExecError::Unsupported {
            what: "a like pattern that is not UTF-8".into(),
        });
    }
    let re = like_regex(pattern);
    // RE2 with dot_nl, searched unanchored (like.cpp:512-516, :992-995)
    RegexBuilder::new(&re)
        .dot_matches_new_line(true)
        .unicode(true)
        .build()
        .map_err(|_| ExecError::Internal(format!("Invalid regex expression: {re}(origin: {})", String::from_utf8_lossy(pattern))))
}

/// `replace_pattern_by_escape` (`like.h:150-171`): a custom ESCAPE rewritten to backslashes.
fn with_escape(pattern: &[u8], escape: Option<u8>) -> Vec<u8> {
    let Some(e) = escape else { return pattern.to_vec() };
    let mut out = Vec::with_capacity(pattern.len() * 2);
    let mut i = 0;
    while i < pattern.len() {
        let c = pattern[i];
        if i + 1 < pattern.len() && c == e && (pattern[i + 1] == e || pattern[i + 1] == b'%' || pattern[i + 1] == b'_') {
            if pattern[i + 1] == b'%' || pattern[i + 1] == b'_' {
                out.push(b'\\');
            }
            out.push(pattern[i + 1]);
            i += 2;
            continue;
        }
        if c == b'\\' {
            out.extend_from_slice(br"\\");
        } else {
            out.push(c);
        }
        i += 1;
    }
    out
}

/// The ESCAPE argument's byte (`FunctionLike::open`, `like.cpp:1018-1027`).
pub fn escape_char(e: &[u8]) -> Result<u8, ExecError> {
    match e {
        [c] => Ok(*c),
        _ => Err(ExecError::Internal(format!(
            "Escape character must be a single character, got: {}",
            String::from_utf8_lossy(e)
        ))),
    }
}

/// `extract_like_fast_path` (`like.h:63-148`): the per-row classification, escapes counted.
fn per_row_kind(p: &[u8]) -> Option<Kind> {
    let len = p.len();
    if len == 0 {
        return Some(Kind::Equals(Vec::new()));
    }
    let unescaped = |mut pos: usize| {
        let mut n = 0;
        while pos > 0 && p[pos - 1] == b'\\' {
            n += 1;
            pos -= 1;
        }
        n % 2 == 0
    };
    let starts_pct = p[0] == b'%';
    let ends_pct = p[len - 1] == b'%' && unescaped(len - 1);
    if p[0] == b'_' || (p[len - 1] == b'_' && unescaped(len - 1)) {
        return None;
    }
    let mut i = 0;
    while i < len && p[i] == b'%' {
        i += 1;
    }
    if i >= len {
        return Some(Kind::AllPass);
    }
    let mut s = Vec::with_capacity(len);
    while i < len {
        let c = p[i];
        if c == b'\\' && i + 1 < len && matches!(p[i + 1], b'%' | b'_' | b'\\') {
            s.push(p[i + 1]);
            i += 2;
            continue;
        }
        if c == b'_' {
            return None;
        }
        if c == b'%' {
            if p[i..].iter().all(|b| *b == b'%') {
                break;
            }
            return None;
        }
        s.push(c);
        i += 1;
    }
    Some(match (starts_pct, ends_pct) {
        (true, true) => Kind::Substring(s),
        (true, false) => Kind::EndsWith(s),
        (false, true) => Kind::StartsWith(s),
        (false, false) => Kind::Equals(s),
    })
}

impl Like {
    /// A constant pattern, as `construct_like_const_state` builds it.
    pub fn constant(pattern: &[u8], escape: Option<u8>) -> Result<Like, ExecError> {
        let p = with_escape(pattern, escape);
        Ok(Like(match classify(&p) {
            Some(k) => k,
            // Hyperscan compiles a constant pattern's c_str(): a NUL would cut it short there
            None if p.contains(&0) => {
                return Err(ExecError::Unsupported {
                    what: "a like pattern holding a NUL byte".into(),
                })
            }
            None => Kind::Regex(compile(&p)?),
        }))
    }

    /// One row's pattern from a column of patterns, as `like_fn_scalar` matches it.
    pub fn per_row(pattern: &[u8], escape: Option<u8>) -> Result<Like, ExecError> {
        let p = with_escape(pattern, escape);
        Ok(Like(match per_row_kind(&p) {
            Some(k) => k,
            None => Kind::Regex(compile(&p)?),
        }))
    }

    pub fn matches(&self, v: &[u8]) -> bool {
        match &self.0 {
            Kind::AllPass => true,
            Kind::Equals(s) => v == s.as_slice(),
            Kind::StartsWith(s) => v.starts_with(s),
            Kind::EndsWith(s) => v.ends_with(s),
            Kind::Substring(s) => s.is_empty() || v.windows(s.len()).any(|w| w == s.as_slice()),
            Kind::Regex(r) => r.is_match(v),
        }
    }
}

fn bytes(d: &Datum) -> Option<&[u8]> {
    match d {
        Datum::Bytes(b) => Some(b),
        _ => None,
    }
}

/// `like` over a column: `constant` when the pattern is a constant expression. NULL in, NULL out.
pub fn like_column(values: &[Datum], patterns: &[Datum], constant: bool, escape: Option<u8>) -> Result<Vec<Datum>, ExecError> {
    let result = |v: &Datum, p: &Datum, l: &Like| match (bytes(v), p) {
        (Some(v), Datum::Bytes(_)) => Datum::Bool(l.matches(v)),
        _ => Datum::Null,
    };
    if constant {
        let Some(p) = patterns.iter().find_map(bytes) else {
            return Ok(vec![Datum::Null; values.len()]);
        };
        let l = Like::constant(p, escape)?;
        return Ok(values.iter().zip(patterns).map(|(v, p)| result(v, p, &l)).collect());
    }
    // a NULL row's pattern is the empty string to the recognition (ColumnString's default)
    let rewritten: Vec<Vec<u8>> = patterns.iter().map(|p| with_escape(bytes(p).unwrap_or_default(), escape)).collect();
    let kinds: Option<Vec<Kind>> = rewritten.iter().map(|p| classify(p)).collect();
    let same = kinds
        .as_ref()
        .is_some_and(|k| k.windows(2).all(|w| std::mem::discriminant(&w[0]) == std::mem::discriminant(&w[1])));
    if let (true, Some(kinds)) = (same, kinds) {
        return Ok(values.iter().zip(patterns).zip(kinds).map(|((v, p), k)| result(v, p, &Like(k))).collect());
    }
    values
        .iter()
        .zip(patterns)
        .zip(&rewritten)
        .map(|((v, p), rp)| match (bytes(v), p) {
            (Some(v), Datum::Bytes(_)) => {
                let l = match per_row_kind(rp) {
                    Some(k) => Like(k),
                    None => Like(Kind::Regex(compile(rp)?)),
                };
                Ok(Datum::Bool(l.matches(v)))
            }
            _ => Ok(Datum::Null),
        })
        .collect()
}

/// `concat(str...)`: the arguments joined; NULL when any is NULL. The FE casts each to text.
pub fn concat_column(args: &[(&ColType, &[Datum])]) -> Vec<Datum> {
    let rows = args.first().map_or(0, |(_, v)| v.len());
    (0..rows)
        .map(|r| {
            let mut out = Vec::new();
            for (_, v) in args {
                match &v[r] {
                    Datum::Bytes(b) => out.extend_from_slice(b),
                    _ => return Datum::Null,
                }
            }
            Datum::Bytes(out)
        })
        .collect()
}

/// `length(str)`: bytes (`function_string.cpp:191-210`).
/// `concat_ws(sep, args...)`: NULL when `sep` is; NULL arguments are skipped.
pub fn concat_ws_column(args: &[&[Datum]]) -> Vec<Datum> {
    let Some((sep, rest)) = args.split_first() else { return vec![] };
    (0..sep.len())
        .map(|r| {
            let Datum::Bytes(s) = &sep[r] else { return Datum::Null };
            let parts: Vec<&[u8]> = rest.iter().filter_map(|a| if let Datum::Bytes(b) = &a[r] { Some(b.as_slice()) } else { None }).collect();
            Datum::Bytes(parts.join(s.as_slice()))
        })
        .collect()
}

pub fn length_column(a: &[Datum]) -> Vec<Datum> {
    a.iter()
        .map(|d| match d {
            Datum::Bytes(b) => Datum::Int(b.len() as i128),
            _ => Datum::Null,
        })
        .collect()
}
