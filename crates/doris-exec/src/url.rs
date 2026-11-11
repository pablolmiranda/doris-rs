//! URL functions, as the C++ BE computes them: `url_encode`/`url_decode`
//! (`be/src/util/url_coding.cpp`) and the ClickHouse domain functions
//! (`be/src/exprs/function/url/domain.h`). See
//! `docs/superpowers/research/2026-11-10-m6-s7b3-strings.md`.
//!
//! `tld.txt` is the `wordlist[]` of `url/tldLookup.generated.cpp` (its 5045 non-empty strings,
//! octal escapes decoded, sorted, one per line); regenerate it with `scripts/gen-tld.py`.
use std::collections::HashSet;
use std::sync::OnceLock;

use doris_thrift::gen::status::TStatusCode;

use crate::ExecError;

/// The two-level public suffixes `tldLookup::is_valid` matches, byte for byte.
fn tld_valid(host: &[u8]) -> bool {
    static SET: OnceLock<HashSet<&'static [u8]>> = OnceLock::new();
    SET.get_or_init(|| include_str!("tld.txt").lines().map(str::as_bytes).collect())
        .contains(host)
}

/// `url_encode`: `[A-Za-z0-9.-*_]` kept, a space `+`, any other byte `%XX` in uppercase.
pub fn url_encode(s: &[u8]) -> Vec<u8> {
    const D: &[u8; 16] = b"0123456789ABCDEF";
    let mut out = Vec::with_capacity(s.len());
    for &c in s {
        match c {
            b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'.' | b'-' | b'*' | b'_' => out.push(c),
            b' ' => out.push(b'+'),
            _ => out.extend_from_slice(&[b'%', D[(c >> 4) as usize], D[(c & 15) as usize]]),
        }
    }
    out
}

/// Two bytes as `istringstream >> std::hex >> int` reads them: leading whitespace skipped, a sign,
/// then hex digits up to the first other byte; none is a failure.
fn read_hex(two: &[u8]) -> Option<i32> {
    let mut i = 0;
    while i < two.len() && matches!(two[i], b' ' | b'\t' | b'\n' | b'\x0b' | b'\x0c' | b'\r') {
        i += 1;
    }
    let negative = match two.get(i) {
        Some(b'-') => {
            i += 1;
            true
        }
        Some(b'+') => {
            i += 1;
            false
        }
        _ => false,
    };
    // num_get takes a `0x` prefix under std::hex; two bytes leave no digit after it
    if two[i..].starts_with(b"0x") || two[i..].starts_with(b"0X") {
        return None;
    }
    let digits: Vec<u32> = two[i..].iter().map_while(|c| (*c as char).to_digit(16)).collect();
    if digits.is_empty() {
        return None;
    }
    let v = digits.iter().fold(0i32, |a, d| a * 16 + *d as i32);
    Some(if negative { -v } else { v })
}

/// `url_decode`: `+` a space, `%XX` a byte (two bytes always consumed), else
/// `[INTERNAL_ERROR]Decode url failed`.
pub fn url_decode(s: &[u8]) -> Result<Vec<u8>, ExecError> {
    let failed = || ExecError::Coded {
        code: TStatusCode::INTERNAL_ERROR.0,
        name: "INTERNAL_ERROR",
        msg: "Decode url failed".into(),
    };
    let mut out = Vec::with_capacity(s.len());
    let mut i = 0;
    while i < s.len() {
        match s[i] {
            b'%' => {
                if i + 3 > s.len() {
                    return Err(failed());
                }
                let v = read_hex(&s[i + 1..i + 3]).ok_or_else(failed)?;
                out.push(v as u8);
                i += 2;
            }
            b'+' => out.push(b' '),
            c => out.push(c),
        }
        i += 1;
    }
    Ok(out)
}

fn restricted(c: u8) -> bool {
    matches!(
        c,
        b' ' | b'\t' | b'<' | b'>' | b'%' | b'{' | b'}' | b'|' | b'\\' | b'^' | b'~' | b'[' | b']' | b';' | b'=' | b'&'
    )
}

/// `check_and_return_host` (domain.h:31-40): the host `start..pos` if it has a dot that is not last
/// and not followed by an end symbol.
fn checked_host(s: &[u8], pos: usize, dot: Option<usize>, start: usize) -> Option<(usize, usize)> {
    let dot = dot?;
    if start >= pos || pos - dot == 1 {
        return None;
    }
    if matches!(s.get(dot + 1), Some(b':' | b'/' | b'?' | b'#')) {
        return None;
    }
    Some((start, pos))
}

/// `get_url_host` (domain.h:45-129): the host's byte range, or None.
fn url_host(s: &[u8]) -> Option<(usize, usize)> {
    let at = |i: usize| s.get(i).copied().unwrap_or(0);
    let mut pos = 0;
    if at(0) == b'/' && at(1) == b'/' {
        pos = 2;
    } else {
        let scheme_end = s.len().min(16);
        pos += 1;
        while pos < scheme_end {
            let c = s[pos];
            if !c.is_ascii_alphanumeric() {
                if matches!(c, b'.' | b'-' | b'+') {
                } else if restricted(c) {
                    return None;
                } else {
                    break;
                }
            }
            pos += 1;
        }
        if scheme_end > pos + 2 && at(pos) == b':' && at(pos + 1) == b'/' && at(pos + 2) == b'/' {
            pos += 3;
        } else {
            pos = 0;
        }
    }
    let mut dot = None;
    let mut start = pos;
    while pos < s.len() {
        match s[pos] {
            b'.' => dot = Some(pos),
            b':' | b'/' | b'?' | b'#' => return checked_host(s, pos, dot, start),
            b'@' => start = pos + 1,
            c if restricted(c) => return None,
            _ => {}
        }
        pos += 1;
    }
    checked_host(s, pos, dot, start)
}

/// `top_level_domain` (domain.h:150-182): after the host's last dot; `''` for an IPv4 address.
pub fn top_level_domain(s: &[u8]) -> Vec<u8> {
    let Some((a, mut b)) = url_host(s) else { return Vec::new() };
    if s[b - 1] == b'.' {
        b -= 1;
    }
    let host = &s[a..b];
    match host.iter().rposition(|c| *c == b'.') {
        Some(d) if !host.get(d + 1).is_some_and(u8::is_ascii_digit) => host[d + 1..].to_vec(),
        _ => Vec::new(),
    }
}

/// `ExtractFirstSignificantSubdomain` (domain.h:184-249): the label's range and the end of the
/// domain (`www.` stripped, a trailing dot kept), or None.
fn first_significant(s: &[u8]) -> Option<((usize, usize), usize)> {
    let (mut a, b) = url_host(s)?;
    if b - a > 4 && s[a..].starts_with(b"www.") {
        a += 4;
    }
    let domain_end = b;
    let end = if s[b - 1] == b'.' { b - 1 } else { b };
    // the last three dots, newest first
    let mut periods: [Option<usize>; 3] = [None; 3];
    for (i, c) in s[a..end].iter().enumerate() {
        if *c == b'.' {
            periods = [Some(a + i), periods[0], periods[1]];
        }
    }
    let label = match periods {
        [None, ..] => (a, end),
        [Some(p0), None, _] => (a, p0),
        [Some(p0), Some(p1), p2] => {
            // `last_periods[2]` defaults to one before the start
            let before = |p: Option<usize>| p.map_or(a, |p| p + 1);
            let level_end = s[p0..end].iter().position(|c| *c == b'/').map_or(end, |i| p0 + i);
            if tld_valid(&s[p1 + 1..level_end]) {
                (before(p2), p1)
            } else {
                (p1 + 1, p0)
            }
        }
    };
    Some((label, domain_end))
}

pub fn first_significant_subdomain(s: &[u8]) -> Vec<u8> {
    first_significant(s).map_or_else(Vec::new, |((a, b), _)| s[a..b].to_vec())
}

/// `cut_to_first_significant_subdomain` (domain.h:251-269): from that label to the domain's end.
pub fn cut_to_first_significant_subdomain(s: &[u8]) -> Vec<u8> {
    match first_significant(s) {
        Some(((a, b), end)) if b > a => s[a..end].to_vec(),
        _ => Vec::new(),
    }
}
