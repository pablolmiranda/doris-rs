//! `like`, `concat`, `length` as the C++ BE computes them (be/src/exprs/function/like.cpp,
//! function_string*.cpp).
use doris_exec::strings::{concat_column, length_column, like_column, like_regex, Like};
use doris_exec::value::ColType;
use doris_segment::writer::Datum;
use doris_thrift::gen::types::TPrimitiveType as T;

fn s(v: &str) -> Datum {
    Datum::Bytes(v.as_bytes().to_vec())
}

fn constant(p: &str) -> Like {
    Like::constant(p.as_bytes(), None).expect("pattern")
}

#[test]
fn like_regex_translation() {
    assert_eq!(like_regex(b"a.c"), r"^a\.c\z");
    assert_eq!(like_regex(b"%[x]%"), r".*\[x\]");
    assert_eq!(like_regex(br"50\%"), r"^50%\z");
    assert_eq!(like_regex(br"ab\"), r"^ab\\\z");
    assert_eq!(like_regex(b""), r"^\z");
    assert_eq!(like_regex(b"a_b%c/d:e"), r"^a.b.*c\/d\:e\z");
}

#[test]
fn like_regex_path_matches() {
    // a middle % forces the regex path
    let l = constant("a%c/d");
    assert!(l.matches(b"abbbc/d"));
    assert!(!l.matches(b"abbbc/dx"));
    assert!(constant("%a.c%x").matches(b"zza.cyyx"));
    assert!(!constant("%a.c%x").matches(b"zzabcyyx"));
}

#[test]
fn like_underscore_is_one_character() {
    assert!(constant("_").matches("é".as_bytes()));
    assert!(!constant("_").matches(b"ab"));
    assert!(constant("a_c").matches("aéc".as_bytes()));
}

#[test]
fn like_fast_paths() {
    assert!(constant("%").matches(b""));
    assert!(constant("").matches(b""));
    assert!(!constant("").matches(b"a"));
    assert!(constant("ab%").matches(b"abc"));
    assert!(!constant("ab%").matches(b"cab"));
    assert!(constant("%ab").matches(b"cab"));
    assert!(constant("%ab%").matches(b"cabd"));
    assert!(constant(r"a\%%").matches(b"a%z"));
    assert!(!constant(r"a\%%").matches(b"az"));
    assert!(constant(r"%a\\").matches(br"xa\"));
}

#[test]
fn like_equals_divergence() {
    // like.cpp:57-61: EQUALS accepts a raw backslash, so a\\_ is the literal a\_ (constant)
    let c = constant(r"a\\_");
    assert!(c.matches(br"a\_"));
    assert!(!c.matches(br"a\x"));
    // like_fn_scalar's extract_like_fast_path counts the escapes: a\\ then a wildcard
    let r = Like::per_row(br"a\\_", None).unwrap();
    assert!(r.matches(br"a\x"));
    assert!(!r.matches(br"a\_x"));
}

#[test]
fn like_custom_escape() {
    let l = Like::constant(b"a!%b", Some(b'!')).unwrap();
    assert!(l.matches(b"a%b"));
    assert!(!l.matches(b"axb"));
    // with an escape other than \, a raw \ is literal
    let l = Like::constant(br"a\%", Some(b'!')).unwrap();
    assert!(l.matches(br"a\xyz"));
    assert!(!l.matches(b"a%"));
}

#[test]
fn like_is_case_sensitive_and_dot_matches_newline() {
    assert!(!constant("AB%").matches(b"abc"));
    assert!(constant("a_c%x").matches(b"a\nc x"));
}

#[test]
fn like_null_is_null() {
    let out = like_column(&[Datum::Null, s("a")], &[s("a"), Datum::Null], false, None).unwrap();
    assert_eq!(out, vec![Datum::Null, Datum::Null]);
    let out = like_column(&[s("a"), s("b")], &[Datum::Null, Datum::Null], true, None).unwrap();
    assert_eq!(out, vec![Datum::Null, Datum::Null]);
}

#[test]
fn like_column_of_patterns() {
    // all rows EQUALS: the vector fast path, with the divergence
    let out = like_column(&[s(r"a\x"), s("b")], &[s(r"a\\_"), s("b")], false, None).unwrap();
    assert_eq!(out, vec![Datum::Bool(false), Datum::Bool(true)]);
    // mixed kinds: each row on its own, escapes counted
    let out = like_column(&[s(r"a\x"), s("bc")], &[s(r"a\\_"), s("b%")], false, None).unwrap();
    assert_eq!(out, vec![Datum::Bool(true), Datum::Bool(true)]);
}

#[test]
fn like_escape_must_be_one_byte() {
    let err = Like::constant(b"a", Some(0)).is_ok();
    assert!(err);
    let e = doris_exec::strings::escape_char(b"ab").unwrap_err();
    assert_eq!(e.to_string(), "Escape character must be a single character, got: ab");
}

#[test]
fn concat_null_is_null() {
    let t = ColType {
        name: String::new(),
        ptype: T::VARCHAR,
        len: -1,
        precision: 0,
        scale: 0,
        nullable: true,
        ..ColType::default()
    };
    let a = vec![s("ab"), Datum::Null];
    let b = vec![s("c"), s("d")];
    assert_eq!(concat_column(&[(&t, &a), (&t, &b)]), vec![s("abc"), Datum::Null]);
}

#[test]
fn length_counts_bytes() {
    assert_eq!(
        length_column(&[s("é"), s(""), Datum::Null]),
        vec![Datum::Int(2), Datum::Int(0), Datum::Null]
    );
}

#[test]
fn a_regex_pattern_holding_nul_is_refused() {
    // Hyperscan compiles the pattern's c_str(): a NUL cuts it short, which doris-rs does not copy
    let err = Like::constant(b"a%b\0c", None).unwrap_err();
    assert!(err.to_string().contains("NUL"), "{err}");
    // a row's pattern is compiled by RE2 at its full length (compile_regex, StringPiece)
    assert!(Like::per_row(b"a%b\0c", None).unwrap().matches(b"axxb\0c"));
    // the fast paths compare bytes, NUL included
    assert!(Like::constant(b"a\0%", None).unwrap().matches(b"a\0z"));
}
