//! `regexp` / `rlike`: C++'s classifiers, then Hyperscan (constant patterns), RE2 (per row) and,
//! with `enable_extended_regex`, Boost — matched with `regex` and `fancy-regex` behind gates.
use doris_exec::regexp::{classify, regexp_column, Class, RegexOptions};
use doris_exec::ExecError;
use doris_segment::writer::Datum;

fn s(v: &str) -> Datum {
    Datum::Bytes(v.as_bytes().to_vec())
}

const OFF: RegexOptions = RegexOptions {
    extended: false,
    hyperscan_fallback: true,
};
const EXT: RegexOptions = RegexOptions {
    extended: true,
    hyperscan_fallback: true,
};

fn one(value: &str, pattern: &str, constant: bool, o: RegexOptions) -> Result<Datum, ExecError> {
    Ok(regexp_column(&[s(value)], &[s(pattern)], constant, o)?.remove(0))
}

#[test]
fn classifies_like_cpp() {
    assert_eq!(classify(b".*"), Some(Class::AllPass));
    assert_eq!(classify(b"^abc$"), Some(Class::Equals(b"abc".to_vec())));
    assert_eq!(classify(b"^abc"), Some(Class::StartsWith(b"abc".to_vec())));
    assert_eq!(classify(b"^abc.*"), Some(Class::StartsWith(b"abc".to_vec())));
    assert_eq!(classify(b"abc$"), Some(Class::EndsWith(b"abc".to_vec())));
    assert_eq!(classify(b"abc"), Some(Class::Substring(b"abc".to_vec())));
    assert_eq!(classify(b""), Some(Class::Substring(vec![])));
    assert_eq!(classify("上海-x".as_bytes()), Some(Class::Substring("上海-x".as_bytes().to_vec())));
    assert_eq!(classify(b".*suffix.$"), None);
    assert_eq!(classify("^上海｜^北京".as_bytes()), None);
}

#[test]
fn literal_classes_search_like_a_regex() {
    assert_eq!(one("prefix0_infix0_suffix0", "^prefix", true, OFF).unwrap(), Datum::Bool(true));
    assert_eq!(
        one("prefix0_infix0_suffix0", "prefix0_infix0_suffix0", true, OFF).unwrap(),
        Datum::Bool(true)
    );
    assert_eq!(one("xabc", "^abc$", true, OFF).unwrap(), Datum::Bool(false));
    assert_eq!(one("", "", false, OFF).unwrap(), Datum::Bool(true));
}

#[test]
fn hyperscan_path_matches_dot_anything() {
    assert_eq!(one("prefix0_infix0_suffix0", ".*suffix.$", true, OFF).unwrap(), Datum::Bool(true));
    assert_eq!(one("prefix0_infix0_suffix", ".*suffix.$", true, OFF).unwrap(), Datum::Bool(false));
    // a mid-pattern ^ (the bar is U+FF5C, a literal) never matches
    assert_eq!(one("上海", "^上海｜^北京", true, OFF).unwrap(), Datum::Bool(false));
    assert_eq!(one("a\nb", "a.b", true, OFF).unwrap(), Datum::Bool(true));
}

#[test]
fn ascii_classes_on_utf8() {
    assert_eq!(one("中文", r"\w+", true, OFF).unwrap(), Datum::Bool(false));
    assert_eq!(one("中文", r"\w+", false, OFF).unwrap(), Datum::Bool(false));
    assert_eq!(one("中文", r"[中文]+", true, OFF).unwrap(), Datum::Bool(true));
    assert_eq!(one("x1", r"\d$", false, OFF).unwrap(), Datum::Bool(true));
    assert_eq!(one("中", r"\b中", false, OFF).unwrap(), Datum::Bool(false));
    // RE2's \s has no \v
    assert_eq!(one("\u{b}", r"^\s$", false, OFF).unwrap(), Datum::Bool(false));
    assert_eq!(one("\t", r"^\s$", false, OFF).unwrap(), Datum::Bool(true));
}

#[test]
fn null_value_or_pattern_is_null() {
    let v = regexp_column(&[Datum::Null, s("foobar")], &[s("foo(?=1)"), Datum::Null], false, EXT).unwrap();
    assert_eq!(v, vec![Datum::Null, Datum::Null]);
}

#[test]
fn re2_rejects_lookaround_with_its_text() {
    let want = "Invalid regex expression: (?<=foo)bar. Error: invalid perl operator: (?<. If you need advanced regex features, try setting enable_extended_regex=true";
    for constant in [true, false] {
        match one("foobar", "(?<=foo)bar", constant, OFF) {
            Err(ExecError::Internal(m)) => assert_eq!(m, want),
            other => panic!("{other:?}"),
        }
    }
    match one("x", "foo(?=123)", false, OFF) {
        Err(ExecError::Internal(m)) => assert!(m.contains("Error: invalid perl operator: (?=. If you need"), "{m}"),
        other => panic!("{other:?}"),
    }
}

#[test]
fn per_row_patterns_compile_even_for_null_values() {
    // row 6 of the extended suite: NULL value, pattern RE2 rejects
    let r = regexp_column(&[Datum::Null], &[s("foo(?=123)")], false, OFF);
    assert!(matches!(r, Err(ExecError::Internal(_))), "{r:?}");
}

#[test]
fn boost_runs_lookaround_with_extended_regex() {
    let rows = [
        ("foobar", "(?<=foo)bar", true),
        ("fobar", "(?<=foo)bar", false),
        ("foo123bar", "foo(?=123)", true),
        ("foo124bar", "foo(?=123)", false),
        ("foobar", "^foo", true),
    ];
    for (v, p, want) in rows {
        assert_eq!(one(v, p, false, EXT).unwrap(), Datum::Bool(want), "{v} {p}");
        assert_eq!(one(v, p, true, EXT).unwrap(), Datum::Bool(want), "{v} {p} constant");
    }
}

#[test]
fn boost_reports_an_unclosed_group() {
    let want = "Invalid regex expression: (. Error: Unmatched marking parenthesis ( or \\(.  The error occurred while parsing the regular expression: '(>>>HERE>>>'.";
    match one("foobar", "(", false, EXT) {
        Err(ExecError::Internal(m)) => assert_eq!(m, want),
        other => panic!("{other:?}"),
    }
}

#[test]
fn boost_nested_quantifiers_are_refused() {
    let r = one(&"a".repeat(60), "(?=a)(a+)+b", false, EXT);
    assert!(matches!(r, Err(ExecError::Unsupported { .. })), "{r:?}");
}

#[test]
fn hyperscan_dollar_before_a_final_newline_is_refused() {
    let r = one("ab\n", "a.$", true, OFF);
    assert!(matches!(r, Err(ExecError::Unsupported { .. })), "{r:?}");
    assert_eq!(one("ab\n", "a.$", false, OFF).unwrap(), Datum::Bool(false));
}

#[test]
fn constant_case_insensitive_flag_is_refused() {
    let r = one("ABC", "(?i)a.c", true, OFF);
    assert!(matches!(r, Err(ExecError::Unsupported { .. })), "{r:?}");
}
