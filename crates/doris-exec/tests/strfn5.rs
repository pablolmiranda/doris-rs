//! `elt`, `locate`, `null_or_empty`, `not_null_or_empty`, `sub_replace`, `starts_with`,
//! `money_format` and `format_round` as the C++ BE computes them: see
//! `docs/superpowers/research/2026-11-11-m6-s7b5-strings-hashes.md` §1 and
//! `docs/superpowers/research/2026-11-11-m6-s7b5-math.md` (money_format, format_round). Expected
//! values are the suites' recorded rows.
use doris_exec::block::Block;
use doris_exec::eval::{check_read, eval_column, SlotMap};
use doris_exec::value::ColType;
use doris_exec::ExecError;
use doris_fixtures::build::{call, decimal, float, int, null, scalar, slot, string, ty};
use doris_segment::writer::Datum;
use doris_thrift::gen::exprs::TExpr;
use doris_thrift::gen::types::{TPrimitiveType as T, TTypeDesc};

fn vc() -> TTypeDesc {
    scalar(T::VARCHAR, 65533, 0, 0)
}

fn eval(e: TExpr) -> Result<Datum, ExecError> {
    check_read(&e)?;
    let block = Block {
        types: vec![],
        rows: vec![vec![]],
    };
    Ok(eval_column(&e, &block, &SlotMap::default())?.1.remove(0))
}

fn text(e: TExpr) -> Option<String> {
    match eval(e).unwrap() {
        Datum::Bytes(b) => Some(String::from_utf8(b).unwrap()),
        Datum::Null => None,
        other => panic!("{other:?}"),
    }
}

fn i(v: i64) -> TExpr {
    int(v, ty(T::INT))
}

#[test]
fn elt_picks_its_argument() {
    let e = |p: i64| text(call("elt", vc(), vec![i(p), string("hello"), string("doris")]));
    assert_eq!(e(0), None);
    assert_eq!(e(1).as_deref(), Some("hello"));
    assert_eq!(e(2).as_deref(), Some("doris"));
    assert_eq!(e(3), None);
    // a NULL that is not chosen is ignored; a chosen one is NULL
    assert_eq!(text(call("elt", vc(), vec![i(1), string("a"), null(vc())])).as_deref(), Some("a"));
    assert_eq!(text(call("elt", vc(), vec![i(2), string("a"), null(vc())])), None);
}

#[test]
fn locate_counts_characters() {
    let three = |sub: &str, s: &str, pos: i64| eval(call("locate", ty(T::INT), vec![string(sub), string(s), i(pos)])).unwrap();
    // the recorded table rows: LOCATE('78', str, 2)
    assert_eq!(three("78", "123456789", 2), Datum::Int(7));
    assert_eq!(three("78", "1234567890", 2), Datum::Int(7));
    assert_eq!(three("78", "abcabc", 2), Datum::Int(0));
    assert_eq!(three("78", "789", 2), Datum::Int(0));
    // MySQL's empty case, an empty needle, positions in characters
    assert_eq!(three("", "", 1), Datum::Int(1));
    assert_eq!(three("", "abc", 2), Datum::Int(2));
    assert_eq!(three("界", "你好世界", 2), Datum::Int(4));
    assert_eq!(three("你", "你好世界", 2), Datum::Int(0));
    let two = |sub: &str, s: &str| eval(call("locate", ty(T::INT), vec![string(sub), string(s)])).unwrap();
    assert_eq!(two("bar", "foobarbar"), Datum::Int(4));
    assert_eq!(two("xbar", "foobar"), Datum::Int(0));
    assert_eq!(two("", ""), Datum::Int(1));
    assert_eq!(two("世", "你好世界"), Datum::Int(3));
}

#[test]
fn null_or_empty_never_null() {
    let b = |name: &str, e: TExpr| eval(call(name, ty(T::BOOLEAN), vec![e])).unwrap();
    assert_eq!(b("null_or_empty", null(vc())), Datum::Bool(true));
    assert_eq!(b("null_or_empty", string("")), Datum::Bool(true));
    assert_eq!(b("null_or_empty", string("a")), Datum::Bool(false));
    assert_eq!(b("not_null_or_empty", null(vc())), Datum::Bool(false));
    assert_eq!(b("not_null_or_empty", string("")), Datum::Bool(false));
    assert_eq!(b("not_null_or_empty", string("a")), Datum::Bool(true));
}

#[test]
fn sub_replace_as_recorded() {
    let s3 = |a: &str, b: &str, start: i64| text(call("sub_replace", vc(), vec![string(a), string(b), i(start)]));
    let s4 = |a: &str, b: &str, start: i64, len: i64| text(call("sub_replace", vc(), vec![string(a), string(b), i(start), i(len)]));
    assert_eq!(s3("this is origin str", "NEW-STR", 1).as_deref(), Some("tNEW-STRorigin str"));
    assert_eq!(s4("doris", "***", 1, 2).as_deref(), Some("d***is"));
    assert_eq!(s3("你好世界", "a", 1).as_deref(), Some("你a世界"));
    assert_eq!(s3("你好世界", "ab", 1).as_deref(), Some("你ab界"));
    assert_eq!(s4("你好世界", "ab", 1, 20).as_deref(), Some("你ab"));
    assert_eq!(s4("你好世界", "abcd我", 1, 2).as_deref(), Some("你abcd我界"));
    assert_eq!(s3("你好世界", "a", 6), None);
    assert_eq!(s3("你好世界", "大家", 0).as_deref(), Some("大家世界"));
    assert_eq!(s4("你好世界", "大家114514", 1, 20).as_deref(), Some("你大家114514"));
    assert_eq!(s4("你好世界", "大家114514", 6, 20), None);
    assert_eq!(s3("你好世界", "大家", 4), None);
    assert_eq!(s3("你好世界", "大家", -1), None);
    // ASCII text: the 3-arg length is the replacement's character count, in bytes
    assert_eq!(s3("abcd", "大家", 0).as_deref(), Some("大家cd"));
    assert_eq!(s3("", "x", 0), None);
}

#[test]
fn starts_with_bytes() {
    let sw = |a: &str, b: &str| eval(call("starts_with", ty(T::BOOLEAN), vec![string(a), string(b)])).unwrap();
    assert_eq!(sw("hello world", "hello"), Datum::Bool(true));
    assert_eq!(sw("hello world", "world"), Datum::Bool(false));
    assert_eq!(sw("x", ""), Datum::Bool(true));
}

fn dec(p: i32, s: i32) -> TTypeDesc {
    let t = match p {
        0..=9 => T::DECIMAL32,
        10..=18 => T::DECIMAL64,
        _ => T::DECIMAL128I,
    };
    scalar(t, -1, p, s)
}

#[test]
fn money_format_as_recorded() {
    let m = |e: TExpr| text(call("money_format", vc(), vec![e])).unwrap();
    let d = |v: &str, p: i32, s: i32| m(decimal(v, dec(p, s)));
    assert_eq!(d("0.000", 3, 3), "0.00");
    assert_eq!(d("-123.125", 6, 3), "-123.13");
    assert_eq!(d("1123.456", 7, 3), "1,123.46");
    assert_eq!(d("1123.4", 5, 1), "1,123.40");
    assert_eq!(d("1.1249", 5, 4), "1.12");
    assert_eq!(d("1.12499999", 9, 8), "1.12");
    assert_eq!(d("-0.1", 1, 1), "-0.10");
    assert_eq!(d("-0.114", 3, 3), "-0.11");
    assert_eq!(d("-0.115", 3, 3), "-0.12");
    assert_eq!(d("0.999999999", 9, 9), "1.00");
    assert_eq!(d("-9.99999999999999999", 18, 17), "-10.00");
    assert_eq!(
        d("99999999999999999999999999999999999999", 38, 0),
        "99,999,999,999,999,999,999,999,999,999,999,999,999.00"
    );
    assert_eq!(m(int(-17014116, ty(T::BIGINT))), "-17,014,116.00");
    assert_eq!(m(int(i64::MIN, ty(T::BIGINT))), "-9,223,372,036,854,775,808.00");
    assert_eq!(m(float(1.2353, ty(T::DOUBLE))), "1.24");
    assert_eq!(m(float(-123456789.2353, ty(T::DOUBLE))), "-123,456,789.24");
    assert_eq!(m(float(1.125, ty(T::DOUBLE))), "1.13");
    assert_eq!(m(float(-1e-308, ty(T::DOUBLE))), "-0.00");
    assert!(m(float(1e308, ty(T::DOUBLE))).starts_with("100,000,000,000,000,001,097,906,362,"));
}

#[test]
fn format_round_copies_cpp() {
    let f = |e: TExpr, places: i64| text(call("format_round", vc(), vec![e, i(places)])).unwrap();
    let d = |v: &str, p: i32, s: i32, places: i64| f(decimal(v, dec(p, s)), places);
    assert_eq!(d("1234567.8910", 11, 4, 5), "1,234,567.89100");
    assert_eq!(d("-1111.235", 7, 3, 2), "-1,111.24");
    assert_eq!(d("123.49", 5, 2, 0), "123");
    assert_eq!(d("-1.4", 9, 1, 0), "-1");
    assert_eq!(d("9876.54321", 9, 5, 0), "9,877");
    assert_eq!(d("0.0000001", 7, 7, 7), "0.0000001");
    assert_eq!(d("-123.456789", 9, 6, 3), "-123.457");
    // C++'s int truncations
    assert_eq!(d("1.23456789", 9, 8, 10), "1.1949288396");
    assert_eq!(d("123.456789", 9, 6, 20), "123.00000000002147026859");
    assert_eq!(f(int(123456, ty(T::BIGINT)), 3), "123,456.000");
    assert_eq!(f(float(1.44999, ty(T::DOUBLE)), 5), "1.44999");
    assert_eq!(f(float(-0.4, ty(T::DOUBLE)), 0), "-0");
    let tiny = f(float(2.2250738585072014e-308, ty(T::DOUBLE)), 324);
    assert_eq!(tiny, format!("0.{}22250738585072014", "0".repeat(307)));
    // places outside 0..=1024
    for bad in [-1, 1025] {
        match eval(call("format_round", vc(), vec![int(1, ty(T::BIGINT)), i(bad)])) {
            Err(ExecError::Coded { name, msg, .. }) => {
                assert_eq!(name, "INVALID_ARGUMENT");
                assert_eq!(msg, format!("The second argument is {bad}, it should be in range [0, 1024]."));
            }
            other => panic!("{other:?}"),
        }
    }
}

/// Over a column, money_format of a NULL row is NULL.
#[test]
fn money_format_over_a_column() {
    let c = ColType::from_type_desc("", &ty(T::BIGINT), true).unwrap();
    let block = Block {
        types: vec![c],
        rows: vec![vec![Datum::Int(1)], vec![Datum::Null]],
    };
    let e = call("money_format", vc(), vec![slot(1, ty(T::BIGINT))]);
    check_read(&e).unwrap();
    let got = eval_column(&e, &block, &SlotMap([(1, 0)].into_iter().collect())).unwrap().1;
    assert_eq!(got, vec![Datum::Bytes(b"1.00".to_vec()), Datum::Null]);
}

/// On the INSERT … VALUES path a failing or numeric call would only filter its row: refused.
#[test]
fn number_formatting_is_refused_in_insert_values() {
    assert!(doris_exec::expr::check(&call("money_format", vc(), vec![int(1, ty(T::BIGINT))])).is_err());
    assert!(doris_exec::expr::check(&call("format_round", vc(), vec![int(1, ty(T::BIGINT)), i(2)])).is_err());
    // the text ones run there
    doris_exec::expr::check(&call("sub_replace", vc(), vec![string("abc"), string("x"), i(0)])).unwrap();
}
