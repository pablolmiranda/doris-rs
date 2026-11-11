//! String functions as the C++ BE computes them (`be/src/exprs/function/function_string.cpp`,
//! `function_string_concat.h`, `function_string_misc.cpp`, `function_hex.cpp`, `nullif.cpp`):
//! see `docs/superpowers/research/2026-11-10-m6-s7b3-strings.md`.
use doris_exec::block::Block;
use doris_exec::eval::{check_read, eval_column, SlotMap};
use doris_exec::ExecError;
use doris_fixtures::build::{call, int, null, scalar, string, ty};
use doris_segment::writer::Datum;
use doris_thrift::gen::exprs::TExpr;
use doris_thrift::gen::types::{TPrimitiveType as T, TTypeDesc};

fn eval(e: TExpr) -> Result<Datum, ExecError> {
    let block = Block {
        types: vec![],
        rows: vec![vec![]],
    };
    check_read(&e)?;
    let (_, v) = eval_column(&e, &block, &SlotMap::default())?;
    Ok(v.into_iter().next().unwrap())
}

fn one_row(e: TExpr) -> Datum {
    eval(e).expect("eval")
}

fn vc() -> TTypeDesc {
    scalar(T::VARCHAR, 65533, 0, 0)
}

fn s(v: &str) -> TExpr {
    string(v)
}

fn text(v: &[u8]) -> Datum {
    Datum::Bytes(v.to_vec())
}

fn str_call(name: &str, args: Vec<TExpr>) -> Datum {
    one_row(call(name, vc(), args))
}

fn i32_(v: i64) -> TExpr {
    int(v, ty(T::INT))
}

#[test]
fn upper_non_ascii() {
    assert_eq!(str_call("upper", vec![s("ab1")]), text(b"AB1"));
    assert_eq!(str_call("upper", vec![s("straße")]), text("STRASSE".as_bytes()));
}

#[test]
fn replace_empty_old_is_noop() {
    assert_eq!(str_call("replace", vec![s("a:b"), s(""), s("x")]), text(b"a:b"));
    assert_eq!(str_call("replace", vec![s("a:b:c"), s(":"), s("\\:")]), text(b"a\\:b\\:c"));
    assert_eq!(str_call("replace", vec![s("aaa"), s("aa"), s("b")]), text(b"ba"));
}

#[test]
fn nullif_with_null_returns_first() {
    let n = |a: TExpr, b: TExpr| one_row(call("nullif", ty(T::INT), vec![a, b]));
    assert_eq!(n(i32_(1), null(ty(T::INT))), Datum::Int(1));
    assert_eq!(n(i32_(1), i32_(1)), Datum::Null);
    assert_eq!(n(null(ty(T::INT)), i32_(1)), Datum::Null);
    assert_eq!(n(i32_(2), i32_(1)), Datum::Int(2));
}

#[test]
fn find_in_set_comma_needle_is_zero() {
    let f = |a: &str, b: &str| one_row(call("find_in_set", ty(T::INT), vec![s(a), s(b)]));
    assert_eq!(f("b", "a,b,c"), Datum::Int(2));
    assert_eq!(f("1 ", "1 "), Datum::Int(1));
    assert_eq!(f("1,2", "1,2"), Datum::Int(0));
    assert_eq!(f("", ""), Datum::Int(1));
    assert_eq!(f("x", "a"), Datum::Int(0));
    assert_eq!(one_row(call("find_in_set", ty(T::INT), vec![s("a"), null(vc())])), Datum::Null);
}

#[test]
fn char_skips_nulls_and_invalid_utf8_is_null() {
    let c = |args: Vec<TExpr>| {
        let mut all = vec![s("utf8")];
        all.extend(args);
        one_row(call("char", ty(T::STRING), all))
    };
    assert_eq!(c(vec![i32_(10)]), text(b"\n"));
    assert_eq!(c(vec![null(ty(T::INT)), i32_(65)]), text(b"A"));
    assert_eq!(c(vec![i32_(0xE4B8AD)]), text("中".as_bytes()));
    assert_eq!(c(vec![i32_(0)]), text(b"\0"));
    assert_eq!(c(vec![i32_(255)]), Datum::Null);
}

#[test]
fn pad_counts_chars_and_truncates() {
    let p = |f: &str, v: &str, n: i64, pad: &str| str_call(f, vec![s(v), i32_(n), s(pad)]);
    assert_eq!(p("lpad", "100000", 2, "0"), text(b"10"));
    assert_eq!(p("rpad", "100000", 2, "0"), text(b"10"));
    assert_eq!(p("lpad", "7", 3, "0"), text(b"007"));
    assert_eq!(p("rpad", "a", 4, "xy"), text(b"axyx"));
    assert_eq!(p("lpad", "中", 3, "ab"), text("ab中".as_bytes()));
    assert_eq!(p("rpad", "123", 5, ""), text(b""));
    assert_eq!(p("lpad", "", 0, "x"), text(b""));
    assert_eq!(p("lpad", "123", -1, ""), Datum::Null);
}

#[test]
fn unhex_bad_input_is_empty() {
    let mut nul = call("unhex", vc(), vec![s("610062")]);
    assert_eq!(one_row(nul.clone()), text(b"a\0b"));
    nul = call("hex", vc(), vec![nul]);
    assert_eq!(one_row(nul), text(b"610062"));
    assert_eq!(str_call("unhex", vec![s("6a6A")]), text(b"jj"));
    assert_eq!(str_call("unhex", vec![s("6")]), text(b""));
    assert_eq!(str_call("unhex", vec![s("zz")]), text(b""));
    assert_eq!(str_call("hex", vec![s("")]), text(b""));
}

#[test]
fn crc32_is_bigint() {
    let c = |v: &str| one_row(call("crc32", ty(T::BIGINT), vec![s(v)]));
    assert_eq!(c("DORIS"), Datum::Int(348606243));
    assert_eq!(c("APACHE DORIS"), Datum::Int(130583814));
    assert_eq!(c("10"), Datum::Int(2707236321));
}

#[test]
fn parse_data_size_units_and_errors() {
    let p = |v: &str| eval(call("parse_data_size", ty(T::LARGEINT), vec![s(v)]));
    assert_eq!(p("1MB").unwrap(), Datum::Int(1048576));
    assert_eq!(p("2.5MB").unwrap(), Datum::Int(2621440));
    assert_eq!(p("0B").unwrap(), Datum::Int(0));
    assert_eq!(p("1kB").unwrap(), Datum::Int(1024));
    // the leading run of digits and dots is the number, read as far as stod reads it
    assert_eq!(p("1.2.3MB").unwrap(), Datum::Int(1258291));
    let err = |v: &str| match p(v) {
        Err(ExecError::Coded { name, msg, .. }) => {
            assert_eq!(name, "INVALID_ARGUMENT");
            msg
        }
        other => panic!("{other:?}"),
    };
    assert_eq!(err(""), "Invalid Input argument \"\" of function parse_data_size");
    assert_eq!(err("invalid"), "Invalid Input argument \"invalid\" of function parse_data_size");
    // units are case-sensitive
    assert_eq!(err("1KB"), "Invalid Input argument \"1KB\" of function parse_data_size");
    assert_eq!(err(".MB"), "Invalid Input argument \".MB\" of function parse_data_size, error: stod");
    assert_eq!(one_row(call("parse_data_size", ty(T::LARGEINT), vec![null(vc())])), Datum::Null);
}

#[test]
fn trim_in_uses_a_char_set() {
    assert_eq!(str_call("trim_in", vec![s("abcba"), s("ab")]), text(b"c"));
    assert_eq!(str_call("ltrim_in", vec![s("abcba"), s("ab")]), text(b"cba"));
    assert_eq!(str_call("rtrim_in", vec![s("abcba"), s("ab")]), text(b"abc"));
    assert_eq!(str_call("trim_in", vec![s("aaba"), s("a")]), text(b"b"));
    assert_eq!(str_call("trim_in", vec![s("abc"), s("")]), text(b"abc"));
    assert_eq!(str_call("trim_in", vec![s("浙江省杭州市"), s("省市杭州")]), text("浙江".as_bytes()));
    assert_eq!(str_call("ltrim_in", vec![s("  a ")]), text(b"a "));
    assert_eq!(str_call("rtrim_in", vec![s("  a ")]), text(b"  a"));
    assert_eq!(str_call("trim_in", vec![s("  a ")]), text(b"a"));
}

/// INSERT … VALUES evaluates its expressions on the write path (`expr.rs`).
#[test]
fn string_functions_in_insert_values() {
    use doris_exec::expr;
    use doris_exec::value::Value;
    let e = call("unhex", vc(), vec![s("6100")]);
    expr::check(&e).unwrap();
    assert_eq!(expr::eval(&e, &Default::default()).unwrap(), Value::Str(b"a\0".to_vec()));
    let e = call("concat", vc(), vec![s("a"), call("char", ty(T::STRING), vec![s("utf8"), i32_(10)])]);
    expr::check(&e).unwrap();
    assert_eq!(expr::eval(&e, &Default::default()).unwrap(), Value::Str(b"a\n".to_vec()));
}
