//! The array functions as the C++ BE computes them (`exprs/function/array/`; research
//! `docs/superpowers/research/2026-11-12-m6-s7b6-arrays.md` §2): `array_with_constant`,
//! `array_join`, `array_range` and its time-unit forms, `array_pushback`/`pushfront` and
//! `array_popback`/`popfront`.
use doris_exec::block::Block;
use doris_exec::eval::{check_read, eval_column, SlotMap};
use doris_exec::row_text::cell_text;
use doris_exec::ExecError;
use doris_fixtures::build::{array_literal, array_of, call, date, int, null, scalar, string, ty};
use doris_segment::writer::Datum;
use doris_thrift::gen::exprs::TExpr;
use doris_thrift::gen::types::{TPrimitiveType as T, TTypeDesc};

fn one_row(e: &TExpr) -> Result<Datum, ExecError> {
    check_read(e)?;
    let block = Block {
        types: vec![],
        rows: vec![vec![]],
    };
    Ok(eval_column(e, &block, &SlotMap::default())?.1.remove(0))
}

/// The value's text in a result row ("NULL" for NULL).
fn text(e: &TExpr) -> String {
    let (t, mut v) = {
        check_read(e).expect("supported");
        let block = Block {
            types: vec![],
            rows: vec![vec![]],
        };
        eval_column(e, &block, &SlotMap::default()).expect("eval")
    };
    cell_text(&v.remove(0), &t).map_or("NULL".into(), |b| String::from_utf8(b).unwrap())
}

fn invalid(e: &TExpr, want: &str) {
    match one_row(e) {
        Err(ExecError::Coded { name, msg, .. }) => {
            assert_eq!(name, "INVALID_ARGUMENT");
            assert_eq!(msg, want);
        }
        other => panic!("{other:?}"),
    }
}

fn varchar(len: i32) -> TTypeDesc {
    scalar(T::VARCHAR, len, 0, 0)
}

fn bigint(v: i64) -> TExpr {
    int(v, ty(T::BIGINT))
}

fn int32(v: i64) -> TExpr {
    int(v, ty(T::INT))
}

#[test]
fn array_with_constant_as_cpp() {
    let awc = |n: TExpr, v: TExpr, elem: TTypeDesc| call("array_with_constant", array_of(elem), vec![n, v]);
    assert_eq!(text(&awc(bigint(3), null(ty(T::INT)), ty(T::INT))), "[null, null, null]");
    assert_eq!(text(&awc(bigint(3), int32(0), ty(T::INT))), "[0, 0, 0]");
    // a NULL count is read as 0, never a NULL array
    assert_eq!(text(&awc(null(ty(T::BIGINT)), string("Hello"), varchar(5))), "[]");
    assert_eq!(text(&awc(bigint(1), string("Hello"), varchar(5))), "[\"Hello\"]");
    let empty = array_literal(ty(T::NULL_TYPE), vec![]);
    assert_eq!(text(&awc(bigint(2), empty, array_of(ty(T::NULL_TYPE)))), "[[], []]");
    match one_row(&awc(bigint(1_000_000), int32(1), ty(T::INT))).unwrap() {
        Datum::Array(a) => assert_eq!(a.len(), 1_000_000),
        other => panic!("{other:?}"),
    }
    let msg = "Array size should in range(0, 1000000) in function: array_with_constant";
    invalid(&awc(bigint(1_000_001), int(1, ty(T::TINYINT)), ty(T::TINYINT)), msg);
    invalid(&awc(bigint(-231), int(-138, ty(T::SMALLINT)), ty(T::SMALLINT)), msg);
}

#[test]
fn array_join_as_cpp() {
    let words = ["", "hello", "", "world", "", "xyz", "def", ""];
    let arr = array_literal(
        ty(T::STRING),
        words.iter().map(|w| if w.is_empty() { null(ty(T::STRING)) } else { string(w) }).collect(),
    );
    let join = |args: Vec<TExpr>| call("array_join", ty(T::STRING), args);
    assert_eq!(text(&join(vec![arr.clone(), string(";")])), "hello;world;xyz;def");
    // an empty null_replace skips NULLs, a non-empty one stands in for them
    assert_eq!(text(&join(vec![arr.clone(), string(";"), string("")])), "hello;world;xyz;def");
    assert_eq!(text(&join(vec![arr, string(";"), string("-")])), "-;hello;-;world;-;xyz;def;-");
    assert_eq!(text(&join(vec![array_literal(ty(T::STRING), vec![]), string(",")])), "");
    let nulls = array_literal(ty(T::STRING), vec![null(ty(T::STRING))]);
    assert_eq!(text(&join(vec![nulls, string(",")])), "");
    // a NULL argument is a NULL row
    assert_eq!(text(&join(vec![null(array_of(ty(T::STRING))), string(",")])), "NULL");
    assert_eq!(
        text(&join(vec![array_literal(ty(T::STRING), vec![string("a")]), null(ty(T::STRING))])),
        "NULL"
    );
}

fn range(args: Vec<TExpr>) -> TExpr {
    call("array_range", array_of(ty(T::INT)), args)
}

#[test]
fn array_range_as_cpp() {
    assert_eq!(text(&range(vec![int32(5)])), "[0, 1, 2, 3, 4]");
    assert_eq!(text(&range(vec![int32(1), int32(4)])), "[1, 2, 3]");
    assert_eq!(text(&range(vec![int32(0), int32(5), int32(2)])), "[0, 2, 4]");
    assert_eq!(text(&range(vec![int32(2), int32(8), int32(10)])), "[2]");
    assert_eq!(text(&range(vec![int32(5), int32(2)])), "[]");
    for args in [
        vec![null(ty(T::INT)), int32(5)],
        vec![int32(-1), int32(3)],
        vec![int32(1), int32(-3)],
        vec![int32(0), int32(3), int32(0)],
        vec![int32(0), int32(3), int32(-1)],
        // no size check on a NULL row
        vec![null(ty(T::INT)), int32(5_000_000)],
    ] {
        assert_eq!(text(&range(args)), "NULL");
    }
    match one_row(&range(vec![int32(0), int32(2_000_000), int32(2)])).unwrap() {
        Datum::Array(a) => assert_eq!(a.len(), 1_000_000),
        other => panic!("{other:?}"),
    }
    invalid(&range(vec![int32(0), int32(1_000_001)]), "Array size exceeds the limit 1000000");
    invalid(&range(vec![int32(0), int32(2_000_001), int32(2)]), "Array size exceeds the limit 1000000");
}

fn dt(s: &str, scale: i32) -> TExpr {
    date(s, scalar(T::DATETIMEV2, -1, 0, scale))
}

fn unit_range(name: &str, start: &str, end: &str, step: i64, scale: i32) -> String {
    let t = scalar(T::DATETIMEV2, -1, 0, scale);
    text(&call(name, array_of(t), vec![dt(start, scale), dt(end, scale), int32(step)]))
}

#[test]
fn array_range_second_unit_as_cpp() {
    assert_eq!(
        unit_range(
            "array_range_second_unit",
            "2024-01-01 00:00:00.123456",
            "2024-01-01 00:00:03.123456",
            1,
            6
        ),
        "[\"2024-01-01 00:00:00.123456\", \"2024-01-01 00:00:01.123456\", \"2024-01-01 00:00:02.123456\"]"
    );
    assert_eq!(
        unit_range("array_range_second_unit", "2024-01-01 00:00:00", "2024-01-01 00:00:02", 1, 6),
        "[\"2024-01-01 00:00:00.000000\", \"2024-01-01 00:00:01.000000\"]"
    );
    // a value below the end belongs even if less than a unit remains
    assert_eq!(
        unit_range("array_range_minute_unit", "2024-01-01 00:00:00", "2024-01-01 00:02:30", 1, 0),
        "[\"2024-01-01 00:00:00\", \"2024-01-01 00:01:00\", \"2024-01-01 00:02:00\"]"
    );
    assert_eq!(
        unit_range("array_range_day_unit", "2024-02-28 00:00:00", "2024-03-02 00:00:00", 1, 0),
        "[\"2024-02-28 00:00:00\", \"2024-02-29 00:00:00\", \"2024-03-01 00:00:00\"]"
    );
    // each month added to the last value, clamped to the month's end
    assert_eq!(
        unit_range("array_range_month_unit", "2024-01-31 00:00:00", "2024-04-01 00:00:00", 1, 0),
        "[\"2024-01-31 00:00:00\", \"2024-02-29 00:00:00\", \"2024-03-29 00:00:00\"]"
    );
    assert_eq!(
        unit_range("array_range_year_unit", "9998-01-01 00:00:00", "9999-12-31 00:00:00", 1, 0),
        "[\"9998-01-01 00:00:00\", \"9999-01-01 00:00:00\"]"
    );
    // a step that leaves the date range stops the array
    assert_eq!(
        unit_range("array_range_year_unit", "9999-01-01 00:00:00", "9999-12-31 00:00:00", 5, 0),
        "[\"9999-01-01 00:00:00\"]"
    );
    assert_eq!(
        unit_range("array_range_day_unit", "2024-01-01 00:00:00", "2024-01-03 00:00:00", 0, 0),
        "NULL"
    );
    // the two-argument form steps by a day
    let t = scalar(T::DATETIMEV2, -1, 0, 0);
    let two = call(
        "array_range",
        array_of(t),
        vec![dt("2024-01-01 00:00:00", 0), dt("2024-01-03 00:00:00", 0)],
    );
    assert_eq!(text(&two), "[\"2024-01-01 00:00:00\", \"2024-01-02 00:00:00\"]");
}

#[test]
fn push_and_pop_as_cpp() {
    let tiny = |v: i64| int(v, ty(T::TINYINT));
    let arr = |v: Vec<TExpr>| array_literal(ty(T::TINYINT), v);
    let at = array_of(ty(T::TINYINT));
    let f = |name: &str, args: Vec<TExpr>| call(name, at.clone(), args);
    assert_eq!(text(&f("array_pushback", vec![null(at.clone()), tiny(1)])), "NULL");
    assert_eq!(text(&f("array_pushback", vec![arr(vec![tiny(1)]), null(ty(T::TINYINT))])), "[1, null]");
    assert_eq!(text(&f("array_pushfront", vec![arr(vec![tiny(1)]), null(ty(T::TINYINT))])), "[null, 1]");
    assert_eq!(text(&f("array_pushfront", vec![arr(vec![]), tiny(1)])), "[1]");
    assert_eq!(text(&f("array_pushback", vec![arr(vec![tiny(1), tiny(2)]), tiny(3)])), "[1, 2, 3]");
    assert_eq!(text(&f("array_popback", vec![arr(vec![tiny(1)])])), "[]");
    assert_eq!(text(&f("array_popback", vec![arr(vec![])])), "[]");
    assert_eq!(
        text(&f("array_popback", vec![arr(vec![tiny(1), null(ty(T::TINYINT)), tiny(3)])])),
        "[1, null]"
    );
    assert_eq!(
        text(&f("array_popfront", vec![arr(vec![tiny(1), null(ty(T::TINYINT)), tiny(3)])])),
        "[null, 3]"
    );
    assert_eq!(text(&f("array_popfront", vec![null(at.clone())])), "NULL");
    let s = array_literal(varchar(2), vec![string("cd"), string("a"), string("b")]);
    assert_eq!(text(&call("array_popfront", array_of(varchar(2)), vec![s])), "[\"a\", \"b\"]");
    // the element type of the result is the FE's: decimals keep their scale
    let d = |s: &str| doris_fixtures::build::decimal(s, scalar(T::DECIMAL32, -1, 2, 1));
    let dec = array_literal(scalar(T::DECIMAL32, -1, 2, 1), vec![d("0.5"), d("0.7")]);
    let pushed = call("array_pushback", array_of(scalar(T::DECIMAL32, -1, 2, 1)), vec![dec, d("1.0")]);
    assert_eq!(text(&pushed), "[0.5, 0.7, 1.0]");
}
