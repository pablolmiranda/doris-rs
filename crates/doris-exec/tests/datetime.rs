//! Date functions as the C++ BE computes them (`core/value/vdatetime_value.cpp`,
//! `exprs/function/function_date_or_datetime_computation.h`): see
//! `docs/superpowers/research/2026-11-10-m6-s7b2-dates-bits.md`.
use doris_exec::block::Block;
use doris_exec::eval::{check_read, eval_column, SlotMap};
use doris_exec::value::{parse_datetimev2, parse_datev2};
use doris_exec::ExecError;
use doris_fixtures::build::{call, date, int, null, scalar, string, ty};
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

fn d() -> TTypeDesc {
    ty(T::DATEV2)
}

fn dt(scale: i32) -> TTypeDesc {
    scalar(T::DATETIMEV2, -1, 18, scale)
}

fn day(s: &str) -> Datum {
    Datum::Int(i128::from(parse_datev2(s).unwrap()))
}

fn at(s: &str, scale: i32) -> Datum {
    Datum::Int(i128::from(parse_datetimev2(s, scale).unwrap()))
}

/// `f(DATEV2 'v', n)` into DATEV2.
fn add_days(f: &str, v: &str, n: i64) -> Result<Datum, ExecError> {
    eval(call(f, d(), vec![date(v, d()), int(n, ty(T::INT))]))
}

#[test]
fn months_add_clamps_to_month_end() {
    assert_eq!(add_days("months_add", "2024-01-31", 1).unwrap(), day("2024-02-29"));
    assert_eq!(add_days("months_add", "2023-01-31", 1).unwrap(), day("2023-02-28"));
    assert_eq!(add_days("months_sub", "2023-03-31", 1).unwrap(), day("2023-02-28"));
    assert_eq!(add_days("months_add", "2023-12-15", 1).unwrap(), day("2024-01-15"));
}

#[test]
fn years_add_leap_day_becomes_28th() {
    assert_eq!(add_days("years_add", "2024-02-29", 1).unwrap(), day("2025-02-28"));
    assert_eq!(add_days("years_sub", "2024-02-29", 4).unwrap(), day("2020-02-29"));
}

#[test]
fn year_zero_is_not_leap() {
    assert_eq!(add_days("days_add", "0000-02-28", 1).unwrap(), day("0000-03-01"));
    assert_eq!(add_days("weeks_add", "0000-01-01", 1).unwrap(), day("0000-01-08"));
    assert_eq!(add_days("days_add", "2024-02-28", 1).unwrap(), day("2024-02-29"));
}

fn coded(e: Result<Datum, ExecError>) -> (i32, &'static str, String) {
    match e {
        Err(ExecError::Coded { code, name, msg }) => (code, name, msg),
        other => panic!("{other:?}"),
    }
}

#[test]
fn add_past_9999_is_e218_with_text() {
    let (code, name, msg) = coded(add_days("years_add", "9999-12-31", 1));
    assert_eq!((code, name), (-218, "E-218"));
    assert_eq!(msg, "Operation year_add of 9999-12-31, 1 out of range");
    assert_eq!(
        coded(add_days("months_add", "9999-12-31", 1)).2,
        "Operation month_add of 9999-12-31, 1 out of range"
    );
    assert_eq!(
        coded(add_days("weeks_add", "9999-12-31", 1)).2,
        "Operation week_add of 9999-12-31, 1 out of range"
    );
}

#[test]
fn sub_prints_negative_delta() {
    assert_eq!(
        coded(add_days("days_sub", "0000-01-01", 1)).2,
        "Operation day_add of 0000-01-01, -1 out of range"
    );
    // a datetime prints its fraction only when it has one, whatever its scale
    let e = call("hours_add", dt(6), vec![date("9999-12-31 23:00:00.5", dt(6)), int(1, ty(T::INT))]);
    assert_eq!(coded(eval(e)).2, "Operation hour_add of 9999-12-31 23:00:00.500000, 1 out of range");
    let e = call("seconds_sub", dt(0), vec![date("0000-01-01 00:00:00", dt(0)), int(1, ty(T::BIGINT))]);
    assert_eq!(coded(eval(e)).2, "Operation second_add of 0000-01-01 00:00:00, -1 out of range");
}

#[test]
fn null_rows_never_throw() {
    let e = call("years_add", d(), vec![null(d()), int(1, ty(T::INT))]);
    assert_eq!(one_row(e), Datum::Null);
}

#[test]
fn time_units_carry_into_days() {
    let e = call("hours_add", dt(0), vec![date("2024-02-28 23:00:00", dt(0)), int(2, ty(T::INT))]);
    assert_eq!(one_row(e), at("2024-02-29 01:00:00", 0));
    let e = call("minutes_sub", dt(0), vec![date("2024-03-01 00:00:00", dt(0)), int(1, ty(T::BIGINT))]);
    assert_eq!(one_row(e), at("2024-02-29 23:59:00", 0));
    let e = call("years_add", dt(6), vec![date("2024-02-29 10:11:12.000013", dt(6)), int(1, ty(T::INT))]);
    assert_eq!(one_row(e), at("2025-02-28 10:11:12.000013", 6));
}

fn diff(f: &str, a: &str, b: &str) -> Datum {
    one_row(call(f, ty(T::BIGINT), vec![date(a, dt(6)), date(b, dt(6))]))
}

#[test]
fn days_diff_moves_toward_zero() {
    assert_eq!(diff("days_diff", "2024-01-02 00:00:00", "2024-01-01 12:00:00"), Datum::Int(0));
    assert_eq!(diff("days_diff", "2024-01-03 12:00:00", "2024-01-01 12:00:00"), Datum::Int(2));
    assert_eq!(diff("days_diff", "2024-01-01 12:00:00", "2024-01-02 00:00:00"), Datum::Int(0));
    assert_eq!(diff("days_diff", "0000-01-08 12:00:00", "0000-01-01 13:00:00"), Datum::Int(6));
    assert_eq!(diff("weeks_diff", "2024-01-15 00:00:00", "2024-01-01 00:00:01"), Datum::Int(1));
    assert_eq!(diff("weeks_diff", "2024-01-01 00:00:00", "2024-01-15 00:00:00"), Datum::Int(-2));
}

fn week(f: &str, v: &str, mode: Option<i64>) -> Datum {
    let mut args = vec![date(v, d())];
    let to = if mode.is_some() { ty(T::INT) } else { ty(T::TINYINT) };
    args.extend(mode.map(|m| int(m, ty(T::INT))));
    one_row(call(f, to, args))
}

#[test]
fn weekofyear_mode_3() {
    // ISO weeks: Monday first, week 1 holds the year's first Thursday
    assert_eq!(week("weekofyear", "2000-01-01", None), Datum::Int(52));
    assert_eq!(week("weekofyear", "2024-01-01", None), Datum::Int(1));
    assert_eq!(week("weekofyear", "2021-01-03", None), Datum::Int(53));
    assert_eq!(week("weekofyear", "2020-12-31", None), Datum::Int(53));
    assert_eq!(week("weekofyear", "0000-06-01", None), Datum::Int(0));
}

#[test]
fn yearweek_mode_6() {
    // mode 6: Sunday first, week 1 has 4 or more days of the year
    assert_eq!(week("yearweek", "2021-01-03", Some(6)), Datum::Int(202101));
    assert_eq!(week("yearweek", "2021-01-01", Some(6)), Datum::Int(202053));
    assert_eq!(week("yearweek", "2020-01-01", Some(6)), Datum::Int(202001));
}

#[test]
fn year_month_day_parts() {
    assert_eq!(one_row(call("year", ty(T::SMALLINT), vec![date("2024-03-05", d())])), Datum::Int(2024));
    assert_eq!(one_row(call("month", ty(T::TINYINT), vec![date("2024-03-05", d())])), Datum::Int(3));
    assert_eq!(
        one_row(call("dayofmonth", ty(T::TINYINT), vec![date("2024-03-05 10:00:00", dt(0))])),
        Datum::Int(5)
    );
}

fn str_to_date(v: &str, f: &str) -> Datum {
    one_row(call("str_to_date", d(), vec![string(v), string(f)]))
}

#[test]
fn str_to_date_invalid_is_null() {
    assert_eq!(str_to_date("2024-02-03", "%Y-%m-%d"), day("2024-02-03"));
    assert_eq!(str_to_date("  2024-2-3 tail", "%Y-%m-%d"), day("2024-02-03"));
    assert_eq!(str_to_date("2024-13-01", "%Y-%m-%d"), Datum::Null);
    assert_eq!(str_to_date("2023-02-29", "%Y-%m-%d"), Datum::Null);
    assert_eq!(str_to_date("2024/02/03", "%Y-%m-%d"), Datum::Null);
    assert_eq!(str_to_date("24-02-03", "%Y-%m-%d"), day("2024-02-03"));
}

#[test]
fn str_to_date_refuses_unported_specifiers() {
    let e = call("str_to_date", d(), vec![string("Monday"), string("%W")]);
    assert!(eval(e).is_err());
}

/// C++ evaluates every all-constant subtree at open (`VExpr::get_const_col`), so its error
/// fails the fragment before any row is read.
#[test]
fn a_constant_overflow_fails_at_prepare() {
    use doris_exec::eval::check_constants;
    use doris_fixtures::build::{binary, slot};
    let over = call("years_add", d(), vec![date("9999-12-31", d()), int(1, ty(T::INT))]);
    let e = binary("eq", over, date("2024-01-01", d()));
    let (code, _, msg) = coded(check_constants(&e).map(|_| Datum::Null));
    assert_eq!(code, -218);
    assert_eq!(msg, "Operation year_add of 9999-12-31, 1 out of range");
    // a column operand waits for the rows
    let col = call("years_add", d(), vec![slot(1, d()), int(1, ty(T::INT))]);
    check_constants(&binary("eq", col, date("2024-01-01", d()))).unwrap();
    // a constant that evaluates is fine
    check_constants(&call("years_add", d(), vec![date("2024-01-01", d()), int(1, ty(T::INT))])).unwrap();
}
