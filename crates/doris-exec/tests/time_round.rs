//! `{year..second}_{floor,ceil}`, `milliseconds_add` and the interval-string adds
//! (`day_hour_add`, …) as the C++ BE computes them: see
//! `docs/superpowers/research/2026-11-11-m6-s7b5-dates.md` §1-3. Expected values are the
//! suites' recorded rows unless worked from the algorithm there.
use doris_exec::block::Block;
use doris_exec::eval::{check_read, eval_column, SlotMap};
use doris_exec::value::{parse_datetimev2, parse_datev2, ColType};
use doris_exec::ExecError;
use doris_fixtures::build::{call, date, int, null, scalar, slot, string, ty};
use doris_segment::writer::Datum;
use doris_thrift::gen::exprs::TExpr;
use doris_thrift::gen::types::{TPrimitiveType as T, TTypeDesc};

fn dt(scale: i32) -> TTypeDesc {
    scalar(T::DATETIMEV2, -1, 18, scale)
}

fn at(s: &str, scale: i32) -> Datum {
    Datum::Int(i128::from(parse_datetimev2(s, scale).unwrap()))
}

fn day(s: &str) -> Datum {
    Datum::Int(i128::from(parse_datev2(s).unwrap()))
}

/// A DATETIMEV2(scale) literal.
fn lit(s: &str, scale: i32) -> TExpr {
    date(s, dt(scale))
}

fn eval(e: TExpr) -> Result<Datum, ExecError> {
    check_read(&e)?;
    let block = Block {
        types: vec![],
        rows: vec![vec![]],
    };
    Ok(eval_column(&e, &block, &SlotMap::default())?.1.remove(0))
}

fn coded(r: Result<Datum, ExecError>) -> (i32, String) {
    match r {
        Err(ExecError::Coded { code, msg, .. }) => (code, msg),
        other => panic!("{other:?}"),
    }
}

/// `name(args)` returning DATETIMEV2(scale).
fn round(name: &str, scale: i32, args: Vec<TExpr>) -> Result<Datum, ExecError> {
    eval(call(name, dt(scale), args))
}

#[test]
fn time_rounding_with_the_default_origin() {
    let one = |name: &str, s: &str| round(name, 0, vec![lit(s, 0)]).unwrap();
    // the recorded rows of test_time_round
    assert_eq!(one("day_ceil", "2022-05-25 00:00:00"), at("2022-05-25 00:00:00", 0));
    assert_eq!(one("month_ceil", "2022-05-01 00:00:00"), at("2022-05-01 00:00:00", 0));
    assert_eq!(one("minute_ceil", "2022-05-25 00:05:10"), at("2022-05-25 00:06:00", 0));
    assert_eq!(one("minute_floor", "2022-05-25 00:05:10"), at("2022-05-25 00:05:00", 0));
    // weeks start on Monday (0001-01-01)
    assert_eq!(one("week_ceil", "2022-05-25 00:00:00"), at("2022-05-30 00:00:00", 0));
    assert_eq!(one("week_floor", "2022-05-25 00:00:00"), at("2022-05-23 00:00:00", 0));
    assert_eq!(one("month_ceil", "2022-05-25 00:00:00"), at("2022-06-01 00:00:00", 0));
    assert_eq!(one("month_floor", "2022-05-25 00:00:00"), at("2022-05-01 00:00:00", 0));
    assert_eq!(one("year_ceil", "2022-05-25 00:00:00"), at("2023-01-01 00:00:00", 0));
    assert_eq!(one("year_floor", "2022-05-25 00:00:00"), at("2022-01-01 00:00:00", 0));
    assert_eq!(one("quarter_floor", "2023-06-15 08:30:45"), at("2023-04-01 00:00:00", 0));
    assert_eq!(one("second_ceil", "2023-06-15 08:30:45"), at("2023-06-15 08:30:45", 0));
    assert_eq!(one("hour_floor", "2023-06-15 08:30:45"), at("2023-06-15 08:00:00", 0));
}

#[test]
fn time_rounding_with_period_and_origin() {
    let o = |s: &str| lit(s, 0);
    assert_eq!(
        round(
            "hour_ceil",
            0,
            vec![o("1970-01-01 01:00:10"), int(1, ty(T::INT)), o("1970-01-01 00:00:30")]
        )
        .unwrap(),
        at("1970-01-01 01:00:30", 0)
    );
    assert_eq!(
        round(
            "hour_floor",
            0,
            vec![o("1970-01-01 01:00:10"), int(1, ty(T::INT)), o("1970-01-01 00:00:30")]
        )
        .unwrap(),
        at("1970-01-01 00:00:30", 0)
    );
    assert_eq!(
        round(
            "day_ceil",
            0,
            vec![o("2022-05-25 02:00:00"), int(3, ty(T::INT)), o("2022-05-20 00:00:00")]
        )
        .unwrap(),
        at("2022-05-26 00:00:00", 0)
    );
    assert_eq!(
        round(
            "day_floor",
            0,
            vec![o("2022-05-25 02:00:00"), int(3, ty(T::INT)), o("2022-05-20 00:00:00")]
        )
        .unwrap(),
        at("2022-05-23 00:00:00", 0)
    );
    // the origin's sub-unit fields are kept, the day clamped to the month's end
    assert_eq!(
        round(
            "quarter_ceil",
            0,
            vec![o("2023-06-15 08:30:45"), int(1, ty(T::INT)), o("1999-12-31 00:00:00")]
        )
        .unwrap(),
        at("2023-06-30 00:00:00", 0)
    );
    // (date, origin)
    assert_eq!(
        round("quarter_floor", 0, vec![o("2023-06-15 08:30:45"), o("2000-02-01 00:00:00")]).unwrap(),
        at("2023-05-01 00:00:00", 0)
    );
    // (date, period)
    assert_eq!(
        round("year_floor", 0, vec![o("2023-06-15 08:30:45"), int(5, ty(T::INT))]).unwrap(),
        at("2021-01-01 00:00:00", 0)
    );
    // microseconds of the origin and the argument
    let w = |name: &str, a: &str| round(name, 6, vec![lit(a, 6), int(1, ty(T::INT)), lit("2024-01-01 00:00:00.500000", 6)]).unwrap();
    assert_eq!(w("week_floor", "2024-01-08 00:00:00.400000"), at("2024-01-01 00:00:00.500000", 6));
    assert_eq!(w("week_ceil", "2024-01-08 00:00:00.600000"), at("2024-01-15 00:00:00.500000", 6));
}

#[test]
fn date_rounding_keeps_dates() {
    let r = eval(call("month_floor", ty(T::DATEV2), vec![date("2023-06-15", ty(T::DATEV2))])).unwrap();
    assert_eq!(r, day("2023-06-01"));
    // 165 days after the origin: on a 3-day step already
    let r = eval(call(
        "day_ceil",
        ty(T::DATEV2),
        vec![date("2023-06-15", ty(T::DATEV2)), int(3, ty(T::INT)), date("2023-01-01", ty(T::DATEV2))],
    ))
    .unwrap();
    assert_eq!(r, day("2023-06-15"));
    // with a time of day past the step it moves up (the research's worked example)
    let r = round(
        "day_ceil",
        0,
        vec![lit("2023-06-15 08:30:45", 0), int(3, ty(T::INT)), lit("2023-01-01 00:00:00", 0)],
    )
    .unwrap();
    assert_eq!(r, at("2023-06-18 00:00:00", 0));
    let r = eval(call("week_ceil", ty(T::DATEV2), vec![date("2022-05-25", ty(T::DATEV2))])).unwrap();
    assert_eq!(r, day("2022-05-30"));
}

#[test]
fn rounding_past_9999_is_out_of_range() {
    assert_eq!(
        coded(round("year_ceil", 0, vec![lit("9999-12-31 00:00:00", 0)])),
        (-218, "Operation year_ceil of 9999-12-31 00:00:00 out of range".into())
    );
    assert_eq!(
        coded(round("year_ceil", 0, vec![lit("9999-12-31 00:00:00", 0), lit("2000-01-01 00:00:00", 0)])),
        (
            -218,
            "Operation year_ceil of 9999-12-31 00:00:00, 2000-01-01 00:00:00 out of range".into()
        )
    );
    // all arguments constant: C++ unwraps them and names every one (test_date_floor_ceil #440)
    assert_eq!(
        coded(round("month_floor", 0, vec![lit("2023-07-14 15:30:45", 0), int(0, ty(T::INT))])),
        (-218, "Operation month_floor of 2023-07-14 15:30:45, 0 out of range".into())
    );
}

/// `date_floor`/`date_ceil` of constants: the default origin the FE adds shows in the error,
/// whether the period is below 1 or the result passes 9999 (test_date_floor_ceil #36, #39, #438).
#[test]
fn all_constant_errors_name_the_default_origin() {
    let origin = || lit("0001-01-01 00:00:00", 0);
    assert_eq!(
        coded(round(
            "year_floor",
            6,
            vec![lit("9999-12-31 23:59:59.999999", 6), int(-10, ty(T::INT)), origin()]
        )),
        (
            -218,
            "Operation year_floor of 9999-12-31 23:59:59.999999, -10, 0001-01-01 00:00:00 out of range".into()
        )
    );
    assert_eq!(
        coded(round(
            "minute_ceil",
            6,
            vec![lit("9999-12-31 23:59:59.999999", 6), int(5, ty(T::INT)), origin()]
        )),
        (
            -218,
            "Operation minute_ceil of 9999-12-31 23:59:59.999999, 5, 0001-01-01 00:00:00 out of range".into()
        )
    );
    assert_eq!(
        coded(round("month_floor", 0, vec![lit("2007-01-02 00:00:00", 0), int(0, ty(T::INT)), origin()])),
        (
            -218,
            "Operation month_floor of 2007-01-02 00:00:00, 0, 0001-01-01 00:00:00 out of range".into()
        )
    );
}

/// A column date with a constant period below 1 fails before any row, naming the period
/// alone (test_time_round #47: `hour_floor(dt, 0)`).
#[test]
fn a_constant_period_over_a_column_names_the_period() {
    let (block, slots) = column_block();
    let e = call("hour_floor", dt(0), vec![slot(1, dt(0)), int(0, ty(T::INT))]);
    match eval_column(&e, &block, &slots) {
        Err(ExecError::Coded { code: -218, msg, .. }) => assert_eq!(msg, "Operation hour_floor of 0 out of range"),
        other => panic!("{other:?}"),
    }
}

fn column_block() -> (Block, SlotMap) {
    let c = |t: TTypeDesc| ColType::from_type_desc("", &t, true).unwrap();
    let rows = [
        ("2020-02-02 13:09:20", Some(1), "1999-12-31 11:45:14"),
        ("2020-02-02 13:09:20", Some(2), "1919-08-10 11:45:14"),
        ("2020-02-02 13:09:20", Some(4), "1145-01-04 19:19:00"),
        ("2020-02-02 13:09:20", None, "1145-01-04 19:19:00"),
    ];
    let block = Block {
        types: vec![c(dt(0)), c(ty(T::INT)), c(dt(0))],
        rows: rows
            .iter()
            .map(|(a, p, o)| vec![at(a, 0), p.map_or(Datum::Null, |p| Datum::Int(p.into())), at(o, 0)])
            .collect(),
    };
    (block, SlotMap([(1, 0), (2, 1), (3, 2)].into_iter().collect()))
}

#[test]
fn rounding_over_columns() {
    let (block, slots) = column_block();
    let e = call("hour_floor", dt(0), vec![slot(1, dt(0)), slot(2, ty(T::INT)), slot(3, dt(0))]);
    check_read(&e).unwrap();
    let got = eval_column(&e, &block, &slots).unwrap().1;
    // the recorded rows; a NULL period is a NULL row
    assert_eq!(
        got,
        vec![
            at("2020-02-02 12:45:14", 0),
            at("2020-02-02 11:45:14", 0),
            at("2020-02-02 11:19:00", 0),
            Datum::Null
        ]
    );
    // a column period below 1 names the row's date and period
    let mut zero = block.clone();
    zero.rows[0][1] = Datum::Int(0);
    let e = call("hour_floor", dt(0), vec![slot(1, dt(0)), slot(2, ty(T::INT))]);
    match eval_column(&e, &zero, &slots) {
        Err(ExecError::Coded { code: -218, msg, .. }) => assert_eq!(msg, "Operation hour_floor of 2020-02-02 13:09:20, 0 out of range"),
        other => panic!("{other:?}"),
    }
}

#[test]
fn milliseconds_add_and_sub() {
    let ms = |name: &str, s: &str, n: i64| eval(call(name, dt(6), vec![lit(s, 6), int(n, ty(T::BIGINT))]));
    assert_eq!(
        ms("milliseconds_add", "2023-01-01 00:00:00.999000", 2).unwrap(),
        at("2023-01-01 00:00:01.001000", 6)
    );
    assert_eq!(
        ms("milliseconds_sub", "2023-01-01 00:00:00", 1).unwrap(),
        at("2022-12-31 23:59:59.999000", 6)
    );
    assert_eq!(
        coded(ms("milliseconds_add", "9999-12-31 23:59:59.999999", 1)),
        (-218, "Operation millisecond_add of 9999-12-31 23:59:59.999999, 1 out of range".into())
    );
}

#[test]
fn interval_string_adds() {
    let add = |name: &str, scale: i32, s: &str, iv: &str| eval(call(name, dt(scale), vec![lit(s, scale), string(iv)]));
    // the recorded rows of test_dateadd_with_other_timeunit
    assert_eq!(
        add("day_second_add", 6, "2025-10-24 01:02:03.123456", "1 1:1:1").unwrap(),
        at("2025-10-25 02:03:04.123456", 6)
    );
    assert_eq!(
        add("day_second_add", 6, "2025-10-29 00:00:00", "-1 -1:-1:-1").unwrap(),
        at("2025-10-27 22:58:59", 6)
    );
    assert_eq!(
        add("day_hour_add", 6, "2025-10-29 10:10:10", "1 -10").unwrap(),
        at("2025-10-30 20:10:10", 6)
    );
    assert_eq!(
        add("day_hour_add", 6, "2025-10-29 10:10:10", "xx 10").unwrap(),
        at("2025-10-29 20:10:10", 6)
    );
    assert_eq!(
        add("second_microsecond_add", 6, "2025-10-24 01:02:03.123456", "1.123423432").unwrap(),
        at("2025-10-24 01:04:07.546888", 6)
    );
    assert_eq!(
        add("second_microsecond_add", 6, "2025-10-24 00:00:00", "-1.-11233213123").unwrap(),
        at("2025-10-23 20:52:45.786877", 6)
    );
    // missing leading parts are 0; the microsecond part is scaled by its digits
    assert_eq!(
        add("day_second_add", 0, "2025-01-01 00:00:00", "1 2").unwrap(),
        at("2025-01-01 00:01:02", 0)
    );
    assert_eq!(
        add("second_microsecond_add", 6, "2025-01-01 00:00:00", "1.12").unwrap(),
        at("2025-01-01 00:00:01.120000", 6)
    );
    assert_eq!(
        add("minute_second_add", 0, "2025-01-01 00:00:00", "2:30").unwrap(),
        at("2025-01-01 00:02:30", 0)
    );
    // _sub negates the string
    assert_eq!(
        add("day_hour_sub", 0, "2025-01-02 00:00:00", "1 1").unwrap(),
        at("2024-12-31 23:00:00", 0)
    );
    // too many groups is invalid, worded as the add
    match add("day_hour_add", 0, "2025-01-01 00:00:00", "1 2 3") {
        Err(ExecError::Coded { name, msg, .. }) => {
            assert_eq!(name, "INVALID_ARGUMENT");
            assert_eq!(msg, "Operation day_hour_add of 1 2 3 is invalid");
        }
        other => panic!("{other:?}"),
    }
    // NULL interval, NULL row
    assert_eq!(
        eval(call("day_hour_add", dt(0), vec![lit("2025-01-01 00:00:00", 0), null(ty(T::VARCHAR))])).unwrap(),
        Datum::Null
    );
}

// review fixes

/// A NULL constant period (or origin) makes every row NULL.
#[test]
fn a_null_constant_period_is_null() {
    assert_eq!(
        round("hour_floor", 0, vec![lit("2020-01-01 10:00:00", 0), null(ty(T::INT))]).unwrap(),
        Datum::Null
    );
    assert_eq!(
        round(
            "hour_floor",
            0,
            vec![lit("2020-01-01 10:00:00", 0), null(ty(T::INT)), lit("2020-01-01 00:00:00", 0)]
        )
        .unwrap(),
        Datum::Null
    );
    assert_eq!(
        round("hour_floor", 0, vec![lit("2020-01-01 10:00:00", 0), int(2, ty(T::INT)), null(dt(0))]).unwrap(),
        Datum::Null
    );
}

/// With a column period, a constant default origin still shows in the error (only the
/// all-constant form drops it).
#[test]
fn a_column_period_error_shows_the_default_origin() {
    let (block, slots) = column_block();
    let mut zero = block.clone();
    zero.rows[0][1] = Datum::Int(0);
    let e = call(
        "hour_floor",
        dt(0),
        vec![slot(1, dt(0)), slot(2, ty(T::INT)), lit("0001-01-01 00:00:00", 0)],
    );
    match eval_column(&e, &zero, &slots) {
        Err(ExecError::Coded { msg, .. }) => assert_eq!(msg, "Operation hour_floor of 2020-02-02 13:09:20, 0, 0001-01-01 00:00:00 out of range"),
        other => panic!("{other:?}"),
    }
}
