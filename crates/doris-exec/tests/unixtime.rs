//! `from_second`/`_millisecond`/`_microsecond`, `from_unixtime` and `unix_timestamp` (their
//! `_new` versions, `new_version_unix_timestamp`), `months_between`, `next_day` and
//! `date_format` as the C++ BE computes them: see
//! `docs/superpowers/research/2026-11-11-m6-s7b5-dates.md` §4-9.
use doris_exec::block::Block;
use doris_exec::clock::{find_zone, with_clock, QueryClock};
use doris_exec::eval::{check_read, eval_column, SlotMap};
use doris_exec::regexp::{with_regex_options, RegexOptions};
use doris_exec::value::{parse_datetimev2, parse_datev2, ColType};
use doris_exec::ExecError;
use doris_fixtures::build::{boolean, call, date, decimal, int, null, scalar, slot, string, ty};
use doris_segment::writer::Datum;
use doris_thrift::gen::exprs::TExpr;
use doris_thrift::gen::types::{TPrimitiveType as T, TTypeDesc};

fn dt(scale: i32) -> TTypeDesc {
    scalar(T::DATETIMEV2, -1, 18, scale)
}

fn vc() -> TTypeDesc {
    scalar(T::VARCHAR, 65533, 0, 0)
}

fn at(s: &str, scale: i32) -> Datum {
    Datum::Int(i128::from(parse_datetimev2(s, scale).unwrap()))
}

fn day(s: &str) -> Datum {
    Datum::Int(i128::from(parse_datev2(s).unwrap()))
}

fn text(s: &str) -> Datum {
    Datum::Bytes(s.as_bytes().to_vec())
}

/// Runs `f` as a query in `zone` with `new_version_unix_timestamp` set as the reference does.
fn in_zone<R>(zone: &str, f: impl FnOnce() -> R) -> R {
    let clock = QueryClock {
        zone: find_zone(zone).unwrap(),
        seconds: 1_700_000_000,
        nanos: 0,
    };
    let opts = RegexOptions {
        new_unix_timestamp: true,
        ..RegexOptions::default()
    };
    with_clock(&clock, || with_regex_options(opts, f))
}

fn eval(e: TExpr) -> Result<Datum, ExecError> {
    check_read(&e)?;
    let block = Block {
        types: vec![],
        rows: vec![vec![]],
    };
    Ok(eval_column(&e, &block, &SlotMap::default())?.1.remove(0))
}

fn utc(e: TExpr) -> Result<Datum, ExecError> {
    in_zone("UTC", || eval(e))
}

fn coded(r: Result<Datum, ExecError>) -> (&'static str, String) {
    match r {
        Err(ExecError::Coded { name, msg, .. }) => (name, msg),
        other => panic!("{other:?}"),
    }
}

fn big(v: i64) -> TExpr {
    int(v, ty(T::BIGINT))
}

#[test]
fn from_seconds_family_in_each_zone() {
    let f = |name: &str, scale: i32, v: i64, zone: &str| in_zone(zone, || eval(call(name, dt(scale), vec![big(v)])));
    // the last second of 9999 in each recorded session zone
    assert_eq!(f("from_second", 0, 253402300799, "+00:00").unwrap(), at("9999-12-31 23:59:59", 0));
    assert_eq!(f("from_second", 0, 253402250399, "+14:00").unwrap(), at("9999-12-31 23:59:59", 0));
    assert_eq!(f("from_second", 0, 253402343999, "-12:00").unwrap(), at("9999-12-31 23:59:59", 0));
    assert_eq!(
        f("from_millisecond", 3, 253402300799999, "UTC").unwrap(),
        at("9999-12-31 23:59:59.999", 3)
    );
    assert_eq!(
        f("from_microsecond", 6, 253402300799999999, "UTC").unwrap(),
        at("9999-12-31 23:59:59.999999", 6)
    );
    assert_eq!(f("from_second", 0, 0, "UTC").unwrap(), at("1970-01-01 00:00:00", 0));
    // one past is an error, as is a negative value and a value past the zone limit
    assert_eq!(
        coded(f("from_second", 0, 253402300800, "UTC")),
        ("E-218", "Operation from_second of 253402300800 out of range".into())
    );
    assert_eq!(
        coded(f("from_millisecond", 3, i64::MAX, "UTC")),
        ("E-218", "Operation from_millisecond of 9223372036854775807 out of range".into())
    );
    assert_eq!(
        coded(f("from_second", 0, -1, "UTC")),
        ("E-218", "Operation from_second of -1 out of range".into())
    );
    assert_eq!(
        in_zone("UTC", || eval(call("from_second", dt(0), vec![null(ty(T::BIGINT))]))).unwrap(),
        Datum::Null
    );
}

#[test]
fn from_unixtime_new() {
    let f = |args: Vec<TExpr>| utc(call("from_unixtime", vc(), args));
    assert_eq!(f(vec![big(253402300799)]).unwrap(), text("9999-12-31 23:59:59"));
    assert_eq!(f(vec![big(0), string("%Y-%m-%d %H:%i:%s")]).unwrap(), text("1970-01-01 00:00:00"));
    assert_eq!(f(vec![big(86400 * 31), string("%Y%m%d")]).unwrap(), text("19700201"));
    assert_eq!(
        coded(f(vec![big(253402300800), string("%Y-%m-%d %H:%i:%s")])),
        (
            "INVALID_ARGUMENT",
            "Operation from_unixtime_new of 253402300800, %Y-%m-%d %H:%i:%s is invalid".into()
        )
    );
    assert_eq!(
        coded(f(vec![big(253402300800)])),
        (
            "INVALID_ARGUMENT",
            "Operation from_unixtime_new of 253402300800, yyyy-MM-dd HH:mm:ss is invalid".into()
        )
    );
    // DECIMAL(18,6) seconds
    let d = scalar(T::DECIMAL64, -1, 18, 6);
    assert_eq!(f(vec![decimal("253402300799.999999", d)]).unwrap(), text("9999-12-31 23:59:59.999999"));
    // without new_version_unix_timestamp it is refused
    let old = with_clock(
        &QueryClock {
            zone: find_zone("UTC").unwrap(),
            seconds: 0,
            nanos: 0,
        },
        || check_read(&call("from_unixtime", vc(), vec![big(0)])),
    );
    assert!(old.is_err());
}

#[test]
fn unix_timestamp_new() {
    let dec = |p: i32, s: i32| scalar(T::DECIMAL64, -1, p, s);
    let f = |zone: &str, e: TExpr| in_zone(zone, || eval(e));
    // (DATEV2) in the session zone; before 1970 is 0
    let d = date("1970-01-02", ty(T::DATEV2));
    assert_eq!(
        f("UTC", call("unix_timestamp", ty(T::BIGINT), vec![d.clone()])).unwrap(),
        Datum::Int(86400)
    );
    assert_eq!(
        f("+14:00", call("unix_timestamp", ty(T::BIGINT), vec![date("1970-01-01", ty(T::DATEV2))])).unwrap(),
        Datum::Int(0)
    );
    // (DATETIMEV2(s)) -> DECIMAL(12+s, s)
    let e = call("unix_timestamp", dec(15, 3), vec![date("1970-01-01 00:00:01.250", dt(3))]);
    assert_eq!(f("UTC", e).unwrap(), Datum::Int(1250));
    // (string, format) -> DECIMAL(18,6); a value that does not parse is an error
    let e = call("unix_timestamp", dec(18, 6), vec![string("01.Jan.1970"), string("%d.%b.%Y")]);
    assert_eq!(f("UTC", e).unwrap(), Datum::Int(0));
    let e = call(
        "unix_timestamp",
        dec(18, 6),
        vec![string("9999-12-31 23:59:59"), string("%Y-%m-%d %H:%i:%s")],
    );
    assert_eq!(f("UTC", e).unwrap(), Datum::Int(253402300799 * 1_000_000));
    let e = call("unix_timestamp", dec(18, 6), vec![string("x"), string("%Y")]);
    assert_eq!(
        coded(f("UTC", e)),
        ("INVALID_ARGUMENT", "Operation unix_timestamp of x, %Y is invalid".into())
    );
    // () is the query's instant
    assert_eq!(
        f("UTC", call("unix_timestamp", ty(T::BIGINT), vec![])).unwrap(),
        Datum::Int(1_700_000_000)
    );
}

#[test]
fn months_between_as_recorded() {
    let mb = |a: &str, b: &str, round: bool| {
        let e = call(
            "months_between",
            ty(T::DOUBLE),
            vec![date(a, ty(T::DATEV2)), date(b, ty(T::DATEV2)), boolean(round)],
        );
        match utc(e).unwrap() {
            Datum::Double(v) => v,
            other => panic!("{other:?}"),
        }
    };
    assert_eq!(mb("2020-06-30", "2020-07-01", true), -0.06451613);
    assert_eq!(mb("1900-01-01", "2100-12-31", true), -2411.96774194);
    assert_eq!(mb("2020-02-28", "2021-02-28", true), -12.0);
    // both at their month's end: no day term
    assert_eq!(mb("2020-01-31", "2020-02-29", true), -1.0);
    assert_eq!(mb("2020-06-30", "2020-07-01", false), -1.0 + 29.0 / 31.0);
}

#[test]
fn next_day_as_recorded() {
    let nd = |d: &str, w: &str| utc(call("next_day", ty(T::DATEV2), vec![date(d, ty(T::DATEV2)), string(w)]));
    assert_eq!(nd("2020-02-27", "SAT").unwrap(), day("2020-02-29"));
    assert_eq!(nd("2020-02-29", "MON").unwrap(), day("2020-03-02"));
    assert_eq!(nd("2019-02-26", "THU").unwrap(), day("2019-02-28"));
    assert_eq!(nd("2019-02-28", "SUN").unwrap(), day("2019-03-03"));
    assert_eq!(nd("2019-02-28", "thursday").unwrap(), day("2019-03-07"));
    // past 9999-12-31 the date stays
    assert_eq!(nd("9999-12-31", "MON").unwrap(), day("9999-12-31"));
    assert_eq!(
        coded(nd("2019-02-28", "XYZ")),
        ("INVALID_ARGUMENT", "Function next_day failed to parse weekday: XYZ".into())
    );
}

/// With a constant date and a column weekday, C++ moves the one date row after row.
#[test]
fn next_day_accumulates_over_a_constant_date() {
    let c = ColType::from_type_desc("", &vc(), true).unwrap();
    let block = Block {
        types: vec![c],
        rows: vec![vec![text("MON")], vec![text("MON")]],
    };
    let slots = SlotMap([(1, 0)].into_iter().collect());
    let e = call("next_day", ty(T::DATEV2), vec![date("2020-02-27", ty(T::DATEV2)), slot(1, vc())]);
    check_read(&e).unwrap();
    let got = in_zone("UTC", || eval_column(&e, &block, &slots)).unwrap().1;
    assert_eq!(got, vec![day("2020-03-02"), day("2020-03-09")]);
}

#[test]
fn date_format_specifiers() {
    let df = |d: &str, scale: i32, f: &str| match utc(call("date_format", vc(), vec![date(d, dt(scale)), string(f)])).unwrap() {
        Datum::Bytes(b) => String::from_utf8(b).unwrap(),
        other => panic!("{other:?}"),
    };
    // the recorded rows
    for f in ["%h", "%I", "%l"] {
        assert_eq!(df("2020-12-01 00:00:30.01", 2, f), "12");
        assert_eq!(df("2020-12-01 12:00:30.01", 2, f), "12");
    }
    assert_eq!(df("2020-12-01 00:00:30.01", 2, "%r"), "12:00:30 AM");
    assert_eq!(df("2020-12-01 12:00:30.01", 2, "%r"), "12:00:30 PM");
    assert_eq!(df("2020-02-02 00:00:00", 0, "%Y%m%d"), "20200202");
    // worked from the formatter
    assert_eq!(df("2021-03-01 09:05:07.000123", 6, "%y %c %e %k %i %S %f %p"), "21 3 1 9 05 07 000123 AM");
    assert_eq!(df("2021-03-01 09:05:07", 0, "%a %W %b %M %D %j %w"), "Mon Monday Mar March 1st 060 1");
    assert_eq!(df("2021-01-01 00:00:00", 0, "%u %U %v %V %x %X"), "00 00 53 52 2020 2020");
    assert_eq!(df("2021-01-01 00:00:00", 0, "100%% %Q %"), "100% Q %");
    assert_eq!(df("2021-01-01 00:00:00", 0, "yyyy-MM-dd HH:mm:ss"), "2021-01-01 00:00:00");
    // an output past the length guard fails
    let long = "%Y".repeat(40);
    let e = call("date_format", vc(), vec![date("2021-01-01 00:00:00", dt(0)), string(&long)]);
    assert_eq!(coded(utc(e)).0, "INVALID_ARGUMENT");
}
