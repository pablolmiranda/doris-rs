//! `convert_tz` as the C++ BE computes it (`function_convert_tz.cpp`): see
//! `docs/superpowers/research/2026-11-11-m6-s7b5-dates.md` §10.
use doris_exec::block::Block;
use doris_exec::eval::{check_read, eval_column, SlotMap};
use doris_exec::value::{parse_datetimev2, ColType};
use doris_exec::ExecError;
use doris_fixtures::build::{call, date, null, scalar, slot, string};
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

fn eval(e: TExpr) -> Result<Datum, ExecError> {
    check_read(&e)?;
    let block = Block {
        types: vec![],
        rows: vec![vec![]],
    };
    Ok(eval_column(&e, &block, &SlotMap::default())?.1.remove(0))
}

fn tz(v: &str, scale: i32, from: &str, to: &str) -> Result<Datum, ExecError> {
    eval(call("convert_tz", dt(scale), vec![date(v, dt(scale)), string(from), string(to)]))
}

#[test]
fn convert_tz_matches_reference() {
    // the recorded row: a time Paris skips is its transition, microseconds dropped
    assert_eq!(
        tz("2021-03-28 02:30:00.00323", 5, "Europe/Paris", "UTC").unwrap(),
        at("2021-03-28 01:00:00", 5)
    );
    // fixed offsets, microseconds carried
    assert_eq!(
        tz("2024-04-18 23:20:00.5", 1, "+00:00", "+08:00").unwrap(),
        at("2024-04-19 07:20:00.5", 1)
    );
    assert_eq!(tz("2024-04-18 23:20:00", 0, "+00:00", "+00:00").unwrap(), at("2024-04-18 23:20:00", 0));
    // a repeated time takes the earlier instant
    assert_eq!(tz("2021-10-31 02:30:00", 0, "Europe/Paris", "UTC").unwrap(), at("2021-10-31 00:30:00", 0));
    assert_eq!(
        tz("2021-07-01 12:00:00", 0, "UTC", "Asia/Shanghai").unwrap(),
        at("2021-07-01 20:00:00", 0)
    );
}

#[test]
fn convert_tz_errors() {
    // an unknown constant zone fails at open
    match tz("2021-01-01 00:00:00", 0, "Mars/Olympus", "UTC") {
        Err(ExecError::Coded { name, msg, .. }) => {
            assert_eq!(name, "INVALID_ARGUMENT");
            assert_eq!(msg, "Operation convert_tz invalid timezone: Mars/Olympus");
        }
        other => panic!("{other:?}"),
    }
    // a NULL constant zone: NULL rows
    let e = call("convert_tz", dt(0), vec![date("2021-01-01 00:00:00", dt(0)), null(vc()), string("UTC")]);
    assert_eq!(eval(e).unwrap(), Datum::Null);
    // past 9999
    match tz("9999-12-31 23:00:00", 0, "+00:00", "+08:00") {
        Err(ExecError::Coded { code: -218, .. }) => {}
        other => panic!("{other:?}"),
    }
}

/// chrono-tz stops applying daylight saving after 2099; cctz does not.
#[test]
fn convert_tz_after_2099_with_dst_is_refused() {
    assert!(matches!(
        tz("2100-07-01 12:00:00", 0, "America/New_York", "UTC"),
        Err(ExecError::Unsupported { .. })
    ));
    // a zone without daylight saving converts
    assert_eq!(
        tz("2100-07-01 12:00:00", 0, "Asia/Shanghai", "UTC").unwrap(),
        at("2100-07-01 04:00:00", 0)
    );
}

/// A column zone is resolved per row.
#[test]
fn convert_tz_over_a_column_zone() {
    let c = |t: TTypeDesc| ColType::from_type_desc("", &t, true).unwrap();
    let block = Block {
        types: vec![c(dt(0)), c(vc())],
        rows: vec![
            vec![at("2024-04-18 23:20:00", 0), Datum::Bytes(b"+00:00".to_vec())],
            vec![at("2024-04-18 23:20:00", 0), Datum::Null],
            vec![at("2024-04-18 23:20:00", 0), Datum::Bytes(b"Asia/Shanghai".to_vec())],
        ],
    };
    let slots = SlotMap([(1, 0), (2, 1)].into_iter().collect());
    let e = call("convert_tz", dt(0), vec![slot(1, dt(0)), string("+00:00"), slot(2, vc())]);
    check_read(&e).unwrap();
    let got = eval_column(&e, &block, &slots).unwrap().1;
    assert_eq!(got, vec![at("2024-04-18 23:20:00", 0), Datum::Null, at("2024-04-19 07:20:00", 0)]);
}

// review fixes

/// `open` checks for a NULL constant zone before looking any up; it looks constant zones up
/// only when both are constant.
#[test]
fn constant_zones_are_checked_as_cpp_opens_them() {
    let e = call(
        "convert_tz",
        dt(0),
        vec![date("2021-01-01 00:00:00", dt(0)), string("Mars/Olympus"), null(vc())],
    );
    assert_eq!(eval(e).unwrap(), Datum::Null);
    // one constant zone, every row NULL: no lookup, no error
    let c = |t: TTypeDesc| ColType::from_type_desc("", &t, true).unwrap();
    let block = Block {
        types: vec![c(dt(0)), c(vc())],
        rows: vec![vec![Datum::Null, Datum::Bytes(b"UTC".to_vec())]],
    };
    let slots = SlotMap([(1, 0), (2, 1)].into_iter().collect());
    let e = call("convert_tz", dt(0), vec![slot(1, dt(0)), string("Mars/Olympus"), slot(2, vc())]);
    assert_eq!(eval_column(&e, &block, &slots).unwrap().1, vec![Datum::Null]);
}
