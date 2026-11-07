//! Converting expression values to stored values.
use doris_exec::value::{to_datum, ColType, Value};
use doris_thrift::gen::types::TPrimitiveType;

#[test]
fn an_integer_too_wide_for_its_decimal_is_a_row_error() {
    // 10^20 at scale 30 needs 10^50: past i128, so the row is filtered, not wrapped or panicking
    let col = ColType { name: "d".into(), ptype: TPrimitiveType::DECIMAL128I, len: 16, precision: 38, scale: 30, nullable: true };
    assert!(to_datum(&Value::Int(10i128.pow(20)), &col, false).is_err());
}

#[test]
fn year_zero_is_not_a_leap_year() {
    // util/time_lut.h is_leap: MySQL's calendar, where 0000-02-29 does not exist
    assert_eq!(doris_exec::value::parse_datev2("0000-02-29"), None);
    assert!(doris_exec::value::parse_datev2("0400-02-29").is_some());
    assert!(doris_exec::value::parse_datev2("2000-02-29").is_some());
    assert_eq!(doris_exec::value::parse_datetimev2("0000-02-29 12:12:12", 0), None);
}
