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

#[test]
fn datetime_text_rounds_to_the_scale() {
    // cast_to_datetimev2_impl.hpp init_microsecond: the next digit rounds half up, carrying
    use doris_exec::value::parse_datetimev2 as p;
    assert_eq!(p("2022-12-01 22:23:24.999999", 3), p("2022-12-01 22:23:25.000", 3));
    assert_eq!(p("2022-12-31 23:59:59.9996", 3), p("2023-01-01 00:00:00", 3));
    assert_eq!(p("2022-12-01 22:23:24.1234", 3), p("2022-12-01 22:23:24.123", 3));
    assert_eq!(p("2022-12-01 22:23:24.5", 0), p("2022-12-01 22:23:25", 0));
    assert_eq!(p("9999-12-31 23:59:59.9", 0), None);
}

fn v1(ptype: doris_thrift::gen::types::TPrimitiveType) -> doris_exec::value::ColType {
    doris_exec::value::ColType { name: "d".into(), ptype, len: -1, precision: 0, scale: 0, nullable: true }
}

/// v1 DATE and DATETIME are written (v2-packed in memory, v1 on disk) but not read.
#[test]
fn v1_dates_are_writable_but_not_readable() {
    use doris_thrift::gen::types::TPrimitiveType as T;
    for t in [T::DATE, T::DATETIME] {
        assert!(v1(t).writable(), "{t:?}");
        assert!(!v1(t).supported(), "{t:?}");
    }
    assert_eq!(v1(T::DATE).type_name(), "Date");
    assert_eq!(v1(T::DATETIME).type_name(), "DateTime");
}

#[test]
fn v1_date_values_convert_as_v2_packed() {
    use doris_exec::value::{parse_text, to_datum, Value};
    use doris_segment::writer::Datum;
    use doris_thrift::gen::types::TPrimitiveType as T;
    let date = 2023i128 << 9 | 6 << 5 | 1;
    let datetime = date << 37 | 12 << 32 | 30 << 26 | 5 << 20;
    assert_eq!(to_datum(&Value::Date("2023-06-01".into()), &v1(T::DATE), true).unwrap(), Datum::Int(date));
    assert_eq!(to_datum(&Value::Date("2023-06-01 12:30:05".into()), &v1(T::DATETIME), true).unwrap(), Datum::Int(datetime));
    assert_eq!(parse_text("2023-06-01", &v1(T::DATE)), Some(Value::Date("2023-06-01".into())));
    assert_eq!(parse_text("2023-06-01 12:30:05", &v1(T::DATETIME)), Some(Value::Date("2023-06-01 12:30:05".into())));
    assert_eq!(parse_text("2023-13-01", &v1(T::DATE)), None);
}
