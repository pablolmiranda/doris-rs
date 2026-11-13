//! Converting expression values to stored values.
use doris_exec::value::{to_datum, ColType, Value};
use doris_thrift::gen::types::TPrimitiveType;

#[test]
fn an_integer_too_wide_for_its_decimal_is_a_row_error() {
    // 10^20 at scale 30 needs 10^50: past i128, so the row is filtered, not wrapped or panicking
    let col = ColType { name: "d".into(), ptype: TPrimitiveType::DECIMAL128I, len: 16, precision: 38, scale: 30, nullable: true, ..ColType::default() };
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
    doris_exec::value::ColType { name: "d".into(), ptype, len: -1, precision: 0, scale: 0, nullable: true, ..Default::default() }
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

fn typed(ptype: TPrimitiveType, precision: i32, scale: i32) -> ColType {
    ColType { name: "c".into(), ptype, len: -1, precision, scale, nullable: true, ..ColType::default() }
}

/// `StringParser::string_to_int_internal` without strict casting: digits, then nothing or a
/// `.digits` suffix (`is_float_suffix`), which is dropped; anything else, or overflow, fails.
#[test]
fn a_string_with_a_fraction_casts_to_its_integer_part() {
    use doris_exec::value::parse_text;
    for t in [TPrimitiveType::TINYINT, TPrimitiveType::SMALLINT, TPrimitiveType::INT, TPrimitiveType::BIGINT, TPrimitiveType::LARGEINT] {
        let c = typed(t, 0, 0);
        assert_eq!(parse_text("3.12", &c), Some(Value::Int(3)), "{t:?}");
        assert_eq!(parse_text("3.", &c), Some(Value::Int(3)), "{t:?}");
        assert_eq!(parse_text("-3.9", &c), Some(Value::Int(-3)), "{t:?}");
        for bad in ["3.1x", "x3", ".5", "2e+128", "3..1", "+"] {
            assert_eq!(parse_text(bad, &c), None, "{t:?} {bad}");
        }
    }
    assert_eq!(parse_text("2147483648", &typed(TPrimitiveType::INT, 0, 0)), None);
    assert_eq!(parse_text("2147483647.9", &typed(TPrimitiveType::INT, 0, 0)), Some(Value::Int(2147483647)));
}

/// `CastToFloat::from_string`: a value past the type's range is an infinity, not NULL.
#[test]
fn a_float_that_overflows_is_infinity() {
    use doris_exec::value::parse_text;
    use doris_segment::writer::Datum;
    let (f, d) = (typed(TPrimitiveType::FLOAT, 0, 0), typed(TPrimitiveType::DOUBLE, 0, 0));
    assert_eq!(to_datum(&parse_text("1e400", &d).unwrap(), &d, true).unwrap(), Datum::Double(f64::INFINITY));
    assert_eq!(to_datum(&parse_text("-1e400", &d).unwrap(), &d, true).unwrap(), Datum::Double(f64::NEG_INFINITY));
    assert_eq!(to_datum(&parse_text("1e40", &f).unwrap(), &f, true).unwrap(), Datum::Float(f32::INFINITY));
}

#[test]
fn dates_normalise_and_bad_dates_fail() {
    use doris_exec::value::parse_text;
    use doris_segment::writer::Datum;
    let date = typed(TPrimitiveType::DATEV2, 0, 0);
    let dt = typed(TPrimitiveType::DATETIMEV2, 0, 0);
    let v = parse_text("2024-4-2", &date).unwrap();
    assert_eq!(to_datum(&v, &date, true).unwrap(), Datum::Int(2024 << 9 | 4 << 5 | 2));
    for bad in ["2024-14-2", "2024-4-222"] {
        assert_eq!(parse_text(bad, &date), None, "{bad}");
    }
    assert_eq!(parse_text("2024-4-2 27:00:00", &dt), None);
}

/// A decimal cast to a string prints its value at the decimal's scale.
#[test]
fn a_decimal_cast_to_a_string_is_at_full_scale() {
    use doris_exec::expr::cast;
    let dec = typed(TPrimitiveType::DECIMAL64, 10, 4);
    let s = typed(TPrimitiveType::VARCHAR, 0, 0);
    assert_eq!(cast(cast(Value::Str(b"1234.5".to_vec()), &dec), &s), Value::Str(b"1234.5000".to_vec()));
    assert_eq!(cast(cast(Value::Str(b"-0.5".to_vec()), &dec), &s), Value::Str(b"-0.5000".to_vec()));
    let int_dec = typed(TPrimitiveType::DECIMAL64, 10, 0);
    assert_eq!(cast(cast(Value::Str(b"1234567890".to_vec()), &int_dec), &s), Value::Str(b"1234567890".to_vec()));
}

/// `concat`: the arguments' strings joined; any NULL argument makes it NULL.
#[test]
fn concat_joins_its_arguments_and_null_wins() {
    use doris_exec::expr::concat;
    let s = |t: &str| Value::Str(t.as_bytes().to_vec());
    assert_eq!(concat(&[s("invalid"), s("1234567890")]), s("invalid1234567890"));
    assert_eq!(concat(&[s("a"), s(""), s("b")]), s("ab"));
    assert_eq!(concat(&[s("a"), Value::Null]), Value::Null);
    assert_eq!(concat(&[]), s(""));
}

/// `cast_to_date_or_datetime_impl.hpp`: a v1 DATETIME keeps whole seconds; its fraction is
/// dropped, not rounded, but for a 7th digit that carries the microseconds to a second.
#[test]
fn a_v1_datetime_drops_its_fraction() {
    use doris_segment::writer::Datum;
    let c = typed(TPrimitiveType::DATETIME, 0, 0);
    let at = |y: i128, mo: i128, d: i128, h: i128, mi: i128, s: i128| Datum::Int((y << 9 | mo << 5 | d) << 37 | h << 32 | mi << 26 | s << 20);
    let dt = |t: &str| to_datum(&doris_exec::value::parse_text(t, &c).unwrap(), &c, true).unwrap();
    assert_eq!(dt("2023-01-01 12:00:00.4"), at(2023, 1, 1, 12, 0, 0));
    assert_eq!(dt("2023-01-01 12:00:00.5"), at(2023, 1, 1, 12, 0, 0));
    assert_eq!(dt("2023-01-01 12:00:00.999999"), at(2023, 1, 1, 12, 0, 0));
    assert_eq!(dt("2023-01-01 12:00:00.9999995"), at(2023, 1, 1, 12, 0, 1));
    assert_eq!(dt("2023-01-01 23:59:59.9999995"), at(2023, 1, 2, 0, 0, 0));
    assert_eq!(to_datum(&Value::Date("2023-01-01 12:00:00.7".into()), &c, true).unwrap(), at(2023, 1, 1, 12, 0, 0));
}

/// `StringParser` skips ASCII whitespace only: a Unicode space does not cast.
#[test]
fn only_ascii_whitespace_is_trimmed() {
    use doris_exec::value::parse_text;
    let int = typed(TPrimitiveType::INT, 0, 0);
    assert_eq!(parse_text(" \t12\n", &int), Some(Value::Int(12)));
    assert_eq!(parse_text("\u{3000}12", &int), None);
    assert_eq!(parse_text("12\u{a0}", &int), None);
}

/// A float cast to a string prints as the C++ BE's fmt does (`1e-07`, not `0.0000001`).
#[test]
fn a_float_cast_to_a_string_is_cpp_text() {
    use doris_exec::expr::cast;
    let s = typed(TPrimitiveType::VARCHAR, 0, 0);
    assert_eq!(cast(Value::Float(1e-7), &s), Value::Str(b"1e-07".to_vec()));
    assert_eq!(cast(Value::Float(1.5), &s), Value::Str(b"1.5".to_vec()));
    assert_eq!(cast(Value::Float(1e16), &s), Value::Str(b"1e+16".to_vec()));
    assert_eq!(cast(Value::Float(f64::INFINITY), &s), Value::Str(b"Infinity".to_vec()));
}
