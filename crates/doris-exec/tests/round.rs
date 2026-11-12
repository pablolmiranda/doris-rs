//! `round` as the C++ BE computes it (be/src/exprs/function/round.h).
use doris_exec::round::{round_decimal, round_double};

#[test]
fn round_double_matches_cpp() {
    assert_eq!(round_double(2.675, 2), 2.68);
    assert_eq!(round_double(1.005, 2), 1.0);
    assert_eq!(round_double(0.285, 2), 0.28);
    assert_eq!(round_double(1.15, 1), 1.2);
    assert_eq!(round_double(-0.5, 0), -1.0);
    assert_eq!(round_double(-2.5, 0), -3.0);
    assert_eq!(round_double(12345.6789, -2), 12300.0);
    assert_eq!(round_double(1e300, 20), f64::INFINITY);
    // 10^d saturates at u64::MAX past 19: x / 1.8446744073709552e19 rounds to 0
    assert_eq!(round_double(12345.0, -20), 0.0);
}

#[test]
fn round_decimal_half_away() {
    // (38,3) to 2 digits: result scale 2
    assert_eq!(round_decimal(-1235, 3, 2, 2), -124);
    assert_eq!(round_decimal(1234, 3, 2, 2), 123);
    assert_eq!(round_decimal(1235, 3, 2, 2), 124);
    // (33,25) to 0 digits: 0.5 -> 1
    assert_eq!(round_decimal(5 * 10i128.pow(24), 25, 0, 0), 1);
    // negative digits: result scale 0, rounded to hundreds
    assert_eq!(round_decimal(12_350, 0, -2, 0), 12_400);
    assert_eq!(round_decimal(-12_349, 0, -2, 0), -12_300);
    // more digits than the scale: unchanged
    assert_eq!(round_decimal(1234, 3, 5, 3), 1234);
    // a divisor past i128 rounds everything to 0
    assert_eq!(round_decimal(10i128.pow(37), 0, -39, 0), 0);
}

// floor and truncate: the same machinery with another rounding mode (round.h:111-123); see
// `docs/superpowers/research/2026-11-10-m6-s7b3-math.md`.
mod modes {
    use doris_exec::block::Block;
    use doris_exec::eval::{check_read, eval_column, SlotMap};
    use doris_exec::value::ColType;
    use doris_exec::ExecError;
    use doris_fixtures::build::{call, decimal, float, int, scalar, slot, ty};
    use doris_segment::writer::Datum;
    use doris_thrift::gen::exprs::TExpr;
    use doris_thrift::gen::types::{TPrimitiveType as T, TTypeDesc};

    fn eval(e: TExpr, scales: &[i128]) -> Result<Vec<Datum>, ExecError> {
        let block = Block {
            types: vec![ColType::from_type_desc("number", &ty(T::INT), true).unwrap()],
            rows: scales.iter().map(|s| vec![Datum::Int(*s)]).collect(),
        };
        check_read(&e)?;
        let (_, v) = eval_column(&e, &block, &SlotMap([(1, 0)].into_iter().collect()))?;
        Ok(v)
    }

    fn dec(p: i32, s: i32) -> TTypeDesc {
        scalar(T::DECIMAL128I, -1, p, s)
    }

    #[test]
    fn floor_decimal_to_scale_zero() {
        let f = |v: &str| eval(call("floor", dec(22, 0), vec![decimal(v, dec(22, 1))]), &[0]).unwrap();
        assert_eq!(f("1.5"), vec![Datum::Int(1)]);
        assert_eq!(f("-1.5"), vec![Datum::Int(-2)]);
        assert_eq!(f("-2.0"), vec![Datum::Int(-2)]);
        let d = eval(call("floor", ty(T::DOUBLE), vec![float(-1.5, ty(T::DOUBLE))]), &[0]).unwrap();
        assert_eq!(d, vec![Datum::Double(-2.0)]);
    }

    #[test]
    fn truncate_double_and_decimal() {
        let t = |x: f64, d: i64| eval(call("truncate", ty(T::DOUBLE), vec![float(x, ty(T::DOUBLE)), int(d, ty(T::INT))]), &[0]).unwrap()[0].clone();
        assert_eq!(t(1.99, 1), Datum::Double(1.9));
        assert_eq!(t(-1.99, 0), Datum::Double(-1.0));
        assert_eq!(t(155.0, -1), Datum::Double(150.0));
        let d32 = |p, s| scalar(T::DECIMAL32, -1, p, s);
        let e = call("truncate", d32(6, 0), vec![decimal("123.456", d32(6, 3)), int(-1, ty(T::INT))]);
        assert_eq!(eval(e, &[0]).unwrap(), vec![Datum::Int(120)]);
        assert!(matches!(
            check_read(&call("truncate", ty(T::DOUBLE), vec![float(1.0, ty(T::DOUBLE)), int(40000, ty(T::INT))])),
            Err(ExecError::Coded { msg, .. }) if msg == "Scale argument for function truncate is out of bound: 40000"
        ));
    }

    #[test]
    fn truncate_column_scale_copies_when_wider() {
        let t = dec(18, 9);
        let e = call(
            "truncate",
            t.clone(),
            vec![decimal("123456789.123456789", t.clone()), slot(1, ty(T::INT))],
        );
        assert_eq!(
            eval(e, &[1, -1, 12]).unwrap(),
            vec![
                Datum::Int(123456789_100000000),
                Datum::Int(123456780_000000000),
                Datum::Int(123456789_123456789)
            ]
        );
        let e = call("truncate", ty(T::DOUBLE), vec![float(12.345, ty(T::DOUBLE)), slot(1, ty(T::INT))]);
        assert_eq!(eval(e, &[1, -1]).unwrap(), vec![Datum::Double(12.3), Datum::Double(10.0)]);
    }

    #[test]
    fn truncate_column_scale_out_of_bound_errors() {
        let e = call("truncate", ty(T::DOUBLE), vec![float(1.0, ty(T::DOUBLE)), slot(1, ty(T::INT))]);
        match eval(e, &[1, 40000]) {
            Err(ExecError::Coded { code, name, msg }) => {
                assert_eq!((code, name), (-218, "E-218"));
                assert_eq!(msg, "Scale argument for function is out of bound: 40000");
            }
            other => panic!("{other:?}"),
        }
    }
}
