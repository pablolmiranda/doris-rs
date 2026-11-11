//! Integer arithmetic and `abs` on the write path (`expr::eval`): a stream load into a table with
//! a generated column `c double as (abs(a + b))` sends `CAST(abs(CAST(a AS BIGINT) +
//! CAST(b AS BIGINT)) AS DOUBLE)` for `c`.
use doris_exec::expr::{check, eval, Slots};
use doris_exec::value::Value;
use doris_fixtures::build::{call, int, ty};
use doris_thrift::gen::exprs::{TExpr, TExprNodeType};
use doris_thrift::gen::types::TPrimitiveType as T;

fn arith(op: &str, t: T, l: TExpr, r: TExpr) -> TExpr {
    let mut e = call(op, ty(t), vec![l, r]);
    e.nodes[0].node_type = TExprNodeType::ARITHMETIC_EXPR;
    e
}

#[test]
fn abs_of_a_sum_on_the_write_path() {
    let sum = arith("add", T::BIGINT, int(-11, ty(T::BIGINT)), int(-21, ty(T::BIGINT)));
    let e = call("abs", ty(T::LARGEINT), vec![sum]);
    check(&e).unwrap();
    assert_eq!(eval(&e, &Slots::new()).unwrap(), Value::Int(32));
    let m = arith("multiply", T::INT, int(6, ty(T::INT)), int(7, ty(T::INT)));
    check(&m).unwrap();
    assert_eq!(eval(&m, &Slots::new()).unwrap(), Value::Int(42));
    // NULL in, NULL out
    let n = arith("subtract", T::BIGINT, doris_fixtures::build::null(ty(T::BIGINT)), int(1, ty(T::BIGINT)));
    assert_eq!(eval(&n, &Slots::new()).unwrap(), Value::Null);
}

#[test]
fn double_arithmetic_stays_refused_on_the_write_path() {
    let e = arith(
        "add",
        T::DOUBLE,
        doris_fixtures::build::float(1.5, ty(T::DOUBLE)),
        doris_fixtures::build::float(2.0, ty(T::DOUBLE)),
    );
    assert!(check(&e).is_err());
}
