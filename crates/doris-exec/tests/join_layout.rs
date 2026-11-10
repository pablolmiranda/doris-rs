//! Where a joined row goes in a join's intermediate tuple: chosen by the join op, as C++ sets
//! `_right_col_idx` (hashjoin_probe_operator.cpp:618-621), not guessed from slot counts.
use doris_exec::hash_join::JoinOp;
use doris_exec::pipeline::Layout;
use doris_segment::writer::Datum;

fn row() -> Vec<Datum> {
    vec![Datum::Int(1), Datum::Int(2)]
}

#[test]
fn right_semi_and_anti_keep_the_build_side_at_equal_widths() {
    for op in [JoinOp::RightSemi, JoinOp::RightAnti] {
        // one probe and one build column, an intermediate of one slot: the build's
        let l = Layout::new(1, 1, 1, false, Layout::sides(op, false, false, false)).unwrap();
        assert_eq!(l.apply(&row()), [Datum::Int(2)], "{op:?}");
        // with other conjuncts the intermediate holds both sides
        let l = Layout::new(2, 1, 1, false, Layout::sides(op, true, false, false)).unwrap();
        assert_eq!(l.apply(&row()), row(), "{op:?}");
    }
}

#[test]
fn left_semi_keeps_the_probe_side() {
    let l = Layout::new(1, 1, 1, false, Layout::sides(JoinOp::LeftSemi, false, false, false)).unwrap();
    assert_eq!(l.apply(&row()), [Datum::Int(1)]);
    // a mark join keeps its mark last
    let l = Layout::new(2, 1, 1, true, Layout::sides(JoinOp::LeftAnti, false, true, false)).unwrap();
    assert_eq!(
        l.apply(&[Datum::Int(1), Datum::Int(2), Datum::Bool(true)]),
        [Datum::Int(1), Datum::Bool(true)]
    );
}

#[test]
fn a_tuple_that_fits_no_side_is_refused() {
    // a right semi join whose intermediate is not its build side
    assert!(Layout::new(3, 1, 1, false, Layout::sides(JoinOp::RightSemi, false, false, false)).is_err());
    // an inner join must carry both sides
    assert!(Layout::new(1, 1, 1, false, Layout::sides(JoinOp::Inner, false, false, false)).is_err());
}

#[test]
fn asof_sides_must_share_a_type() {
    use doris_exec::hash_join::check_asof_condition;
    use doris_fixtures::build::{binary, slot, ty};
    use doris_thrift::gen::types::TPrimitiveType as T;
    let date = || slot(1, ty(T::DATEV2));
    let datetime = || slot(2, ty(T::DATETIMEV2));
    check_asof_condition(&binary("ge", date(), slot(3, ty(T::DATEV2)))).unwrap();
    check_asof_condition(&binary("lt", datetime(), slot(4, ty(T::DATETIMEV2)))).unwrap();
    // packed DATEV2 and DATETIMEV2 do not compare as integers
    let err = check_asof_condition(&binary("ge", date(), datetime())).unwrap_err().to_string();
    assert!(err.contains("ASOF"), "{err}");
    assert!(check_asof_condition(&binary("ge", slot(1, ty(T::INT)), slot(2, ty(T::INT)))).is_err());
    assert!(check_asof_condition(&binary("eq", date(), slot(3, ty(T::DATEV2)))).is_err());
}
