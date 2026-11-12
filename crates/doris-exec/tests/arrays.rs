//! ARRAY values: `ARRAY_LITERAL`, `split_by_string`, and their text in a result row, as the C++
//! BE gives them (see `docs/superpowers/research/2026-11-10-m6-s7b2-tablefn.md`).
use doris_exec::block::Block;
use doris_exec::eval::{check_read, eval_column, SlotMap};
use doris_exec::row_text::cell_text;
use doris_exec::value::ColType;
use doris_fixtures::build::{array_literal, array_of, binary, call, int, null, string, ty};
use doris_segment::writer::Datum;
use doris_thrift::gen::exprs::TExpr;
use doris_thrift::gen::types::TPrimitiveType as T;

fn one_row(e: TExpr) -> Datum {
    let block = Block {
        types: vec![],
        rows: vec![vec![]],
    };
    check_read(&e).expect("supported");
    let (_, v) = eval_column(&e, &block, &SlotMap::default()).expect("eval");
    v.into_iter().next().unwrap()
}

fn split(v: &str, sep: &str) -> Datum {
    one_row(call("split_by_string", array_of(ty(T::VARCHAR)), vec![string(v), string(sep)]))
}

fn texts(v: &[&str]) -> Datum {
    Datum::Array(v.iter().map(|s| Datum::Bytes(s.as_bytes().to_vec())).collect())
}

#[test]
fn split_keeps_empty_parts() {
    assert_eq!(split(",,,,", ",,"), texts(&["", "", ""]));
    assert_eq!(split("a,b,,c", ","), texts(&["a", "b", "", "c"]));
    assert_eq!(split(",a,", ","), texts(&["", "a", ""]));
}

#[test]
fn split_of_empty_string_is_empty_array() {
    assert_eq!(split("", ","), texts(&[]));
    let v = ty(T::VARCHAR);
    assert_eq!(
        one_row(call("split_by_string", array_of(v.clone()), vec![null(v.clone()), string(",")])),
        Datum::Null
    );
}

#[test]
fn split_by_empty_sep_is_chars() {
    assert_eq!(split("中a", ""), texts(&["中", "a"]));
}

#[test]
fn array_literal_holds_its_elements() {
    let t = ty(T::TINYINT);
    let e = array_literal(t.clone(), vec![int(1, t.clone()), int(2, t.clone()), null(t.clone())]);
    assert_eq!(one_row(e), Datum::Array(vec![Datum::Int(1), Datum::Int(2), Datum::Null]));
}

#[test]
fn array_prints_as_doris_does() {
    let t = ColType::from_type_desc("", &array_of(ty(T::VARCHAR)), true).expect("an array type");
    let row = Datum::Array(vec![Datum::Bytes(b"a".to_vec()), Datum::Null]);
    assert_eq!(cell_text(&row, &t), Some(br#"["a", null]"#.to_vec()));
    assert_eq!(cell_text(&texts(&[]), &t), Some(b"[]".to_vec()));
    assert_eq!(cell_text(&Datum::Null, &t), None);
    let ints = ColType::from_type_desc("", &array_of(ty(T::INT)), true).unwrap();
    assert_eq!(
        cell_text(&Datum::Array(vec![Datum::Int(1), Datum::Int(-2)]), &ints),
        Some(b"[1, -2]".to_vec())
    );
    // percentile_array's and retention's results, as recorded
    let doubles = ColType::from_type_desc("", &array_of(ty(T::DOUBLE)), true).unwrap();
    let v = Datum::Array(vec![Datum::Double(61.0), Datum::Double(66.30000000000001), Datum::Double(1e20)]);
    assert_eq!(cell_text(&v, &doubles), Some(b"[61, 66.30000000000001, 1e+20]".to_vec()));
    let bools = ColType::from_type_desc("", &array_of(ty(T::BOOLEAN)), true).unwrap();
    assert_eq!(
        cell_text(&Datum::Array(vec![Datum::Bool(true), Datum::Bool(false)]), &bools),
        Some(b"[1, 0]".to_vec())
    );
}

#[test]
fn an_array_compared_is_refused() {
    let a = || call("split_by_string", array_of(ty(T::VARCHAR)), vec![string("a,b"), string(",")]);
    let err = check_read(&binary("eq", a(), a())).unwrap_err().to_string();
    assert!(err.contains("an ARRAY value"), "{err}");
    // an array of a type the printer does not know is refused
    assert!(ColType::from_type_desc("", &array_of(ty(T::DATEV2)), true).is_some_and(|t| !t.supported_array()));
}

#[test]
fn an_array_after_a_nested_sibling_is_still_an_operand() {
    use doris_fixtures::build::compound;
    // eq(not(true), split(..)): the array's parent is eq, not the finished not(…)
    let b = ty(T::BOOLEAN);
    let not = compound("not", vec![doris_fixtures::build::boolean(true)]);
    let mut e = binary(
        "eq",
        not,
        call("split_by_string", array_of(ty(T::VARCHAR)), vec![string("a"), string(",")]),
    );
    e.nodes[0].type_ = b;
    assert!(check_read(&e).is_err());
}

/// The first SORT_NODE of a tree.
fn sort_of(t: &mut doris_exec::query::PlanTree) -> Option<&mut doris_thrift::gen::plan_nodes::TPlanNode> {
    if t.node.node_type == doris_thrift::gen::plan_nodes::TPlanNodeType::SORT_NODE {
        return Some(&mut t.node);
    }
    t.children.iter_mut().find_map(sort_of)
}

/// A sort keyed on an ARRAY is refused: no order of arrays is ported. The plan is the
/// `test_split_by_string` replay's, its sort key pointed at the array slot.
#[test]
fn an_array_sort_key_is_refused() {
    use doris_exec::query::{check_plan, plan_query};
    use doris_fixtures::replays;
    let r = replays::load("nereids_p0.sql_functions.string_functions.test_split_by_string.test_split_by_string");
    for q in r.queries() {
        let Ok(mut p) = plan_query(&q) else { continue };
        let Some(id) = p.desc.slots.iter().find(|(_, s)| s.ty.supported_array()).map(|(id, _)| *id) else {
            continue;
        };
        if !p.fragments.iter_mut().any(|f| sort_of(&mut f.root).is_some()) {
            continue;
        }
        check_plan(&p).expect("the replay's plan is accepted");
        let sort = p.fragments.iter_mut().find_map(|f| sort_of(&mut f.root)).unwrap();
        sort.sort_node.as_mut().unwrap().sort_info.ordering_exprs[0] = doris_fixtures::build::slot(id, array_of(ty(T::VARCHAR)));
        let err = check_plan(&p).unwrap_err().to_string();
        assert!(err.contains("an ARRAY"), "{err}");
        return;
    }
    panic!("no plan with an array slot and a sort");
}
