//! A stream load's rows: the error a row is filtered with.
use doris_exec::file_scan::stream_rows;
use doris_exec::fragment::plan_write;
use doris_fixtures::stream_loads::stream_loads;
use doris_thrift::gen::palo_internal_service::TPipelineFragmentParams;

/// test_stream_load_strict_mode's load of one BIGINT NOT NULL column k00, cast straight from the
/// file's column, in strict mode; with `before`, a second NOT NULL column k99 first in the tuple,
/// computed by the same cast but not mapped straight (so a NULL there is a not-nullable error).
fn strict_load(before: bool) -> TPipelineFragmentParams {
    let s = stream_loads()
        .into_iter()
        .find(|s| s.suite.ends_with("test_stream_load_strict_mode_and_filter_ratio") && s.reply.contains("column(k00) values is null while columns is not nullable"))
        .expect("the sample");
    let mut params = s.put.1.pipeline_params.clone().unwrap();
    let mut strict = |p: &mut doris_thrift::gen::plan_nodes::TFileScanRangeParams| {
        p.strict_mode = Some(true);
        if before {
            let e = p.expr_of_dest_slot.as_ref().unwrap()[&0].clone();
            p.expr_of_dest_slot.as_mut().unwrap().insert(99, e);
        }
    };
    params.file_scan_params.iter_mut().flat_map(|m| m.values_mut()).for_each(&mut strict);
    for l in params.local_params.iter_mut().flatten() {
        for r in l.per_node_scan_ranges.values_mut().flatten() {
            if let Some(p) = r.scan_range.ext_scan_range.as_mut().and_then(|e| e.file_scan_range.as_mut()).and_then(|f| f.params.as_mut()) {
                strict(p);
            }
        }
    }
    if before {
        let slots = params.desc_tbl.as_mut().unwrap().slot_descriptors.as_mut().unwrap();
        let mut k99 = slots.iter().find(|s| s.id == 0).unwrap().clone();
        k99.id = 99;
        k99.col_name = "k99".into();
        slots.insert(0, k99);
    }
    params
}

fn errors(params: &TPipelineFragmentParams, body: &[u8]) -> Vec<String> {
    let f = plan_write(params).unwrap();
    stream_rows(&f, body, u64::MAX).unwrap().iter().filter_map(|r| r.as_ref().err()).map(|e| e.msg.clone()).collect()
}

/// `FileScanner::_convert_to_output_block` checks the destination columns in tuple order: the
/// first that came out NULL decides the row's error, the strict-mode one when it is mapped
/// straight from a source column with a value, else the not-nullable one.
#[test]
fn the_first_failing_column_names_the_error() {
    assert_eq!(errors(&strict_load(false), b"abc\n"), vec!["column(k00) value is incorrect while strict mode is true, src value is abc".to_string()]);
    assert_eq!(errors(&strict_load(true), b"abc\n"), vec!["column(k99) values is null while columns is not nullable".to_string()]);
}

/// test_json_load_default_value's first load (simple_json.json: no country nor date_time key),
/// its plan as captured; with `without_now`, date_time's default (CURRENT_TIMESTAMP, planned as
/// now()) is the NULL literal the FE sends for a column with no default.
fn json_default_load(without_now: bool) -> (TPipelineFragmentParams, Vec<u8>) {
    use doris_thrift::gen::exprs::{TExpr, TExprNode};
    use doris_thrift::gen::exprs::TExprNodeType;
    let s = stream_loads().into_iter().find(|s| s.suite.ends_with("test_json_load_default_value")).expect("the sample");
    let mut params = s.put.1.pipeline_params.clone().unwrap();
    if without_now {
        let null = |defaults: &mut std::collections::BTreeMap<i32, TExpr>| {
            for e in defaults.values_mut() {
                if e.nodes.iter().any(|n| n.fn_.as_ref().is_some_and(|f| f.name.function_name == "now")) {
                    let mut n: TExprNode = e.nodes[0].clone();
                    n.node_type = TExprNodeType::NULL_LITERAL;
                    n.num_children = 0;
                    n.fn_ = None;
                    *e = TExpr::new(vec![n]);
                }
            }
        };
        for l in params.local_params.iter_mut().flatten() {
            for r in l.per_node_scan_ranges.values_mut().flatten() {
                if let Some(d) = r.scan_range.ext_scan_range.as_mut().and_then(|e| e.file_scan_range.as_mut()).and_then(|f| f.params.as_mut()).and_then(|p| p.default_value_of_src_slot.as_mut()) {
                    null(d);
                }
            }
        }
        params.file_scan_params.iter_mut().flat_map(|m| m.values_mut()).filter_map(|p| p.default_value_of_src_slot.as_mut()).for_each(null);
    }
    (params, s.body)
}

/// NewJsonReader::_fill_missing_column: a missing key reads as its source slot's default
/// (default_value_of_src_slot) — the .out's `1 default_country beijing 2345671`.
#[test]
fn a_missing_json_key_takes_its_column_default() {
    let (params, body) = json_default_load(true);
    let f = plan_write(&params).unwrap();
    let rows = stream_rows(&f, &body, u64::MAX).unwrap();
    let text: Vec<String> = rows[0]
        .as_ref()
        .unwrap()
        .iter()
        .map(|v| match v {
            doris_exec::value::Value::Str(b) => String::from_utf8_lossy(b).into_owned(),
            other => format!("{other:?}"),
        })
        .collect();
    assert_eq!(text[..4], ["Int(1)", "default_country", "beijing", "Int(2345671)"], "{text:?}");
}

/// A default doris-rs cannot compute refuses the load rather than storing NULL.
#[test]
fn a_missing_json_key_whose_default_is_not_supported_refuses_the_load() {
    let (params, body) = json_default_load(false);
    let f = plan_write(&params).unwrap();
    let err = stream_rows(&f, &body, u64::MAX).unwrap_err();
    assert!(err.to_string().contains("now"), "{err}");
}

/// _get_column_default_value keeps the default's text (get_data_at(0).to_string()): a default
/// that is not the NULL literal but evaluates to NULL is the empty string. Here country's
/// default becomes CAST(NULL AS VARCHAR), built from id's CAST(0 AS VARCHAR).
#[test]
fn a_default_that_evaluates_to_null_is_the_empty_string() {
    use doris_thrift::gen::exprs::TExprNodeType;
    let (mut params, body) = json_default_load(true);
    let null_cast = |d: &mut std::collections::BTreeMap<i32, doris_thrift::gen::exprs::TExpr>| {
        let mut cast = d.values().find(|e| e.nodes.len() == 2 && e.nodes[1].node_type == TExprNodeType::INT_LITERAL).unwrap().clone();
        cast.nodes[1].node_type = TExprNodeType::NULL_LITERAL;
        cast.nodes[1].int_literal = None;
        let country = *d.iter().find(|(_, e)| e.nodes[0].string_literal.as_ref().is_some_and(|l| l.value == "default_country")).unwrap().0;
        d.insert(country, cast);
    };
    for l in params.local_params.iter_mut().flatten() {
        for r in l.per_node_scan_ranges.values_mut().flatten() {
            if let Some(d) = r.scan_range.ext_scan_range.as_mut().and_then(|e| e.file_scan_range.as_mut()).and_then(|f| f.params.as_mut()).and_then(|p| p.default_value_of_src_slot.as_mut()) {
                null_cast(d);
            }
        }
    }
    params.file_scan_params.iter_mut().flat_map(|m| m.values_mut()).filter_map(|p| p.default_value_of_src_slot.as_mut()).for_each(null_cast);
    let f = plan_write(&params).unwrap();
    let rows = stream_rows(&f, &body, u64::MAX).unwrap();
    assert_eq!(rows[0].as_ref().unwrap()[1], doris_exec::value::Value::Str(Vec::new()), "{:?}", rows[0]);
}
