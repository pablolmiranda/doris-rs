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
