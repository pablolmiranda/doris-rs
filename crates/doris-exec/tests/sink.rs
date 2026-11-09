//! The table sink: the report it gives the FE, the tablets it commits, the rows it filters.
use std::sync::Mutex;

use doris_exec::fragment::{plan_write, WriteFragment};
use doris_exec::sink::{exec_status, run_sink};
use doris_exec::value::Value;
use doris_fixtures::values::{values_vectors, ValuesVector};
use doris_storage::{DataDir, TabletManager, TabletOptions};
use doris_thrift::gen::frontend_service::TReportExecStatusParams;
use doris_thrift::gen::status::TStatusCode;
use doris_thrift::gen::types::TPrimitiveType;

/// A tablet manager holding the vector's tablets; None when its tables use types M4 lacks.
fn manager(v: &ValuesVector, root: &std::path::Path) -> Option<Mutex<TabletManager>> {
    let mut mgr = TabletManager::open(vec![DataDir::open(root, "127.0.0.1").unwrap()], TabletOptions::default()).unwrap();
    for req in v.create_requests() {
        mgr.create_tablet(&req).ok()?;
        doris_storage::rowset::segment_schema(mgr.tablet(req.tablet_id)?.schema.as_ref()?).ok()?;
    }
    Some(Mutex::new(mgr))
}

/// Fields that depend on this run: ids and times.
fn mask(got: &mut TReportExecStatusParams, expected: &TReportExecStatusParams) {
    got.query_id = expected.query_id.clone();
    got.backend_id = expected.backend_id;
    got.tracking_url = expected.tracking_url.clone();
    for (g, e) in got.fragment_instance_reports.iter_mut().flatten().zip(expected.fragment_instance_reports.iter().flatten()) {
        g.fragment_instance_id = e.fragment_instance_id.clone();
    }
    if let Some(c) = got.commit_infos.as_mut() {
        c.sort_by_key(|i| i.tablet_id);
    }
}

fn run(v: &ValuesVector, f: &WriteFragment, mgr: &Mutex<TabletManager>, root: &std::path::Path) -> TReportExecStatusParams {
    let rows = f.values_rows().unwrap();
    let outcome = run_sink(f, rows, mgr, &root.join("error_log"));
    let url = outcome.error_log.as_ref().map(|_| "url".to_string());
    let _ = v;
    exec_status(f, &outcome, url)
}

#[test]
fn exec_status_matches_the_corpus() {
    let mut checked = 0;
    for v in values_vectors() {
        let Ok(f) = plan_write(&v.params()) else { continue };
        if f.values_rows().is_err() {
            continue;
        }
        let tmp = tempfile::tempdir().unwrap();
        let Some(mgr) = manager(&v, tmp.path()) else { continue };
        let mut got = run(&v, &f, &mgr, tmp.path());
        let mut expected = v.report.clone();
        if let Some(c) = expected.commit_infos.as_mut() {
            c.sort_by_key(|i| i.tablet_id);
        }
        mask(&mut got, &expected);
        assert_eq!(got, expected, "{} ({})", v.dir, v.suite);
        checked += 1;
    }
    assert!(checked >= 30, "{checked}");
}

#[test]
fn untouched_tablets_get_empty_rowsets() {
    // a statement whose rows reach fewer tablets than its partition has
    let v = values_vectors()
        .into_iter()
        .find(|v| {
            let deltas = v.finish.table_id_to_tablet_id_to_delta_num_rows.clone().unwrap_or_default().into_values().flatten().count();
            let commits = v.report.commit_infos.as_ref().map_or(0, Vec::len);
            deltas > 0 && commits > deltas && plan_write(&v.params()).is_ok_and(|f| f.values_rows().is_ok())
        })
        .expect("a vector with untouched tablets");
    let tmp = tempfile::tempdir().unwrap();
    let mgr = manager(&v, tmp.path()).expect("supported types");
    let f = plan_write(&v.params()).unwrap();
    let outcome = run_sink(&f, f.values_rows().unwrap(), &mgr, &tmp.path().join("error_log"));
    assert_eq!(outcome.commit_infos.len(), v.report.commit_infos.as_ref().unwrap().len());
    // every committed tablet holds the version after the publish
    let mut mgr = mgr.into_inner().unwrap();
    let publish = v.publish.publish_version_req.as_ref().unwrap();
    for p in &publish.partition_version_infos {
        let res = mgr.publish_txn(p.partition_id, publish.transaction_id, p.version);
        assert!(res.errors.is_empty(), "{res:?}");
    }
}

#[test]
fn filtered_rows_are_counted_and_logged() {
    // one vector with a NOT NULL column: a NULL there is filtered, the rest load
    let v = values_vectors()
        .into_iter()
        .find(|v| {
            plan_write(&v.params()).is_ok_and(|f| {
                f.values_rows().is_ok_and(|r| r.len() >= 2) && f.dest_columns().iter().any(|c| !c.nullable)
            })
        })
        .unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let mgr = manager(&v, tmp.path()).unwrap();
    let f = plan_write(&v.params()).unwrap();
    let col = f.dest_columns().iter().position(|c| !c.nullable).unwrap();
    let name = f.dest_columns()[col].name.clone();
    let mut rows = f.values_rows().unwrap();
    let total = rows.len() as i64;
    rows[0].as_mut().unwrap()[col] = Value::Null;
    let outcome = run_sink(&f, rows, &mgr, &tmp.path().join("error_log"));
    assert_eq!(outcome.status.status_code, TStatusCode::OK);
    assert_eq!((outcome.filtered, outcome.loaded), (1, total - 1));
    let first = outcome.first_error.clone().unwrap();
    assert!(first.starts_with(&format!("column_name[{name}], null value for not null column, type=")), "{first}");
    let log = std::fs::read_to_string(tmp.path().join("error_log").join(outcome.error_log.as_ref().unwrap())).unwrap();
    assert_eq!(log.lines().count(), 1);
    assert!(log.starts_with("Reason: column_name["), "{log}");
    let report = exec_status(&f, &outcome, Some("http://x/api/_load_error_log?file=y".into()));
    assert_eq!(report.load_counters.as_ref().unwrap()["dpp.abnorm.ALL"], "1");
    assert_eq!(report.first_error_msg.as_deref(), Some(first.as_str()));
}

#[test]
fn an_over_long_string_is_filtered() {
    // a VARCHAR(n) column gets n + 1 bytes
    let v = values_vectors()
        .into_iter()
        .find(|v| {
            plan_write(&v.params()).is_ok_and(|f| {
                f.values_rows().is_ok() && f.dest_columns().iter().any(|c| c.ptype == TPrimitiveType::VARCHAR && c.len < 1000)
            })
        })
        .unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let mgr = manager(&v, tmp.path()).unwrap();
    let f = plan_write(&v.params()).unwrap();
    let col = f.dest_columns().iter().position(|c| c.ptype == TPrimitiveType::VARCHAR && c.len < 1000).unwrap();
    let (name, len) = (f.dest_columns()[col].name.clone(), f.dest_columns()[col].len);
    let mut rows = f.values_rows().unwrap();
    rows[0].as_mut().unwrap()[col] = Value::Str(vec![b'a'; len as usize + 1]);
    let outcome = run_sink(&f, rows, &mgr, &tmp.path().join("error_log"));
    assert_eq!(outcome.filtered, 1);
    let shown: String = "a".repeat((len as usize + 1).min(32));
    assert_eq!(
        outcome.first_error.unwrap(),
        format!(
            "column_name[{name}], the length of input is too long than schema. first 32 bytes of input str: [{shown}] schema length: {len}; actual length: {}; . Src line: ",
            len + 1
        )
    );
}

/// test_coalesce_new's line-43 insert: three rows into a UNIQUE merge-on-write table with a
/// DATETIME v1 column `dt`. The suite sets `enable_date_conversion=false` before it and back
/// after it, so a refusal here leaves the FE creating v1 columns for every later suite.
#[test]
fn a_v1_datetime_insert_is_written() {
    let v = values_vectors().into_iter().find(|v| v.suite.ends_with("test_coalesce_new")).expect("the vector");
    let f = plan_write(&v.params()).expect("plans");
    let tmp = tempfile::tempdir().unwrap();
    let mgr = manager(&v, tmp.path()).expect("its tablets");
    let mut got = run(&v, &f, &mgr, tmp.path());
    let mut expected = v.report.clone();
    if let Some(c) = expected.commit_infos.as_mut() {
        c.sort_by_key(|i| i.tablet_id);
    }
    mask(&mut got, &expected);
    assert_eq!(got.status.as_ref().map(|s| s.status_code), Some(TStatusCode::OK));
    assert_eq!(got, expected);
}

/// `RawValue::zlib_crc32` hashes a v1 date as its text (`to_buffer`), not its bytes.
#[test]
fn v1_dates_hash_their_text() {
    let v = values_vectors().into_iter().find(|v| v.suite.ends_with("test_coalesce_new")).expect("the vector");
    let mut f = plan_write(&v.params()).expect("plans");
    f.sink.partition.distributed_columns = Some(vec!["dt".into()]);
    let part = f.sink.partition.partitions[0].clone();
    let buckets = part.num_buckets as u32;
    let router = doris_exec::route::Router::new(&f).unwrap();
    for row in f.values_rows().unwrap() {
        let row = row.unwrap();
        let Value::Date(text) = &row[2] else { panic!("{row:?}") };
        let want = part.indexes[0].tablets[(crc32fast::hash(text.as_bytes()) % buckets) as usize];
        let routed = router.route(&row).unwrap();
        assert_eq!(routed.tablets[0].1, want, "{text}");
    }
}

/// A vector of at least `n` rows that all load, with its tablets.
fn loading(n: usize) -> (ValuesVector, WriteFragment, Vec<Vec<Value>>) {
    for v in values_vectors() {
        let Ok(f) = plan_write(&v.params()) else { continue };
        let Ok(rows) = f.values_rows() else { continue };
        let Ok(rows) = rows.into_iter().collect::<Result<Vec<_>, _>>() else { continue };
        let tmp = tempfile::tempdir().unwrap();
        if rows.len() >= n && manager(&v, tmp.path()).is_some() && v.report.commit_infos.as_ref().is_some_and(|c| !c.is_empty()) {
            return (v, f, rows);
        }
    }
    panic!("no vector of {n} rows");
}

fn instance(n: i64) -> doris_thrift::gen::types::TUniqueId {
    doris_thrift::gen::types::TUniqueId { hi: 7, lo: n }
}

/// `VTabletWriterV2::close`: the last sink to close commits, once, every tablet of each touched
/// partition; the report sums the instances' counters and lists each instance that had rows.
#[test]
fn a_table_sink_fed_by_two_instances_commits_once() {
    use doris_exec::sink::TableSink;
    let (v, f, rows) = loading(3);
    let tmp = tempfile::tempdir().unwrap();
    let mgr = manager(&v, tmp.path()).unwrap();
    let nullable = f.input_nullable();
    let sink = TableSink::new(&f, vec![instance(1), instance(2), instance(3)]).unwrap();
    let (a, b) = rows.split_at(2);
    sink.push(0, a.iter().cloned().map(Ok).collect(), &nullable).unwrap();
    sink.push(2, b.iter().cloned().map(Ok).collect(), &nullable).unwrap();
    let outcome = sink.finish(&mgr, &tmp.path().join("error_log"), None);
    let report = exec_status(&f, &outcome, None);
    // the same commit as one instance writing every row
    let one = tempfile::tempdir().unwrap();
    let single = exec_status(&f, &run_sink(&f, rows.iter().cloned().map(Ok).collect::<Vec<_>>(), &manager(&v, one.path()).unwrap(), one.path()), None);
    let tablets = |r: &TReportExecStatusParams| {
        let mut t: Vec<i64> = r.commit_infos.iter().flatten().map(|c| c.tablet_id).collect();
        t.sort();
        t
    };
    assert_eq!(tablets(&report), tablets(&single));
    assert_eq!(report.load_counters.as_ref().unwrap()["dpp.norm.ALL"], rows.len().to_string());
    let reports = report.fragment_instance_reports.clone().unwrap_or_default();
    let per: Vec<(i64, i64)> = reports.iter().map(|r| (r.fragment_instance_id.as_ref().unwrap().lo, r.loaded_rows.unwrap())).collect();
    assert_eq!(per, vec![(1, 2), (3, rows.len() as i64 - 2)]);
    let bytes: i64 = reports.iter().map(|r| r.loaded_bytes.unwrap()).sum();
    assert_eq!(bytes, single.fragment_instance_reports.unwrap()[0].loaded_bytes.unwrap());
}

/// No row reached the sink: OK, and no counters, instance reports or commit infos, so the FE
/// commits an empty transaction (`_coordinator_callback` sets them only when there are rows).
#[test]
fn an_insert_select_of_no_rows_reports_no_counters() {
    use doris_exec::sink::TableSink;
    let (v, f, _) = loading(1);
    let tmp = tempfile::tempdir().unwrap();
    let mgr = manager(&v, tmp.path()).unwrap();
    let sink = TableSink::new(&f, vec![instance(1), instance(2)]).unwrap();
    sink.push(1, Vec::new(), &f.input_nullable()).unwrap();
    let report = exec_status(&f, &sink.finish(&mgr, &tmp.path().join("error_log"), None), None);
    assert_eq!(report.status.as_ref().map(|s| s.status_code), Some(TStatusCode::OK));
    assert_eq!(report.load_counters, None);
    assert_eq!(report.fragment_instance_reports, None);
    assert_eq!(report.commit_infos, None);
}

/// `RuntimeState::append_error_msg_to_file`: with `load_zero_tolerance`, the 52nd error row of an
/// instance (51 are logged) stops the load with DATA_QUALITY_ERROR. Nothing is committed; the
/// report carries the rows that reached the sink as loaded, none as filtered, the log and the
/// first error (fail_url's query 6: 96 rows, 64 with no partition).
#[test]
fn the_fifty_second_error_row_cancels_a_zero_tolerance_load() {
    use doris_exec::sink::TableSink;
    use doris_exec::{ExecError, RowError};
    let (v, mut f, rows) = loading(1);
    f.zero_tolerance = true;
    let tmp = tempfile::tempdir().unwrap();
    let mgr = manager(&v, tmp.path()).unwrap();
    let input: Vec<Result<Vec<Value>, RowError>> =
        (0..96).map(|i| if i % 3 == 0 { Ok(rows[0].clone()) } else { Err(RowError::new(format!("bad row {i}"))) }).collect();
    let sink = TableSink::new(&f, vec![instance(1)]).unwrap();
    let err = sink.push(0, input, &f.input_nullable()).unwrap_err();
    let ExecError::Cancelled(msg) = &err else { panic!("{err:?}") };
    let q = &f.query_id;
    assert_eq!(
        msg.as_str(),
        format!(
            "load_id={:x}-{:x}, reason: [DATA_QUALITY_ERROR]Encountered unqualified data, stop processing. Please check if the source data matches the schema, and consider disabling strict mode or increasing max_filter_ratio.",
            q.hi as u64, q.lo as u64
        )
        .as_str()
    );
    let failure = doris_thrift::gen::status::TStatus { status_code: TStatusCode::CANCELLED, error_msgs: Some(vec![msg.clone()]) };
    let outcome = sink.finish(&mgr, &tmp.path().join("error_log"), Some(failure.clone()));
    let report = exec_status(&f, &outcome, Some("url".into()));
    assert_eq!(report.status, Some(failure));
    let c = report.load_counters.clone().unwrap();
    assert_eq!((c["dpp.norm.ALL"].as_str(), c["dpp.abnorm.ALL"].as_str()), ("96", "0"));
    assert_eq!(report.fragment_instance_reports.as_ref().unwrap()[0].loaded_rows, Some(96));
    assert_eq!(report.commit_infos, None);
    assert_eq!(report.tracking_url.as_deref(), Some("url"));
    assert_eq!(report.first_error_msg.as_deref(), Some("bad row 1. Src line: "));
    let log = std::fs::read_to_string(tmp.path().join("error_log").join(outcome.error_log.unwrap())).unwrap();
    assert_eq!(log.lines().count(), 51);
}

/// Without zero tolerance the same rows load what they can and filter the rest.
#[test]
fn errors_past_fifty_without_zero_tolerance_are_filtered() {
    use doris_exec::sink::TableSink;
    use doris_exec::RowError;
    let (v, mut f, rows) = loading(1);
    f.zero_tolerance = false;
    let tmp = tempfile::tempdir().unwrap();
    let mgr = manager(&v, tmp.path()).unwrap();
    let input: Vec<Result<Vec<Value>, RowError>> =
        (0..96).map(|i| if i % 3 == 0 { Ok(rows[0].clone()) } else { Err(RowError::new(format!("bad row {i}"))) }).collect();
    let sink = TableSink::new(&f, vec![instance(1)]).unwrap();
    sink.push(0, input, &f.input_nullable()).unwrap();
    let outcome = sink.finish(&mgr, &tmp.path().join("error_log"), None);
    assert_eq!((outcome.loaded, outcome.filtered), (32, 64));
    assert_eq!(outcome.status.status_code, TStatusCode::OK);
}

/// `RuntimeState::init`: an insert's error log is `insert_stmt`'s, labelled with the instance id
/// (`print_id`): `error_log_insert_stmt_<hi>-<lo>_<hi>_<lo>`.
#[test]
fn the_error_log_is_named_as_cpp() {
    use doris_exec::RowError;
    let (v, f, rows) = loading(1);
    let tmp = tempfile::tempdir().unwrap();
    let mgr = manager(&v, tmp.path()).unwrap();
    let outcome = run_sink(&f, vec![Ok(rows[0].clone()), Err(RowError::new("bad"))], &mgr, &tmp.path().join("error_log"));
    let (hi, lo) = (f.instance_id.hi as u64, f.instance_id.lo as u64);
    let name = outcome.error_log.unwrap();
    let file = name.split('/').nth(1).unwrap();
    assert!(name.starts_with("__shard_"), "{name}");
    assert_eq!(file, format!("error_log_insert_stmt_{hi:x}-{lo:x}_{hi:x}_{lo:x}"));
}
