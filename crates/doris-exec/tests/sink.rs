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
