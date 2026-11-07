//! PUBLISH_VERSION and CLEAR_TRANSACTION_TASK answer as the reference C++ BE did.
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use doris_be::agent::AgentService;
use doris_be::config::BeConfig;
use doris_be::master::MasterInfo;
use doris_be::testing::FakeFe;
use doris_fixtures::values::{values_vectors, ValuesVector};
use doris_proto::doris::PUniqueId;
use doris_segment::column::field_type;
use doris_segment::writer::{Datum, SegmentSchema};
use doris_storage::rowset::{segment_schema, write_rowset};
use doris_storage::{DataDir, TabletManager, TabletOptions};
use doris_thrift::gen::agent_service::{TAgentTaskRequest, TClearTransactionTaskRequest, TPublishVersionRequest};
use doris_thrift::gen::master_service::TFinishTaskRequest;
use doris_thrift::gen::status::TStatusCode;
use doris_thrift::gen::types::TTaskType;

struct Be {
    agent: AgentService,
    fe: Arc<FakeFe>,
    tablets: Arc<Mutex<TabletManager>>,
    _tmp: tempfile::TempDir,
}

fn be() -> Be {
    let tmp = tempfile::tempdir().unwrap();
    let tablets = Arc::new(Mutex::new(TabletManager::open(vec![DataDir::open(tmp.path(), "127.0.0.1").unwrap()], TabletOptions::default()).unwrap()));
    let master = MasterInfo::new("127.0.0.1");
    master.set_fe_address(doris_thrift::gen::types::TNetworkAddress::new("127.0.0.1".to_string(), 9020));
    let fe = Arc::new(FakeFe::default());
    let agent = AgentService::new(&BeConfig::default(), tablets.clone(), master, fe.clone());
    Be { agent, fe, tablets, _tmp: tmp }
}

/// A row of non-null values for every column of the schema.
fn row(schema: &SegmentSchema, i: i128) -> Vec<Datum> {
    schema
        .columns
        .iter()
        .map(|c| match c.field_type {
            field_type::BOOL => Datum::Bool(i % 2 == 0),
            field_type::FLOAT => Datum::Float(i as f32),
            field_type::DOUBLE => Datum::Double(i as f64),
            field_type::CHAR | field_type::VARCHAR | field_type::STRING => Datum::Bytes(b"x".to_vec()),
            field_type::DATEV2 => Datum::Int(2020 << 9 | 1 << 5 | 1),
            field_type::DATETIMEV2 => Datum::Int((2020 << 9 | 1 << 5 | 1) << 37),
            _ => Datum::Int(i),
        })
        .collect()
}

/// Creates the vector's tablets and commits its transaction: each tablet gets as many rows as
/// the reference's publish counted for it, the others an empty rowset. None when a table uses
/// a type the writer lacks.
fn commit_vector(be: &Be, v: &ValuesVector) -> Option<()> {
    let mut mgr = be.tablets.lock().unwrap();
    let publish = v.publish.publish_version_req.as_ref().unwrap();
    let deltas: BTreeMap<i64, i64> = v.finish.table_id_to_tablet_id_to_delta_num_rows.clone().unwrap_or_default().into_values().flatten().collect();
    let partitions: Vec<i64> = publish.partition_version_infos.iter().map(|p| p.partition_id).collect();
    for req in v.create_requests() {
        mgr.create_tablet(&req).ok()?;
    }
    // tablets of the partition the sink does not write (a schema change's shadow index): the
    // reference lists them as error tablets, as they lack the version
    let errors = v.finish.error_tablet_ids.clone().unwrap_or_default();
    if !errors.is_empty() {
        let [p] = partitions.as_slice() else { return None };
        let template = v.create_requests().into_iter().find(|r| r.partition_id == Some(*p))?;
        for id in errors {
            mgr.create_tablet(&doris_thrift::gen::agent_service::TCreateTabletReq { tablet_id: id, ..template.clone() }).ok()?;
        }
    }
    for req in v.create_requests() {
        let pid = req.partition_id.unwrap();
        if !partitions.contains(&pid) {
            continue;
        }
        let meta = mgr.tablet(req.tablet_id).unwrap().clone();
        let schema = segment_schema(meta.schema.as_ref().unwrap()).ok()?;
        let n = deltas.get(&req.tablet_id).copied().unwrap_or(0);
        let rows = (0..n).map(|i| row(&schema, i128::from(i))).collect();
        let id = mgr.next_rowset_id();
        let dir = mgr.tablet_path(req.tablet_id).unwrap();
        let rs = write_rowset(&dir, &meta, &id, publish.transaction_id, &PUniqueId { hi: 1, lo: 1 }, rows).unwrap().meta;
        mgr.commit_txn(pid, publish.transaction_id, req.tablet_id, PUniqueId { hi: 1, lo: 1 }, rs).unwrap();
    }
    Some(())
}

fn run(be: &Be, task: TAgentTaskRequest) -> TFinishTaskRequest {
    let (t, sig) = (task.task_type, task.signature);
    assert_eq!(be.agent.submit_tasks(vec![task]).status.status_code, TStatusCode::OK);
    assert!(be.agent.wait_idle(Duration::from_secs(10)));
    be.fe.finishes().into_iter().rev().find(|f| f.task_type == t && f.signature == sig).expect("a finishTask")
}

#[test]
fn publish_finish_task_matches_the_corpus() {
    let mut checked = 0;
    for v in values_vectors() {
        let be = be();
        if commit_vector(&be, &v).is_none() {
            continue;
        }
        let mut got = run(&be, v.publish.clone());
        let e = &v.finish;
        // deployment facts and the time-based report version
        got.backend = e.backend.clone();
        assert!(got.report_version.is_some(), "{}", v.dir);
        got.report_version = e.report_version;
        assert_eq!(got, *e, "{} ({})", v.dir, v.suite);
        checked += 1;
    }
    assert!(checked >= 40, "{checked}");
}

#[test]
fn publish_without_partitions_sends_empty_maps() {
    let be = be();
    let task = TAgentTaskRequest {
        signature: 77,
        publish_version_req: Some(TPublishVersionRequest::new(77, vec![], None, None)),
        ..publish_template()
    };
    let f = run(&be, task);
    assert_eq!(f.task_status.status_code, TStatusCode::OK);
    assert_eq!(f.succ_tablets, Some(BTreeMap::new()));
    assert_eq!(f.error_tablet_ids, Some(vec![]));
    assert_eq!(f.table_id_to_tablet_id_to_delta_num_rows, Some(BTreeMap::new()));
    assert!(f.report_version.is_some());
}

#[test]
fn clear_finish_task_has_no_report_version() {
    let be = be();
    let v = values_vectors().into_iter().find(|v| commit_vector(&be, v).is_some()).unwrap();
    let txn = v.publish.publish_version_req.as_ref().unwrap().transaction_id;
    let task = TAgentTaskRequest {
        task_type: TTaskType::CLEAR_TRANSACTION_TASK,
        signature: txn,
        publish_version_req: None,
        clear_transaction_task_req: Some(TClearTransactionTaskRequest::new(txn, vec![])),
        ..publish_template()
    };
    let f = run(&be, task);
    assert_eq!(f.task_status.status_code, TStatusCode::OK);
    assert_eq!(f.report_version, None);
    assert_eq!((f.succ_tablets.as_ref(), f.error_tablet_ids.as_ref()), (None, None));
    // the transaction is gone: its publish now finds no rowsets
    let after = run(&be, v.publish.clone());
    assert!(after.table_id_to_tablet_id_to_delta_num_rows.unwrap().is_empty());
}

fn publish_template() -> TAgentTaskRequest {
    values_vectors().into_iter().next().unwrap().publish
}
