//! INSERT … VALUES through the BE: the fragment is accepted at once, runs, and one final
//! reportExecStatus goes to the fragment's coordinator.
use std::sync::{Arc, Mutex};
use std::time::Duration;

use doris_be::load::LoadService;
use doris_be::master::MasterInfo;
use doris_be::testing::{FakeFe, LoadCall};
use doris_exec::fragment::plan_write;
use doris_fixtures::values::values_vectors;
use doris_proto::doris::PCancelPlanFragmentRequest;
use doris_storage::{DataDir, TabletManager, TabletOptions};

fn service(tmp: &std::path::Path) -> (LoadService, Arc<FakeFe>, Arc<Mutex<TabletManager>>) {
    let tablets = Arc::new(Mutex::new(TabletManager::open(vec![DataDir::open(tmp, "127.0.0.1").unwrap()], TabletOptions::default()).unwrap()));
    let fe = Arc::new(FakeFe::default());
    let svc = LoadService::new(tablets.clone(), fe.clone(), MasterInfo::new("127.0.0.1"), 8040);
    (svc, fe, tablets)
}

#[test]
fn insert_values_reports_and_commits() {
    let v = values_vectors().into_iter().find(|v| plan_write(&v.params()).is_ok_and(|f| f.values_rows().is_ok())).unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let (svc, fe, tablets) = service(tmp.path());
    for req in v.create_requests() {
        tablets.lock().unwrap().create_tablet(&req).unwrap();
    }
    // the reply comes before the fragment runs
    let reply = svc.exec_plan_fragment(&v.request);
    assert_eq!(reply.status.status_code, 0);
    assert!(svc.wait_idle(Duration::from_secs(20)));
    let calls = fe.load_calls();
    assert_eq!(calls.len(), 1, "{calls:?}");
    let LoadCall::ExecStatus(coord, mut got) = calls[0].clone() else { panic!("{calls:?}") };
    assert_eq!(Some(coord), v.params().coord);
    let expected = &v.report;
    got.query_id = expected.query_id.clone();
    got.backend_id = expected.backend_id;
    for (g, e) in got.fragment_instance_reports.iter_mut().flatten().zip(expected.fragment_instance_reports.iter().flatten()) {
        g.fragment_instance_id = e.fragment_instance_id.clone();
    }
    let sort = |r: &mut doris_thrift::gen::frontend_service::TReportExecStatusParams| {
        if let Some(c) = r.commit_infos.as_mut() {
            c.sort_by_key(|i| i.tablet_id);
        }
    };
    let mut expected = expected.clone();
    sort(&mut got);
    sort(&mut expected);
    assert_eq!(got, expected);
}

#[test]
fn a_query_fragment_is_refused_at_once() {
    let v = &values_vectors()[0];
    let mut req = v.request.clone();
    // a SELECT plan: the first node becomes an OLAP scan
    let mut params = v.params();
    params.fragment.as_mut().unwrap().plan.as_mut().unwrap().nodes[0].node_type = doris_thrift::gen::plan_nodes::TPlanNodeType::OLAP_SCAN_NODE;
    let list = doris_thrift::gen::palo_internal_service::TPipelineFragmentParamsList::new(vec![params], None, None, None, None, None, None, None, None, None, None, None, None, None);
    let mut buf = Vec::new();
    {
        use thrift::protocol::TSerializable;
        let mut p = thrift::protocol::TCompactOutputProtocol::new(&mut buf);
        list.write_to_out_protocol(&mut p).unwrap();
    }
    req.request = Some(buf);
    let tmp = tempfile::tempdir().unwrap();
    let (svc, fe, _) = service(tmp.path());
    let reply = svc.exec_plan_fragment(&req);
    let st = reply.status;
    assert_ne!(st.status_code, 0);
    assert!(st.error_msgs.iter().any(|m| m.contains("OLAP_SCAN_NODE")), "{st:?}");
    assert!(svc.wait_idle(Duration::from_secs(5)));
    assert!(fe.load_calls().is_empty());
}

#[test]
fn cancel_finished_is_ok() {
    let tmp = tempfile::tempdir().unwrap();
    let (svc, _, _) = service(tmp.path());
    let reply = svc.cancel_plan_fragment(&PCancelPlanFragmentRequest::default());
    assert_eq!(reply.status.status_code, 0);
}
