use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use doris_be::agent::AgentService;
use doris_be::config::BeConfig;
use doris_be::heartbeat::HeartbeatHandler;
use doris_be::master::MasterInfo;
use doris_be::testing::FakeFe;
use doris_fixtures::{decode, Fixtures};
use doris_storage::{DataDir, TabletManager};
use doris_thrift::gen::backend_service::{BackendServiceSubmitTasksArgs, BackendServiceSubmitTasksResult};
use doris_thrift::gen::heartbeat_service::{HeartbeatServiceHeartbeatArgs, HeartbeatServiceSyncHandler};
use doris_thrift::gen::master_service::TFinishTaskRequest;
use doris_thrift::gen::status::TStatusCode;
use doris_thrift::gen::types::TTaskType;

struct Be {
    agent: AgentService,
    fe: Arc<FakeFe>,
    master: MasterInfo,
    tablets: Arc<Mutex<TabletManager>>,
    heartbeat: HeartbeatHandler,
    _tmp: tempfile::TempDir,
}

fn be() -> Be {
    let tmp = tempfile::tempdir().unwrap();
    let tablets = Arc::new(Mutex::new(TabletManager::open(vec![DataDir::open(tmp.path(), "127.0.0.1").unwrap()]).unwrap()));
    let master = MasterInfo::new("127.0.0.1");
    let fe = Arc::new(FakeFe::default());
    let cfg = BeConfig::default();
    let agent = AgentService::new(&cfg, tablets.clone(), master.clone(), fe.clone());
    let heartbeat = HeartbeatHandler::new(&cfg, master.clone(), tablets.clone());
    Be { agent, fe, master, tablets, heartbeat, _tmp: tmp }
}

fn first_heartbeat(be: &Be, fx: &Fixtures) {
    let (_, bytes) = &fx.messages("heartbeat", "call")[0];
    let info = decode(bytes, HeartbeatServiceHeartbeatArgs::read_from_in_protocol).master_info;
    assert_eq!(be.heartbeat.handle_heartbeat(info).unwrap().status.status_code, TStatusCode::OK);
}

/// Overwrites the deployment- and time-dependent fields of a finishTask request.
fn mask(got: &mut TFinishTaskRequest, expected: &TFinishTaskRequest) {
    // BackendOptions::get_localhost(): deployment fact be-host
    got.backend.host = expected.backend.host.clone();
    // s_report_version: unix time * 10000 at start, plus changes
    got.report_version = expected.report_version;
    // DataDir::path_hash(): hash_of_path(host, storage root), deployment fact
    for (g, e) in got.finish_tablet_infos.iter_mut().flatten().zip(expected.finish_tablet_infos.iter().flatten()) {
        g.path_hash = e.path_hash;
    }
}

#[test]
fn create_and_drop_finish_like_the_reference() {
    let fx = Fixtures::m2();
    let be = be();
    first_heartbeat(&be, &fx);
    for (entry, bytes) in fx.messages("submit_tasks", "call") {
        let tasks = decode(&bytes, BackendServiceSubmitTasksArgs::read_from_in_protocol).tasks;
        let (_, reply) = fx.reply_to(&entry).expect("reply");
        let expected = decode(&reply, BackendServiceSubmitTasksResult::read_from_in_protocol).result_value.unwrap();
        assert_eq!(be.agent.submit_tasks(tasks), expected);
        assert!(be.agent.wait_idle(Duration::from_secs(10)));
    }
    let key = |f: &TFinishTaskRequest| (f.task_type.0, f.signature);
    let wanted = |f: &&TFinishTaskRequest| matches!(f.task_type, TTaskType::CREATE | TTaskType::DROP);
    let expected: BTreeMap<_, _> = fx.finish_requests().iter().filter(wanted).map(|f| (key(f), f.clone())).collect();
    let got: BTreeMap<_, _> = be.fe.finishes().iter().filter(wanted).map(|f| (key(f), f.clone())).collect();
    assert_eq!(got.keys().collect::<Vec<_>>(), expected.keys().collect::<Vec<_>>());
    for (k, mut g) in got {
        let e = &expected[&k];
        mask(&mut g, e);
        assert_eq!(&g, e, "finishTask {k:?}");
    }
}

#[test]
fn update_visible_version_moves_partitions_and_sends_nothing() {
    let fx = Fixtures::m2();
    let be = be();
    first_heartbeat(&be, &fx);
    let create = fx.tasks().into_iter().find(|t| t.task_type == TTaskType::CREATE).unwrap();
    let req = create.create_tablet_req.clone().unwrap();
    be.agent.submit_tasks(vec![create]);
    assert!(be.agent.wait_idle(Duration::from_secs(10)));
    let uvv = fx.tasks().into_iter().find(|t| t.task_type == TTaskType::UPDATE_VISIBLE_VERSION).unwrap();
    let mut uvv = uvv.clone();
    let pid = req.partition_id.unwrap();
    uvv.visible_version_req.as_mut().unwrap().partition_version = Some([(pid, 4)].into());
    let before = be.fe.finishes().len();
    be.agent.submit_tasks(vec![uvv]);
    assert!(be.agent.wait_idle(Duration::from_secs(10)));
    assert_eq!(be.fe.finishes().len(), before);
    assert_eq!(be.tablets.lock().unwrap().partitions_version()[&pid], 4);
}

#[test]
fn an_unsupported_task_finishes_with_an_error() {
    let fx = Fixtures::m2();
    let be = be();
    first_heartbeat(&be, &fx);
    let mut task = fx.tasks()[0].clone();
    task.task_type = TTaskType::ALTER;
    task.signature = 77;
    let reply = be.agent.submit_tasks(vec![task]);
    assert_eq!(reply.status.status_code, TStatusCode::OK);
    assert!(be.agent.wait_idle(Duration::from_secs(10)));
    let finishes = be.fe.finishes();
    let f = finishes.iter().find(|f| f.signature == 77).expect("finishTask");
    assert_eq!(f.task_status.status_code, TStatusCode::NOT_IMPLEMENTED_ERROR);
    assert!(f.task_status.error_msgs.as_ref().unwrap()[0].contains("ALTER"));
}

#[test]
fn tasks_before_the_first_heartbeat_are_refused() {
    // AgentServer::submit_tasks: no master yet → Status::Cancelled, nothing runs
    let fx = Fixtures::m2();
    let be = be();
    let task = fx.tasks()[0].clone();
    let reply = be.agent.submit_tasks(vec![task]);
    assert_eq!(reply.status.status_code, TStatusCode::CANCELLED);
    assert_eq!(reply.status.error_msgs, Some(vec!["(127.0.0.1)[CANCELLED]Have not get FE Master heartbeat yet".into()]));
    assert!(be.agent.wait_idle(Duration::from_secs(1)));
    assert!(be.fe.finishes().is_empty());
    assert!(be.tablets.lock().unwrap().tablet_infos().is_empty());
    assert_eq!(be.master.fe_address(), None);
}

#[test]
fn a_repeated_signature_runs_once() {
    let fx = Fixtures::m2();
    let be = be();
    first_heartbeat(&be, &fx);
    let task = fx.tasks().into_iter().find(|t| t.task_type == TTaskType::CREATE).unwrap();
    be.fe.block_finishes(true);
    be.agent.submit_tasks(vec![task.clone()]);
    be.agent.submit_tasks(vec![task.clone()]);
    be.fe.block_finishes(false);
    assert!(be.agent.wait_idle(Duration::from_secs(10)));
    assert_eq!(be.fe.finishes().iter().filter(|f| f.signature == task.signature).count(), 1);
}

#[test]
fn finish_task_is_retried_three_times() {
    let fx = Fixtures::m2();
    let be = be();
    first_heartbeat(&be, &fx);
    be.fe.fail_next(2);
    let task = fx.tasks().into_iter().find(|t| t.task_type == TTaskType::CREATE).unwrap();
    be.agent.submit_tasks(vec![task]);
    assert!(be.agent.wait_idle(Duration::from_secs(10)));
    assert_eq!(be.fe.finishes().len(), 1);
    assert_eq!(be.fe.attempts(), 3);
}
