use std::sync::{Arc, Mutex};

use doris_be::config::BeConfig;
use doris_be::heartbeat::HeartbeatHandler;
use doris_be::master::MasterInfo;
use doris_fixtures::{decode, Fixtures};
use doris_storage::{DataDir, TabletManager};
use doris_thrift::gen::heartbeat_service::{
    HeartbeatServiceHeartbeatArgs, HeartbeatServiceHeartbeatResult, HeartbeatServiceSyncHandler, TMasterInfo,
};
use doris_thrift::gen::status::TStatusCode;

fn handler(root: &std::path::Path) -> (HeartbeatHandler, MasterInfo) {
    let tablets = TabletManager::open(vec![DataDir::open(root, "127.0.0.1").unwrap()]).unwrap();
    let master = MasterInfo::new("127.0.0.1");
    let h = HeartbeatHandler::new(&BeConfig::default(), master.clone(), Arc::new(Mutex::new(tablets)));
    (h, master)
}

fn first_call(fx: &Fixtures) -> TMasterInfo {
    let (_, bytes) = &fx.messages("heartbeat", "call")[0];
    decode(bytes, HeartbeatServiceHeartbeatArgs::read_from_in_protocol).master_info
}

#[test]
fn heartbeat_reply_matches_the_reference() {
    let fx = Fixtures::m2();
    let tmp = tempfile::tempdir().unwrap();
    let (h, _) = handler(tmp.path());
    let calls = fx.messages("heartbeat", "call");
    assert!(calls.len() >= 5);
    for (entry, bytes) in calls {
        let info = decode(&bytes, HeartbeatServiceHeartbeatArgs::read_from_in_protocol).master_info;
        let (_, reply) = fx.reply_to(&entry).expect("reply");
        let expected = decode(&reply, HeartbeatServiceHeartbeatResult::read_from_in_protocol).result_value.unwrap();
        let mut got = h.handle_heartbeat(info).unwrap();
        let (g, e) = (&mut got.backend_info, &expected.backend_info);
        // _be_epoch: process start in ms (heartbeat_server.cpp:59)
        g.be_start_time = e.be_start_time;
        // g_fragment_last_active_time: time of the last fragment (fragment_mgr.cpp)
        g.fragment_last_active_time = e.fragment_last_active_time;
        // MemInfo::physical_mem(): deployment fact be-mem
        g.be_mem = e.be_mem;
        assert_eq!(got, expected);
    }
}

#[test]
fn first_heartbeat_assigns_cluster_id() {
    let fx = Fixtures::m2();
    let tmp = tempfile::tempdir().unwrap();
    let (h, master) = handler(tmp.path());
    assert_eq!(master.fe_address(), None);
    let info = first_call(&fx);
    let reply = h.handle_heartbeat(info.clone()).unwrap();
    assert_eq!(reply.status.status_code, TStatusCode::OK);
    assert_eq!(std::fs::read_to_string(tmp.path().join("cluster_id")).unwrap(), info.cluster_id.to_string());
    assert_eq!(master.fe_address(), Some(info.network_address.clone()));
    assert_eq!(master.backend_id(), info.backend_id);
}

#[test]
fn a_different_cluster_id_is_rejected() {
    let fx = Fixtures::m2();
    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(tmp.path().join("cluster_id"), "7").unwrap();
    let (h, master) = handler(tmp.path());
    let mut info = first_call(&fx);
    info.cluster_id = 8;
    let reply = h.handle_heartbeat(info).unwrap();
    assert_eq!(reply.status.status_code, TStatusCode::INTERNAL_ERROR);
    // HeartbeatServer::_heartbeat, through Status::to_thrift: "(<host>)[<code>]<message>"
    assert_eq!(
        reply.status.error_msgs,
        Some(vec![
            "(127.0.0.1)[INTERNAL_ERROR]invalid cluster id. ignore. Record cluster id =7, record frontend info . \
             Invalid cluster_id=8, invalid frontend info TFrontendInfo(coordinator_address=TNetworkAddress(\
             hostname=127.0.0.1, port=9020), process_uuid=1793832160329) "
                .to_string()
        ])
    );
    assert_eq!(reply.backend_info.be_port, 0);
    assert_eq!(reply.backend_info.version, None);
    assert_eq!(std::fs::read_to_string(tmp.path().join("cluster_id")).unwrap(), "7");
    assert_eq!(master.fe_address(), None);
}

#[test]
fn a_lower_epoch_from_another_master_is_rejected() {
    let fx = Fixtures::m2();
    let tmp = tempfile::tempdir().unwrap();
    let (h, master) = handler(tmp.path());
    let info = first_call(&fx);
    h.handle_heartbeat(info.clone()).unwrap();
    let mut other = info.clone();
    other.network_address.hostname = "10.0.0.9".into();
    let reply = h.handle_heartbeat(other).unwrap();
    assert_eq!(reply.status.status_code, TStatusCode::INTERNAL_ERROR);
    let msg = &reply.status.error_msgs.unwrap()[0];
    assert!(msg.contains("epoch is not greater than local. ignore heartbeat."), "{msg}");
    assert_eq!(master.fe_address(), Some(info.network_address));
}

#[test]
fn heartbeats_do_not_wait_for_storage() {
    // the cluster id is cached, as the C++ BE caches it in ClusterInfo; a heartbeat must answer
    // while tablet work holds the storage lock
    let fx = Fixtures::m2();
    let tmp = tempfile::tempdir().unwrap();
    let info = first_call(&fx);
    std::fs::write(tmp.path().join("cluster_id"), info.cluster_id.to_string()).unwrap();
    let tablets = Arc::new(Mutex::new(TabletManager::open(vec![DataDir::open(tmp.path(), "127.0.0.1").unwrap()]).unwrap()));
    let h = HeartbeatHandler::new(&BeConfig::default(), MasterInfo::new("127.0.0.1"), tablets.clone());
    let _held = tablets.lock().unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || tx.send(h.handle_heartbeat(info).unwrap().status.status_code).unwrap());
    assert_eq!(rx.recv_timeout(std::time::Duration::from_secs(2)), Ok(TStatusCode::OK));
}
