use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use doris_be::testing::FakeFeServer;
use doris_fixtures::{decode, Fixtures};
use doris_thrift::gen::backend_service::{BackendServiceSyncClient, TBackendServiceSyncClient};
use doris_thrift::gen::heartbeat_service::{
    HeartbeatServiceHeartbeatArgs, HeartbeatServiceSyncClient, THeartbeatServiceSyncClient, TMasterInfo,
};
use doris_thrift::gen::status::TStatusCode;
use doris_thrift::gen::types::{TNetworkAddress, TTaskType};
use thrift::protocol::{TBinaryInputProtocol, TBinaryOutputProtocol};
use thrift::transport::{TBufferedReadTransport, TBufferedWriteTransport};

const BIN: &str = env!("CARGO_BIN_EXE_doris-rs");

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

struct Ports {
    heartbeat: u16,
    be: u16,
    brpc: u16,
    http: u16,
}

fn write_conf(dir: &Path, root: &Path, p: &Ports) -> std::path::PathBuf {
    let conf = dir.join("be.conf");
    std::fs::write(
        &conf,
        format!(
            "heartbeat_service_port = {}\nbe_port = {}\nbrpc_port = {}\nwebserver_port = {}\n\
             storage_root_path = {}\nhost = 127.0.0.1\nreport_tablet_interval_seconds = 1\n\
             report_task_interval_seconds = 1\nreport_disk_state_interval_seconds = 1\n",
            p.heartbeat,
            p.be,
            p.brpc,
            p.http,
            root.display()
        ),
    )
    .unwrap();
    conf
}

struct Proc(Child);

impl Drop for Proc {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn spawn(conf: &Path) -> Proc {
    Proc(Command::new(BIN).args(["be", "--config"]).arg(conf).stderr(Stdio::null()).spawn().unwrap())
}

fn wait_port(port: u16) {
    let deadline = Instant::now() + Duration::from_secs(20);
    while TcpStream::connect(("127.0.0.1", port)).is_err() {
        assert!(Instant::now() < deadline, "port {port} never opened");
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn wait_for(what: &str, mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while !f() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(100));
    }
}

type Protocols = (TBinaryInputProtocol<TBufferedReadTransport<TcpStream>>, TBinaryOutputProtocol<TBufferedWriteTransport<TcpStream>>);

fn protocols(port: u16) -> Protocols {
    let s = TcpStream::connect(("127.0.0.1", port)).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    let r = s.try_clone().unwrap();
    (TBinaryInputProtocol::new(TBufferedReadTransport::new(r), true), TBinaryOutputProtocol::new(TBufferedWriteTransport::new(s), true))
}

/// The reference's first heartbeat, pointed at `fe_port`.
fn master_info(fe_port: u16) -> TMasterInfo {
    let fx = Fixtures::m2();
    let (_, bytes) = &fx.messages("heartbeat", "call")[0];
    let mut info = decode(bytes, HeartbeatServiceHeartbeatArgs::read_from_in_protocol).master_info;
    info.network_address = TNetworkAddress { hostname: "127.0.0.1".into(), port: i32::from(fe_port) };
    info
}

fn heartbeat(port: u16, info: TMasterInfo) -> TStatusCode {
    let (i, o) = protocols(port);
    HeartbeatServiceSyncClient::new(i, o).heartbeat(info).unwrap().status.status_code
}

#[test]
fn the_binary_answers_on_every_port() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("storage");
    std::fs::create_dir(&root).unwrap();
    let p = Ports { heartbeat: free_port(), be: free_port(), brpc: free_port(), http: free_port() };
    let _be = spawn(&write_conf(tmp.path(), &root, &p));
    for port in [p.heartbeat, p.be, p.brpc, p.http] {
        wait_port(port);
    }
    assert_eq!(heartbeat(p.heartbeat, master_info(free_port())), TStatusCode::OK);

    let mut s = TcpStream::connect(("127.0.0.1", p.http)).unwrap();
    s.write_all(b"GET /api/health HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n").unwrap();
    let mut out = String::new();
    s.read_to_string(&mut out).unwrap();
    assert!(out.starts_with("HTTP/1.1 200") && out.ends_with(r#"{"status": "OK","msg": "OK"}"#), "{out}");

    // HTTP/2 preface and an empty SETTINGS frame: the server answers with its SETTINGS frame
    let mut s = TcpStream::connect(("127.0.0.1", p.brpc)).unwrap();
    s.write_all(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n\x00\x00\x00\x04\x00\x00\x00\x00\x00").unwrap();
    let mut frame = [0u8; 9];
    s.read_exact(&mut frame).unwrap();
    assert_eq!(frame[3], 4, "SETTINGS frame");

    let (i, o) = protocols(p.be);
    let stats = BackendServiceSyncClient::new(i, o).get_tablet_stat().unwrap();
    assert_eq!(stats.tablet_stat_list, Some(vec![]));
}

#[test]
fn tablets_survive_a_process_restart() {
    let fx = Fixtures::m2();
    let fe = FakeFeServer::start();
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("storage");
    std::fs::create_dir(&root).unwrap();
    let p = Ports { heartbeat: free_port(), be: free_port(), brpc: free_port(), http: free_port() };
    let conf = write_conf(tmp.path(), &root, &p);
    let creates: Vec<_> = fx.tasks().into_iter().filter(|t| t.task_type == TTaskType::CREATE).take(8).collect();
    let ids: Vec<i64> = creates.iter().map(|t| t.create_tablet_req.as_ref().unwrap().tablet_id).collect();
    let full_report = |fe: &FakeFeServer| {
        fe.fe.reports().into_iter().rev().find_map(|r| r.tablets.filter(|t| ids.iter().all(|id| t.contains_key(id))))
    };

    let before = {
        let _be = spawn(&conf);
        wait_port(p.heartbeat);
        wait_port(p.be);
        assert_eq!(heartbeat(p.heartbeat, master_info(fe.port)), TStatusCode::OK);
        let (i, o) = protocols(p.be);
        let reply = BackendServiceSyncClient::new(i, o).submit_tasks(creates.clone()).unwrap();
        assert_eq!(reply.status.status_code, TStatusCode::OK);
        wait_for("finishTask for every CREATE", || fe.fe.finishes().len() >= creates.len());
        wait_for("a tablet report with every tablet", || full_report(&fe).is_some());
        full_report(&fe).unwrap()
    };
    let reports_before = fe.fe.reports().len();

    let _be = spawn(&conf);
    wait_port(p.heartbeat);
    assert_eq!(heartbeat(p.heartbeat, master_info(fe.port)), TStatusCode::OK);
    wait_for("a tablet report after the restart", || {
        fe.fe.reports().iter().skip(reports_before).any(|r| r.tablets.is_some())
    });
    let after = fe.fe.reports().into_iter().skip(reports_before).find_map(|r| r.tablets).unwrap();
    assert_eq!(after, before);
}

#[test]
fn a_foreign_root_stops_the_start() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("storage");
    std::fs::create_dir_all(root.join("meta")).unwrap();
    let p = Ports { heartbeat: free_port(), be: free_port(), brpc: free_port(), http: free_port() };
    let out = Command::new(BIN).args(["be", "--config"]).arg(write_conf(tmp.path(), &root, &p)).output().unwrap();
    assert_eq!(out.status.code(), Some(1));
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains(&root.display().to_string()) && stderr.contains("C++ BE"), "{stderr}");
}
