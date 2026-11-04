//! `HeartbeatService` (port 9050), as `HeartbeatServer` does it (`be/src/agent/heartbeat_server.cpp`).

use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use doris_storage::TabletManager;
use doris_thrift::gen::heartbeat_service::{
    HeartbeatServiceSyncHandler, TBackendInfo, TFrontendInfo, THeartbeatResult, TMasterInfo,
};
use doris_thrift::gen::types::TNetworkAddress;

use crate::config::BeConfig;
use crate::master::MasterInfo;
use crate::status;

/// The reference's `get_short_version()`.
pub const VERSION: &str = "doris-4.1.5-release-917b20be838";
/// `config::arrow_flight_sql_port`'s default.
const ARROW_FLIGHT_SQL_PORT: i32 = 8050;
/// `config::be_node_role`'s default.
const BE_NODE_ROLE: &str = "mix";

pub struct HeartbeatHandler {
    master: MasterInfo,
    tablets: Arc<Mutex<TabletManager>>,
    be_port: i32,
    http_port: i32,
    brpc_port: i32,
    start_ms: i64,
    be_mem: i64,
}

fn now_ms() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0)
}

impl HeartbeatHandler {
    pub fn new(cfg: &BeConfig, master: MasterInfo, tablets: Arc<Mutex<TabletManager>>) -> HeartbeatHandler {
        HeartbeatHandler {
            master,
            tablets,
            be_port: i32::from(cfg.be_port),
            http_port: i32::from(cfg.webserver_port),
            brpc_port: i32::from(cfg.brpc_port),
            start_ms: now_ms(),
            be_mem: physical_mem(),
        }
    }

    /// `HeartbeatServer::_heartbeat`, in the C++ order: an error stops the checks that follow
    /// but keeps what earlier ones set.
    fn apply(&self, info: &TMasterInfo) -> Result<(), String> {
        {
            let mut tablets = self.tablets.lock().unwrap_or_else(|e| e.into_inner());
            match tablets.cluster_id() {
                None => tablets.set_cluster_id(info.cluster_id).map_err(|e| e.to_string())?,
                Some(id) if id != info.cluster_id => {
                    let recorded = self.master.with(|s| print_frontend_infos(&s.frontends));
                    return Err(format!(
                        "invalid cluster id. ignore. Record cluster id ={id}, record frontend info {recorded}. \
                         Invalid cluster_id={}, invalid frontend info {}",
                        info.cluster_id,
                        print_frontend_infos(info.frontend_infos.as_deref().unwrap_or_default())
                    ));
                }
                Some(_) => {}
            }
        }
        self.master.with(|s| {
            // backend_ip: the address the FE knows this BE by becomes its local host.
            if let Some(ip) = &info.backend_ip {
                if *ip != s.localhost {
                    s.localhost = ip.clone();
                }
            }
            let addr = &info.network_address;
            let current = s.master.clone().unwrap_or(TNetworkAddress { hostname: String::new(), port: 0 });
            if current.hostname != addr.hostname || current.port != addr.port {
                if info.epoch > s.epoch {
                    s.master = Some(addr.clone());
                    s.epoch = info.epoch;
                    s.report_signal += 1;
                } else {
                    return Err(format!(
                        "epoch is not greater than local. ignore heartbeat. host: {}, port: {}, local epoch: {}, received epoch: {}",
                        current.hostname, current.port, s.epoch, info.epoch
                    ));
                }
            } else if info.epoch > s.epoch {
                s.epoch = info.epoch;
                s.report_signal += 1;
            }
            if let Some(token) = &info.token {
                match &s.token {
                    None => s.token = Some(token.clone()),
                    Some(local) if local != token => return Err(format!("invalid token. local: {local}, master: {token}")),
                    Some(_) => {}
                }
            }
            if info.http_port.is_some() {
                s.master_http_port = info.http_port;
            }
            if info.backend_id.is_some() {
                s.backend_id = info.backend_id;
            }
            if let Some(fes) = &info.frontend_infos {
                s.frontends = fes.clone();
            }
            if let Some(auth) = &info.auth_token {
                s.auth_token = Some(auth.clone());
            }
            Ok(())
        })
    }
}

impl HeartbeatServiceSyncHandler for HeartbeatHandler {
    fn handle_heartbeat(&self, info: TMasterInfo) -> thrift::Result<THeartbeatResult> {
        let empty = TBackendInfo::new(0, 0, None, None, None, None, None, None, None, None, None, None, None);
        Ok(match self.apply(&info) {
            Err(msg) => THeartbeatResult { status: status::internal_error(&self.master.localhost(), &msg), backend_info: empty },
            Ok(()) => THeartbeatResult {
                status: status::ok(),
                backend_info: TBackendInfo {
                    be_port: self.be_port,
                    http_port: self.http_port,
                    be_rpc_port: Some(-1),
                    brpc_port: Some(self.brpc_port),
                    version: Some(VERSION.to_string()),
                    be_start_time: Some(self.start_ms),
                    be_node_role: Some(BE_NODE_ROLE.to_string()),
                    is_shutdown: Some(false),
                    arrow_flight_sql_port: Some(ARROW_FLIGHT_SQL_PORT),
                    be_mem: Some(self.be_mem),
                    supports_paimon_rust_reader: Some(true),
                    fragment_executing_count: Some(0),
                    fragment_last_active_time: Some(self.start_ms),
                },
            },
        })
    }
}

/// `PrintFrontendInfos`: each `TFrontendInfo::printTo`, then a space.
fn print_frontend_infos(infos: &[TFrontendInfo]) -> String {
    let addr = |a: &TNetworkAddress| format!("TNetworkAddress(hostname={}, port={})", a.hostname, a.port);
    infos
        .iter()
        .map(|f| {
            format!(
                "TFrontendInfo(coordinator_address={}, process_uuid={}) ",
                f.coordinator_address.as_ref().map(addr).unwrap_or_else(|| "<null>".into()),
                f.process_uuid.map(|u| u.to_string()).unwrap_or_else(|| "<null>".into())
            )
        })
        .collect()
}

/// `MemInfo::physical_mem()`: total memory of the host.
fn physical_mem() -> i64 {
    if let Ok(text) = std::fs::read_to_string("/proc/meminfo") {
        let kb = text.lines().find_map(|l| l.strip_prefix("MemTotal:")).and_then(|v| v.trim().trim_end_matches("kB").trim().parse::<i64>().ok());
        if let Some(kb) = kb {
            return kb * 1024;
        }
    }
    std::process::Command::new("sysctl")
        .args(["-n", "hw.memsize"])
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(0)
}
