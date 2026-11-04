//! What this BE knows about its cluster from heartbeats (the C++ `ClusterInfo` and
//! `BackendOptions`): the master FE's address and epoch, tokens, its backend id, and the address
//! it registers as. Shared between the heartbeat service and everything that calls the FE.

use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use doris_thrift::gen::heartbeat_service::TFrontendInfo;
use doris_thrift::gen::types::TNetworkAddress;

#[derive(Debug, Default)]
pub(crate) struct ClusterState {
    pub localhost: String,
    pub master: Option<TNetworkAddress>,
    pub epoch: i64,
    pub token: Option<String>,
    pub auth_token: Option<String>,
    pub backend_id: Option<i64>,
    pub master_http_port: Option<i32>,
    pub frontends: Vec<TFrontendInfo>,
}

#[derive(Debug, Clone)]
pub struct MasterInfo {
    inner: Arc<(Mutex<ClusterState>, Condvar)>,
}

impl MasterInfo {
    pub fn new(localhost: &str) -> MasterInfo {
        let state = ClusterState { localhost: localhost.to_string(), ..Default::default() };
        MasterInfo { inner: Arc::new((Mutex::new(state), Condvar::new())) }
    }

    pub(crate) fn with<R>(&self, f: impl FnOnce(&mut ClusterState) -> R) -> R {
        let (lock, cv) = &*self.inner;
        let mut state = lock.lock().unwrap_or_else(|e| e.into_inner());
        let r = f(&mut state);
        cv.notify_all();
        r
    }

    pub fn fe_address(&self) -> Option<TNetworkAddress> {
        self.with(|s| s.master.clone())
    }

    /// Waits up to `timeout` for a master address.
    pub fn wait_fe_address(&self, timeout: Duration) -> Option<TNetworkAddress> {
        let (lock, cv) = &*self.inner;
        let state = lock.lock().unwrap_or_else(|e| e.into_inner());
        let (state, _) = cv.wait_timeout_while(state, timeout, |s| s.master.is_none()).unwrap_or_else(|e| e.into_inner());
        state.master.clone()
    }

    pub fn backend_id(&self) -> Option<i64> {
        self.with(|s| s.backend_id)
    }

    /// `BackendOptions::get_localhost()`: the address this BE registers as.
    pub fn localhost(&self) -> String {
        self.with(|s| s.localhost.clone())
    }

    /// Sets the master directly, as a heartbeat would (tests and tools).
    pub fn set_fe_address(&self, addr: TNetworkAddress) {
        self.with(|s| s.master = Some(addr));
    }
}
