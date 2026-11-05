//! Calls this BE makes to the master FE (`FrontendService` on the FE's Thrift port): `report`
//! and `finishTask`, as the C++ BE's `MasterServerClient` makes them.

use std::net::TcpStream;
use std::time::Duration;

use doris_thrift::gen::frontend_service::{FrontendServiceSyncClient, TFrontendServiceSyncClient};
use doris_thrift::gen::master_service::{TFinishTaskRequest, TMasterResult, TReportRequest};
use thrift::protocol::{TBinaryInputProtocol, TBinaryOutputProtocol};
use thrift::transport::{TBufferedReadTransport, TBufferedWriteTransport};

use crate::master::MasterInfo;

#[derive(Debug, thiserror::Error)]
pub enum FeError {
    #[error("no master FE is known yet")]
    NoMaster,
    #[error("calling the master FE at {addr}: {msg}")]
    Call { addr: String, msg: String },
}

pub trait FeClient: Send + Sync {
    fn finish_task(&self, req: TFinishTaskRequest) -> Result<TMasterResult, FeError>;
    fn report(&self, req: TReportRequest) -> Result<TMasterResult, FeError>;
}

/// Strict binary protocol, unframed, one connection per call, to the address the last
/// heartbeat named.
pub struct ThriftFeClient {
    master: MasterInfo,
    timeout: Duration,
}

impl ThriftFeClient {
    pub fn new(master: MasterInfo) -> ThriftFeClient {
        ThriftFeClient { master, timeout: Duration::from_secs(30) }
    }

    fn call<T>(
        &self,
        f: impl FnOnce(&mut dyn TFrontendServiceSyncClient) -> thrift::Result<T>,
    ) -> Result<T, FeError> {
        let master = self.master.fe_address().ok_or(FeError::NoMaster)?;
        let addr = format!("{}:{}", master.hostname, master.port);
        let err = |msg: String| FeError::Call { addr: addr.clone(), msg };
        let stream = TcpStream::connect(&addr).map_err(|e| err(e.to_string()))?;
        stream.set_read_timeout(Some(self.timeout)).map_err(|e| err(e.to_string()))?;
        stream.set_write_timeout(Some(self.timeout)).map_err(|e| err(e.to_string()))?;
        let read = stream.try_clone().map_err(|e| err(e.to_string()))?;
        let i_prot = TBinaryInputProtocol::new(TBufferedReadTransport::new(read), true);
        let o_prot = TBinaryOutputProtocol::new(TBufferedWriteTransport::new(stream), true);
        let mut client = FrontendServiceSyncClient::new(i_prot, o_prot);
        f(&mut client).map_err(|e| err(e.to_string()))
    }
}

impl FeClient for ThriftFeClient {
    fn finish_task(&self, req: TFinishTaskRequest) -> Result<TMasterResult, FeError> {
        self.call(|c| c.finish_task(req))
    }

    fn report(&self, req: TReportRequest) -> Result<TMasterResult, FeError> {
        self.call(|c| c.report(req))
    }
}
