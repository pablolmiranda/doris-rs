//! Calls this BE makes to the FE (`FrontendService` on the FE's Thrift port): `report` and
//! `finishTask` (`MasterServerClient`), a write fragment's `reportExecStatus` (to the
//! fragment's coordinator), and the stream load transaction calls (`StreamLoadExecutor`).

use std::net::TcpStream;
use std::time::Duration;

use doris_thrift::gen::frontend_service::{
    FrontendServiceSyncClient, TFrontendServiceSyncClient, TLoadTxnBeginRequest, TLoadTxnBeginResult, TLoadTxnCommitRequest,
    TLoadTxnCommitResult, TLoadTxnRollbackRequest, TLoadTxnRollbackResult, TReportExecStatusParams, TReportExecStatusResult,
    TStreamLoadPutRequest, TStreamLoadPutResult,
};
use doris_thrift::gen::types::TNetworkAddress;
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
    /// To the fragment's coordinator, which may not be the master.
    fn report_exec_status(&self, coord: &TNetworkAddress, req: TReportExecStatusParams) -> Result<TReportExecStatusResult, FeError>;
    fn load_txn_begin(&self, req: TLoadTxnBeginRequest) -> Result<TLoadTxnBeginResult, FeError>;
    fn stream_load_put(&self, req: TStreamLoadPutRequest) -> Result<TStreamLoadPutResult, FeError>;
    fn load_txn_commit(&self, req: TLoadTxnCommitRequest) -> Result<TLoadTxnCommitResult, FeError>;
    fn load_txn_rollback(&self, req: TLoadTxnRollbackRequest) -> Result<TLoadTxnRollbackResult, FeError>;
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
        self.call_at(&master, f)
    }

    fn call_at<T>(
        &self,
        fe: &TNetworkAddress,
        f: impl FnOnce(&mut dyn TFrontendServiceSyncClient) -> thrift::Result<T>,
    ) -> Result<T, FeError> {
        let addr = format!("{}:{}", fe.hostname, fe.port);
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

    fn report_exec_status(&self, coord: &TNetworkAddress, req: TReportExecStatusParams) -> Result<TReportExecStatusResult, FeError> {
        self.call_at(coord, |c| c.report_exec_status(req))
    }

    fn load_txn_begin(&self, req: TLoadTxnBeginRequest) -> Result<TLoadTxnBeginResult, FeError> {
        self.call(|c| c.load_txn_begin(req))
    }

    fn stream_load_put(&self, req: TStreamLoadPutRequest) -> Result<TStreamLoadPutResult, FeError> {
        self.call(|c| c.stream_load_put(req))
    }

    fn load_txn_commit(&self, req: TLoadTxnCommitRequest) -> Result<TLoadTxnCommitResult, FeError> {
        self.call(|c| c.load_txn_commit(req))
    }

    fn load_txn_rollback(&self, req: TLoadTxnRollbackRequest) -> Result<TLoadTxnRollbackResult, FeError> {
        self.call(|c| c.load_txn_rollback(req))
    }
}
