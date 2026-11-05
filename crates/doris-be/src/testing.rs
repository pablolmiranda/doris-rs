//! Test doubles for the BE's collaborators.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Condvar, Mutex};

use doris_thrift::gen::frontend_service::{
    TLoadTxnBeginRequest, TLoadTxnBeginResult, TLoadTxnCommitRequest, TLoadTxnCommitResult, TLoadTxnRollbackRequest,
    TLoadTxnRollbackResult, TReportExecStatusParams, TReportExecStatusResult, TStreamLoadPutRequest, TStreamLoadPutResult,
};
use doris_thrift::gen::master_service::{TFinishTaskRequest, TMasterResult, TReportRequest};
use doris_thrift::gen::types::TNetworkAddress;

use crate::fe_client::{FeClient, FeError};
use crate::status;

/// A call the BE made to the FE for a load.
#[derive(Debug, Clone, PartialEq)]
pub enum LoadCall {
    ExecStatus(TNetworkAddress, TReportExecStatusParams),
    Begin(TLoadTxnBeginRequest),
    Put(TStreamLoadPutRequest),
    Commit(TLoadTxnCommitRequest),
    Rollback(TLoadTxnRollbackRequest),
}

/// The FE's answers to a stream load's calls.
#[derive(Debug, Clone, Default)]
pub struct LoadScript {
    pub begin: Option<TLoadTxnBeginResult>,
    pub put: Option<TStreamLoadPutResult>,
    pub commit: Option<TLoadTxnCommitResult>,
    pub rollback: Option<TLoadTxnRollbackResult>,
}

/// A master FE that records what the BE sends and answers OK. It can fail the next calls, or
/// hold `finishTask` calls until released. Load calls get the scripted answers.
#[derive(Default)]
pub struct FakeFe {
    loads: Mutex<Vec<LoadCall>>,
    script: Mutex<LoadScript>,
    finishes: Mutex<Vec<TFinishTaskRequest>>,
    reports: Mutex<Vec<TReportRequest>>,
    attempts: AtomicUsize,
    failures: AtomicUsize,
    blocked: Mutex<bool>,
    unblocked: Condvar,
}

impl FakeFe {
    pub fn load_calls(&self) -> Vec<LoadCall> {
        self.loads.lock().unwrap().clone()
    }

    pub fn script(&self, script: LoadScript) {
        *self.script.lock().unwrap() = script;
    }

    fn load(&self, call: LoadCall) {
        self.loads.lock().unwrap().push(call);
    }

    pub fn finishes(&self) -> Vec<TFinishTaskRequest> {
        self.finishes.lock().unwrap().clone()
    }

    pub fn reports(&self) -> Vec<TReportRequest> {
        self.reports.lock().unwrap().clone()
    }

    /// `finishTask` calls received, failed ones included.
    pub fn attempts(&self) -> usize {
        self.attempts.load(Ordering::SeqCst)
    }

    pub fn fail_next(&self, n: usize) {
        self.failures.store(n, Ordering::SeqCst);
    }

    pub fn block_finishes(&self, block: bool) {
        *self.blocked.lock().unwrap() = block;
        self.unblocked.notify_all();
    }

    fn fail(&self) -> bool {
        self.failures.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1)).is_ok()
    }
}

fn ok() -> TMasterResult {
    TMasterResult { status: status::ok() }
}

impl FeClient for FakeFe {
    fn finish_task(&self, req: TFinishTaskRequest) -> Result<TMasterResult, FeError> {
        let blocked = self.blocked.lock().unwrap();
        drop(self.unblocked.wait_while(blocked, |b| *b).unwrap());
        self.attempts.fetch_add(1, Ordering::SeqCst);
        if self.fail() {
            return Err(FeError::Call { addr: "fake".into(), msg: "injected failure".into() });
        }
        self.finishes.lock().unwrap().push(req);
        Ok(ok())
    }

    fn report(&self, req: TReportRequest) -> Result<TMasterResult, FeError> {
        self.reports.lock().unwrap().push(req);
        Ok(ok())
    }

    fn report_exec_status(&self, coord: &TNetworkAddress, req: TReportExecStatusParams) -> Result<TReportExecStatusResult, FeError> {
        self.load(LoadCall::ExecStatus(coord.clone(), req));
        Ok(TReportExecStatusResult::new(status::ok()))
    }

    fn load_txn_begin(&self, req: TLoadTxnBeginRequest) -> Result<TLoadTxnBeginResult, FeError> {
        self.load(LoadCall::Begin(req));
        self.script.lock().unwrap().begin.clone().ok_or_else(unscripted)
    }

    fn stream_load_put(&self, req: TStreamLoadPutRequest) -> Result<TStreamLoadPutResult, FeError> {
        self.load(LoadCall::Put(req));
        self.script.lock().unwrap().put.clone().ok_or_else(unscripted)
    }

    fn load_txn_commit(&self, req: TLoadTxnCommitRequest) -> Result<TLoadTxnCommitResult, FeError> {
        self.load(LoadCall::Commit(req));
        self.script.lock().unwrap().commit.clone().ok_or_else(unscripted)
    }

    fn load_txn_rollback(&self, req: TLoadTxnRollbackRequest) -> Result<TLoadTxnRollbackResult, FeError> {
        self.load(LoadCall::Rollback(req));
        self.script.lock().unwrap().rollback.clone().ok_or_else(unscripted)
    }
}

fn unscripted() -> FeError {
    FeError::Call { addr: "fake".into(), msg: "no scripted answer".into() }
}

/// A [`FakeFe`] reachable over the network as the master FE's Thrift port: it answers
/// `report` and `finishTask` (strict binary, unframed) with OK and records them; other methods
/// get an application exception.
pub struct FakeFeServer {
    pub fe: std::sync::Arc<FakeFe>,
    pub port: u16,
}

impl FakeFeServer {
    pub fn start() -> FakeFeServer {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind fake FE");
        let port = listener.local_addr().expect("addr").port();
        let fe = std::sync::Arc::new(FakeFe::default());
        let shared = fe.clone();
        std::thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                let fe = shared.clone();
                std::thread::spawn(move || serve_fake_fe(&fe, stream));
            }
        });
        FakeFeServer { fe, port }
    }
}

fn serve_fake_fe(fe: &FakeFe, stream: std::net::TcpStream) {
    use doris_thrift::gen::frontend_service::{FrontendServiceFinishTaskArgs, FrontendServiceReportArgs};
    use thrift::protocol::{
        TBinaryInputProtocol, TBinaryOutputProtocol, TFieldIdentifier, TInputProtocol, TMessageIdentifier, TMessageType,
        TOutputProtocol, TSerializable, TStructIdentifier, TType,
    };
    use thrift::transport::{TBufferedReadTransport, TBufferedWriteTransport};

    let Ok(read) = stream.try_clone() else { return };
    let mut i = TBinaryInputProtocol::new(TBufferedReadTransport::new(read), true);
    let mut o = TBinaryOutputProtocol::new(TBufferedWriteTransport::new(stream), true);
    loop {
        let Ok(msg) = i.read_message_begin() else { return };
        let result = match msg.name.as_str() {
            "report" => FrontendServiceReportArgs::read_from_in_protocol(&mut i).ok().map(|a| fe.report(a.request)),
            "finishTask" => FrontendServiceFinishTaskArgs::read_from_in_protocol(&mut i).ok().map(|a| fe.finish_task(a.request)),
            _ => None,
        };
        let _ = i.read_message_end();
        let Some(Ok(result)) = result else { return };
        let reply = TMessageIdentifier::new(msg.name.clone(), TMessageType::Reply, msg.sequence_number);
        let wrote = (|| -> thrift::Result<()> {
            o.write_message_begin(&reply)?;
            o.write_struct_begin(&TStructIdentifier::new("result"))?;
            o.write_field_begin(&TFieldIdentifier::new("success", TType::Struct, 0))?;
            result.write_to_out_protocol(&mut o)?;
            o.write_field_end()?;
            o.write_field_stop()?;
            o.write_struct_end()?;
            o.write_message_end()?;
            o.flush()
        })();
        if wrote.is_err() {
            return;
        }
    }
}
