//! Test doubles for the BE's collaborators.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Condvar, Mutex};

use doris_thrift::gen::master_service::{TFinishTaskRequest, TMasterResult, TReportRequest};

use crate::fe_client::{FeClient, FeError};
use crate::status;

/// A master FE that records what the BE sends and answers OK. It can fail the next calls, or
/// hold `finishTask` calls until released.
#[derive(Default)]
pub struct FakeFe {
    finishes: Mutex<Vec<TFinishTaskRequest>>,
    reports: Mutex<Vec<TReportRequest>>,
    attempts: AtomicUsize,
    failures: AtomicUsize,
    blocked: Mutex<bool>,
    unblocked: Condvar,
}

impl FakeFe {
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
}
