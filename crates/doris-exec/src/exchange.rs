//! In-process exchanges ([wire] §2): the C++ BE hands blocks to a local receiver without an RPC
//! when sender and receiver share a BE, and doris-rs is the only BE. A receiver is keyed by
//! query, destination instance and exchange node; it ends once every sender has ended, or when
//! the query is cancelled.
use std::collections::HashMap;
use std::sync::{Condvar, Mutex};

use crate::block::Block;
use crate::ExecError;

pub type QueryId = (i64, i64);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ExchangeKey {
    pub query: QueryId,
    /// The receiving fragment instance.
    pub instance: (i64, i64),
    /// The exchange node.
    pub node: i32,
}

/// Rows and the slot ids of their columns.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Chunk {
    pub block: Block,
    pub slots: Vec<i32>,
}

#[derive(Default)]
struct Channel {
    chunks: Vec<Chunk>,
    ends: usize,
}

#[derive(Default)]
struct State {
    channels: HashMap<ExchangeKey, Channel>,
    cancelled: HashMap<QueryId, String>,
}

#[derive(Default)]
pub struct Exchanges {
    state: Mutex<State>,
    changed: Condvar,
}

impl Exchanges {
    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub fn send(&self, key: &ExchangeKey, chunk: Chunk) {
        let mut s = self.lock();
        if s.cancelled.contains_key(&key.query) {
            return;
        }
        s.channels.entry(key.clone()).or_default().chunks.push(chunk);
        self.changed.notify_all();
    }

    /// One sender is done.
    pub fn end(&self, key: &ExchangeKey) {
        let mut s = self.lock();
        s.channels.entry(key.clone()).or_default().ends += 1;
        self.changed.notify_all();
    }

    /// Every chunk sent to the receiver, once `senders` senders have ended.
    pub fn receive(&self, key: &ExchangeKey, senders: usize) -> Result<Vec<Chunk>, ExecError> {
        let mut s = self.lock();
        loop {
            if let Some(why) = s.cancelled.get(&key.query) {
                return Err(ExecError::Cancelled(why.clone()));
            }
            if s.channels.get(key).is_some_and(|c| c.ends >= senders) {
                return Ok(s.channels.remove(key).map(|c| c.chunks).unwrap_or_default());
            }
            s = self.changed.wait(s).unwrap_or_else(|e| e.into_inner());
        }
    }

    /// Ends every receiver of the query, now and later, with `why`.
    pub fn cancel(&self, query: QueryId, why: String) {
        let mut s = self.lock();
        s.cancelled.entry(query).or_insert(why);
        s.channels.retain(|k, _| k.query != query);
        self.changed.notify_all();
    }

    /// Channels and cancellations held, for checks that a query left nothing behind.
    pub fn entries(&self) -> usize {
        let s = self.lock();
        s.channels.len() + s.cancelled.len()
    }

    /// Drops what is left of a finished query.
    pub fn forget(&self, query: QueryId) {
        let mut s = self.lock();
        s.cancelled.remove(&query);
        s.channels.retain(|k, _| k.query != query);
    }
}
