//! A running BE: storage opened, the four ports listening, reports and agent workers started.

use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

use doris_storage::{DataDir, StorageError, TabletManager, TabletOptions};
use doris_thrift::gen::backend_service::BackendServiceSyncProcessor;
use doris_thrift::gen::heartbeat_service::HeartbeatServiceSyncProcessor;
use thrift::protocol::{TBinaryInputProtocol, TBinaryOutputProtocol};
use thrift::server::TProcessor;
use thrift::transport::{TBufferedReadTransport, TBufferedWriteTransport};

use crate::agent::AgentService;
use crate::backend_service::BackendHandler;
use crate::config::BeConfig;
use crate::fe_client::ThriftFeClient;
use crate::heartbeat::HeartbeatHandler;
use crate::master::MasterInfo;
use crate::report::{Reporter, ReporterHandle};

#[derive(Debug, thiserror::Error)]
pub enum BeError {
    #[error("{0}")]
    Storage(#[from] StorageError),
    #[error("cannot listen on port {port}: {source}")]
    Listen { port: u16, source: std::io::Error },
    #[error("cannot start the async runtime: {0}")]
    Runtime(std::io::Error),
}

pub struct Be {
    stop: Arc<AtomicBool>,
    thrift: Vec<(SocketAddr, JoinHandle<()>)>,
    runtime: Option<tokio::runtime::Runtime>,
    _reporter: ReporterHandle,
}

fn bind(port: u16) -> Result<TcpListener, BeError> {
    TcpListener::bind(("0.0.0.0", port)).map_err(|source| BeError::Listen { port, source })
}

impl Be {
    pub fn start(cfg: BeConfig) -> Result<Be, BeError> {
        let host = cfg.host_or_default();
        let dirs = cfg.storage_root_paths.iter().map(|root| DataDir::open(root, &host)).collect::<Result<Vec<_>, _>>()?;
        let tablets = Arc::new(Mutex::new(TabletManager::open(dirs, TabletOptions { sync_tablet_meta: cfg.sync_tablet_meta })?));
        let master = MasterInfo::new(&host);
        let fe = Arc::new(ThriftFeClient::new(master.clone()));
        let agent = AgentService::new(&cfg, tablets.clone(), master.clone(), fe.clone());
        let reporter = Reporter::new(&cfg, tablets.clone(), agent.clone(), fe, master.clone());

        let heartbeat_listener = bind(cfg.heartbeat_service_port)?;
        let be_listener = bind(cfg.be_port)?;
        let brpc_listener = bind(cfg.brpc_port)?;
        let http_listener = bind(cfg.webserver_port)?;

        let stop = Arc::new(AtomicBool::new(false));
        let heartbeat = HeartbeatServiceSyncProcessor::new(HeartbeatHandler::new(&cfg, master.clone(), tablets.clone()));
        let backend = BackendServiceSyncProcessor::new(BackendHandler::new(agent, tablets, master));
        let thrift = vec![
            serve_thrift("heartbeat service", heartbeat_listener, Arc::new(heartbeat), stop.clone()),
            serve_thrift("backend service", be_listener, Arc::new(backend), stop.clone()),
        ];

        let runtime = tokio::runtime::Builder::new_multi_thread().enable_all().build().map_err(BeError::Runtime)?;
        for (name, listener) in [("brpc", &brpc_listener), ("webserver", &http_listener)] {
            eprintln!("doris-rs be: {name} listening on {}", listener.local_addr().map(|a| a.to_string()).unwrap_or_default());
        }
        runtime.spawn(async move {
            brpc_listener.set_nonblocking(true).ok();
            if let Ok(l) = tokio::net::TcpListener::from_std(brpc_listener) {
                crate::brpc::serve(l).await;
            }
        });
        runtime.spawn(async move {
            http_listener.set_nonblocking(true).ok();
            if let Ok(l) = tokio::net::TcpListener::from_std(http_listener) {
                crate::http::serve(l).await;
            }
        });
        Ok(Be { stop, thrift, runtime: Some(runtime), _reporter: reporter.spawn() })
    }

    /// Stops listening and stops the background threads.
    pub fn shutdown(mut self) {
        self.stop.store(true, Ordering::SeqCst);
        for (addr, handle) in self.thrift.drain(..) {
            // wake the accept loop
            let _ = TcpStream::connect(("127.0.0.1", addr.port()));
            let _ = handle.join();
        }
        if let Some(rt) = self.runtime.take() {
            rt.shutdown_background();
        }
    }
}

/// One thread accepting, one thread per connection; strict binary protocol, unframed.
fn serve_thrift<P: TProcessor + Send + Sync + 'static>(
    name: &str,
    listener: TcpListener,
    processor: Arc<P>,
    stop: Arc<AtomicBool>,
) -> (SocketAddr, JoinHandle<()>) {
    let addr = listener.local_addr().expect("bound address");
    eprintln!("doris-rs be: {name} listening on {addr}");
    let handle = std::thread::Builder::new()
        .name(name.to_string())
        .spawn(move || {
            for stream in listener.incoming() {
                if stop.load(Ordering::SeqCst) {
                    return;
                }
                let Ok(stream) = stream else { continue };
                let processor = processor.clone();
                std::thread::spawn(move || {
                    let Ok(read) = stream.try_clone() else { return };
                    let mut i = TBinaryInputProtocol::new(TBufferedReadTransport::new(read), true);
                    let mut o = TBinaryOutputProtocol::new(TBufferedWriteTransport::new(stream), true);
                    while processor.process(&mut i, &mut o).is_ok() {}
                });
            }
        })
        .expect("spawn thrift server");
    (addr, handle)
}

/// Starts the BE and serves until the process ends.
pub fn run(cfg: BeConfig) -> Result<(), BeError> {
    let _be = Be::start(cfg)?;
    loop {
        std::thread::park();
    }
}
