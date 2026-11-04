//! Reports this BE sends the master FE (`FrontendService.report`), as the C++ report workers
//! do (`be/src/agent/task_worker_pool.cpp`, `ReportWorker`): tasks, disks, tablets and index
//! policies, each on its own interval, all at once when a heartbeat changes the master, and
//! none before the first heartbeat.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use doris_storage::TabletManager;
use doris_thrift::gen::master_service::{TDisk, TReportRequest};
use doris_thrift::gen::types::{TBackend, TStorageMedium};

use crate::agent::AgentService;
use crate::config::BeConfig;
use crate::fe_client::FeClient;
use crate::master::MasterInfo;

/// `config::report_index_policy_interval_seconds`.
const INDEX_POLICY_INTERVAL: Duration = Duration::from_secs(10);

#[derive(Clone)]
pub struct Reporter {
    tablets: Arc<Mutex<TabletManager>>,
    agent: AgentService,
    fe: Arc<dyn FeClient>,
    master: MasterInfo,
    ports: (i32, i32, i32),
    intervals: [Duration; 3],
}

/// The running report threads; dropping it stops them.
pub struct ReporterHandle {
    stop: Arc<AtomicBool>,
    threads: Vec<JoinHandle<()>>,
}

impl Drop for ReporterHandle {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        for t in self.threads.drain(..) {
            let _ = t.join();
        }
    }
}

#[derive(Clone, Copy)]
enum Kind {
    Task,
    Disk,
    Tablet,
    IndexPolicy,
}

impl Reporter {
    pub fn new(
        cfg: &BeConfig,
        tablets: Arc<Mutex<TabletManager>>,
        agent: AgentService,
        fe: Arc<dyn FeClient>,
        master: MasterInfo,
    ) -> Reporter {
        Reporter {
            tablets,
            agent,
            fe,
            master,
            ports: (i32::from(cfg.be_port), i32::from(cfg.webserver_port), i32::from(cfg.brpc_port)),
            intervals: [
                Duration::from_secs(cfg.report_task_interval_seconds),
                Duration::from_secs(cfg.report_disk_state_interval_seconds),
                Duration::from_secs(cfg.report_tablet_interval_seconds),
            ],
        }
    }

    pub fn spawn(&self) -> ReporterHandle {
        let stop = Arc::new(AtomicBool::new(false));
        let kinds = [
            (Kind::Task, self.intervals[0]),
            (Kind::Disk, self.intervals[1]),
            (Kind::Tablet, self.intervals[2]),
            (Kind::IndexPolicy, INDEX_POLICY_INTERVAL),
        ];
        let threads = kinds
            .into_iter()
            .map(|(kind, interval)| {
                let me = self.clone();
                let stop = stop.clone();
                std::thread::Builder::new().name("report".into()).spawn(move || me.run(kind, interval, &stop)).expect("spawn reporter")
            })
            .collect();
        ReporterHandle { stop, threads }
    }

    /// `ReportWorker`'s loop: wait for the interval or a heartbeat's signal, then report if a
    /// master is known.
    fn run(&self, kind: Kind, interval: Duration, stop: &AtomicBool) {
        let mut seen = self.master.report_signal();
        while !stop.load(Ordering::SeqCst) {
            let deadline = Instant::now() + interval;
            while Instant::now() < deadline && !stop.load(Ordering::SeqCst) {
                let signal = self.master.report_signal();
                if signal != seen {
                    seen = signal;
                    break;
                }
                std::thread::sleep(Duration::from_millis(50));
            }
            if stop.load(Ordering::SeqCst) || self.master.fe_address().is_none() {
                continue;
            }
            let req = match kind {
                Kind::Task => self.build_task_report(),
                Kind::Disk => self.build_disk_report(),
                Kind::Tablet => self.build_tablet_report(),
                Kind::IndexPolicy => self.build_index_policy_report(),
            };
            let _ = self.fe.report(req);
        }
    }

    fn tablets(&self) -> std::sync::MutexGuard<'_, TabletManager> {
        self.tablets.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// `BackendOptions::get_local_backend()`.
    fn backend(&self) -> TBackend {
        let (be_port, http_port, brpc_port) = self.ports;
        TBackend {
            host: self.master.localhost(),
            be_port,
            http_port,
            brpc_port: Some(brpc_port),
            is_alive: None,
            id: self.master.backend_id(),
        }
    }

    /// A report with only the backend set; `num_cores` and `pipeline_executor_size` have no
    /// requiredness keyword, so the C++ BE always writes them (0 unless set).
    fn empty(&self) -> TReportRequest {
        TReportRequest {
            backend: self.backend(),
            report_version: None,
            tasks: None,
            tablets: None,
            disks: None,
            force_recovery: None,
            tablet_list: None,
            tablet_max_compaction_score: None,
            storage_policy: None,
            resource: None,
            num_cores: Some(0),
            pipeline_executor_size: Some(0),
            partitions_version: None,
            num_tablets: None,
            index_policy: None,
            running_tasks: None,
        }
    }

    /// `report_task_callback`.
    pub fn build_task_report(&self) -> TReportRequest {
        TReportRequest {
            tasks: Some(self.agent.running_tasks()),
            // FragmentMgr::running_query_num(): no queries run in M2
            running_tasks: Some(0),
            ..self.empty()
        }
    }

    /// `report_disk_callback`.
    pub fn build_disk_report(&self) -> TReportRequest {
        let tablets = self.tablets();
        let mut disks = std::collections::BTreeMap::new();
        for (i, dir) in tablets.data_dirs().iter().enumerate() {
            let path = dir.root().display().to_string();
            let (total, available) = dir.capacity().unwrap_or((0, 0));
            let disk = TDisk {
                root_path: path.clone(),
                disk_total_capacity: total,
                data_used_capacity: tablets.data_used(i),
                used: true,
                disk_available_capacity: Some(available),
                path_hash: Some(dir.path_hash()),
                storage_medium: Some(TStorageMedium::HDD),
                remote_used_capacity: Some(0),
                trash_used_capacity: Some(dir.trash_used()),
            };
            disks.insert(path, disk);
        }
        let cores = num_cores();
        TReportRequest {
            disks: Some(disks),
            num_cores: Some(cores),
            pipeline_executor_size: Some(cores),
            ..self.empty()
        }
    }

    /// `report_tablet_callback`.
    pub fn build_tablet_report(&self) -> TReportRequest {
        let mut tablets = self.tablets();
        tablets.increase_report_version();
        let report = tablets.report_tablets();
        TReportRequest {
            report_version: Some(tablets.report_version()),
            tablets: Some(report),
            partitions_version: Some(tablets.partitions_version()),
            // no compaction runs, so the max compaction score metric stays 0
            tablet_max_compaction_score: Some(0),
            storage_policy: Some(vec![]),
            resource: Some(vec![]),
            ..self.empty()
        }
    }

    /// `report_index_policy_callback`: no index policies are held in M2.
    pub fn build_index_policy_report(&self) -> TReportRequest {
        TReportRequest { index_policy: Some(vec![]), ..self.empty() }
    }
}

/// `CpuInfo::num_cores()`.
fn num_cores() -> i32 {
    std::thread::available_parallelism().map(|n| n.get() as i32).unwrap_or(1)
}
