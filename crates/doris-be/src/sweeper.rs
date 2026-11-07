//! The trash sweep thread (`StorageEngine::_garbage_sweeper_thread_callback`): one sweep as soon
//! as it starts, then one every `min_garbage_sweep_interval` seconds. The C++ BE computes an
//! interval from disk usage between the min and max settings but never uses it ("TODO(dx): fix
//! it"), so the min is the interval.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use doris_storage::{SweepOptions, TabletManager};

use crate::config::BeConfig;

/// The running sweep thread; dropping it stops it.
pub struct SweeperHandle {
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl Drop for SweeperHandle {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

pub fn spawn(tablets: Arc<Mutex<TabletManager>>, cfg: &BeConfig) -> SweeperHandle {
    let stop = Arc::new(AtomicBool::new(false));
    let interval = Duration::from_secs(cfg.min_garbage_sweep_interval.max(1));
    let opts = SweepOptions { trash_expire_secs: cfg.trash_file_expire_time_sec, snapshot_expire_secs: cfg.snapshot_expire_time_sec };
    let flag = stop.clone();
    let thread = std::thread::Builder::new()
        .name("garbage_sweeper".into())
        .spawn(move || {
            while !flag.load(Ordering::SeqCst) {
                let result = tablets.lock().unwrap_or_else(|e| e.into_inner()).start_trash_sweep(opts);
                if let Err(e) = result {
                    eprintln!("doris-rs be: trash sweep: {e}");
                }
                let deadline = Instant::now() + interval;
                while Instant::now() < deadline && !flag.load(Ordering::SeqCst) {
                    std::thread::sleep(Duration::from_millis(50));
                }
            }
        })
        .expect("spawn the trash sweeper");
    SweeperHandle { stop, thread: Some(thread) }
}
