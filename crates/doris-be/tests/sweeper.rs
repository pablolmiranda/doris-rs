use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use doris_be::config::BeConfig;
use doris_be::sweeper;
use doris_fixtures::Fixtures;
use doris_storage::{DataDir, TabletManager, TabletOptions};
use doris_thrift::gen::agent_service::TDropTabletReq;

fn manager(root: &std::path::Path) -> Arc<Mutex<TabletManager>> {
    Arc::new(Mutex::new(TabletManager::open(vec![DataDir::open(root, "127.0.0.1").unwrap()], TabletOptions::default()).unwrap()))
}

fn create_and_drop(tablets: &Mutex<TabletManager>) -> i64 {
    let fx = Fixtures::m2();
    let req = fx.create_requests().into_iter().next().unwrap();
    let mut t = tablets.lock().unwrap();
    t.create_tablet(&req).unwrap();
    let drop = TDropTabletReq { tablet_id: req.tablet_id, schema_hash: None, replica_id: None, is_drop_table_or_partition: Some(true) };
    t.drop_tablet(&drop).unwrap();
    req.tablet_id
}

fn swept_within(tablets: &Mutex<TabletManager>, limit: Duration) -> bool {
    let deadline = Instant::now() + limit;
    while Instant::now() < deadline {
        if tablets.lock().unwrap().shutdown_tablets().is_empty() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    false
}

#[test]
fn the_sweeper_runs_at_start() {
    // _garbage_sweeper_thread_callback sweeps first, then waits min_garbage_sweep_interval
    let tmp = tempfile::tempdir().unwrap();
    let tablets = manager(tmp.path());
    create_and_drop(&tablets);
    let _h = sweeper::spawn(tablets.clone(), &BeConfig { min_garbage_sweep_interval: 3600, ..BeConfig::default() });
    assert!(swept_within(&tablets, Duration::from_secs(5)));
}

#[test]
fn the_sweeper_runs_every_interval() {
    let tmp = tempfile::tempdir().unwrap();
    let tablets = manager(tmp.path());
    let _h = sweeper::spawn(tablets.clone(), &BeConfig { min_garbage_sweep_interval: 1, ..BeConfig::default() });
    std::thread::sleep(Duration::from_millis(200));
    create_and_drop(&tablets);
    assert!(!tablets.lock().unwrap().shutdown_tablets().is_empty());
    assert!(swept_within(&tablets, Duration::from_secs(5)));
}
