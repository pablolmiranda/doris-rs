use std::sync::{Arc, Mutex};
use std::time::Duration;

use doris_be::agent::AgentService;
use doris_be::backend_service::BackendHandler;
use doris_be::config::BeConfig;
use doris_be::heartbeat::HeartbeatHandler;
use doris_be::master::MasterInfo;
use doris_be::report::Reporter;
use doris_be::testing::FakeFe;
use doris_fixtures::{decode, Fixtures};
use doris_storage::{DataDir, TabletManager, TabletOptions};
use doris_thrift::gen::backend_service::{
    BackendServiceGetDictionaryStatusArgs, BackendServiceGetDictionaryStatusResult, BackendServiceGetTabletStatResult,
    BackendServicePublishTopicInfoArgs, BackendServicePublishTopicInfoResult, BackendServiceSubmitTasksArgs,
    BackendServiceSyncHandler,
};
use doris_thrift::gen::frontend_service::FrontendServiceReportArgs;
use doris_thrift::gen::heartbeat_service::{HeartbeatServiceHeartbeatArgs, HeartbeatServiceSyncHandler};
use doris_thrift::gen::master_service::TReportRequest;

struct Be {
    reporter: Reporter,
    handler: BackendHandler,
    agent: AgentService,
    heartbeat: HeartbeatHandler,
    _tmp: tempfile::TempDir,
}

fn be() -> Be {
    let tmp = tempfile::tempdir().unwrap();
    let tablets = Arc::new(Mutex::new(TabletManager::open(vec![DataDir::open(tmp.path(), "127.0.0.1").unwrap()], TabletOptions::default()).unwrap()));
    let master = MasterInfo::new("127.0.0.1");
    let fe = Arc::new(FakeFe::default());
    let cfg = BeConfig::default();
    let agent = AgentService::new(&cfg, tablets.clone(), master.clone(), fe.clone());
    let reporter = Reporter::new(&cfg, tablets.clone(), agent.clone(), fe, master.clone());
    let handler = BackendHandler::new(agent.clone(), tablets.clone(), master.clone());
    let heartbeat = HeartbeatHandler::new(&cfg, master, tablets);
    Be { reporter, handler, agent, heartbeat, _tmp: tmp }
}

/// The fields of a report that depend on the deployment or the clock, overwritten from the
/// expected report.
fn mask(got: &mut TReportRequest, expected: &TReportRequest) {
    // BackendOptions::get_localhost(): fact be-host
    got.backend.host = expected.backend.host.clone();
    // s_report_version: time(nullptr) * 100000 at start, plus changes; check the scale, then
    // take the reference's value
    if let Some(v) = got.report_version {
        let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() as i64;
        assert!((v / 100_000 - now).abs() <= 60, "report_version {v} is not time(nullptr) * 100000 scale");
    }
    got.report_version = expected.report_version;
    // CpuInfo::num_cores() and pipeline_executor_size: facts be-cores
    got.num_cores = expected.num_cores;
    got.pipeline_executor_size = expected.pipeline_executor_size;
    for (g, e) in got.tablets.iter_mut().flatten().zip(expected.tablets.iter().flatten()) {
        for (gi, ei) in g.1.tablet_infos.iter_mut().zip(&e.1.tablet_infos) {
            // DataDir::path_hash(): hash_of_path(host, root), fact be-data-path
            gi.path_hash = ei.path_hash;
        }
    }
    if let (Some(g), Some(e)) = (&mut got.disks, &expected.disks) {
        // one root each; its path is fact be-data-path and its sizes are the host's disk
        let (gd, (ek, ed)) = (g.values().next().unwrap().clone(), e.iter().next().unwrap());
        let mut d = gd;
        d.root_path = ed.root_path.clone();
        d.path_hash = ed.path_hash;
        d.disk_total_capacity = ed.disk_total_capacity;
        d.data_used_capacity = ed.data_used_capacity;
        d.disk_available_capacity = ed.disk_available_capacity;
        d.remote_used_capacity = ed.remote_used_capacity;
        d.trash_used_capacity = ed.trash_used_capacity;
        *g = [(ek.clone(), d)].into();
    }
}

/// A tablet report whose tablets all still hold only their initial rowset: no data loaded.
fn holds_no_data(r: &TReportRequest) -> bool {
    r.tablets.iter().flatten().all(|(_, t)| t.tablet_infos.iter().all(|i| i.row_count == 0 && i.version <= 1))
}

#[test]
fn reports_match_the_reference() {
    let fx = Fixtures::m2();
    let be = be();
    let (mut tablet_reports, mut disk_reports, mut task_reports, mut policy_reports) = (0, 0, 0, 0);
    for entry in fx.entries() {
        let bytes = fx.bytes(entry);
        match (entry.method.as_str(), entry.kind.as_str()) {
            ("heartbeat", "call") => {
                let info = decode(&bytes, HeartbeatServiceHeartbeatArgs::read_from_in_protocol).master_info;
                be.heartbeat.handle_heartbeat(info).unwrap();
            }
            ("submit_tasks", "call") => {
                let tasks = decode(&bytes, BackendServiceSubmitTasksArgs::read_from_in_protocol).tasks;
                be.agent.submit_tasks(tasks);
                assert!(be.agent.wait_idle(Duration::from_secs(10)));
            }
            ("report", "call") => {
                let expected = decode(&bytes, FrontendServiceReportArgs::read_from_in_protocol).request;
                let mut got = if expected.tablets.is_some() {
                    let got = be.reporter.build_tablet_report();
                    if !holds_no_data(&expected) {
                        continue;
                    }
                    tablet_reports += 1;
                    got
                } else if expected.disks.is_some() {
                    disk_reports += 1;
                    be.reporter.build_disk_report()
                } else if expected.tasks.is_some() {
                    task_reports += 1;
                    be.reporter.build_task_report()
                } else {
                    policy_reports += 1;
                    be.reporter.build_index_policy_report()
                };
                mask(&mut got, &expected);
                assert_eq!(got, expected, "{}", entry.file);
            }
            _ => {}
        }
    }
    assert!(tablet_reports >= 1 && disk_reports >= 1 && task_reports >= 3 && policy_reports >= 3);
}

#[test]
fn background_calls_match_the_reference() {
    let fx = Fixtures::m2();
    let be = be();
    let (mut stats, mut topics, mut dicts) = (0, 0, 0);
    for entry in fx.entries() {
        let bytes = fx.bytes(entry);
        match (entry.method.as_str(), entry.kind.as_str()) {
            ("heartbeat", "call") => {
                let info = decode(&bytes, HeartbeatServiceHeartbeatArgs::read_from_in_protocol).master_info;
                be.heartbeat.handle_heartbeat(info).unwrap();
            }
            ("submit_tasks", "call") => {
                let tasks = decode(&bytes, BackendServiceSubmitTasksArgs::read_from_in_protocol).tasks;
                be.agent.submit_tasks(tasks);
                assert!(be.agent.wait_idle(Duration::from_secs(10)));
            }
            ("report", "call") => {
                let expected = decode(&bytes, FrontendServiceReportArgs::read_from_in_protocol).request;
                if expected.tablets.is_some() {
                    be.reporter.build_tablet_report(); // refreshes the stat cache
                }
            }
            ("get_tablet_stat", "call") => {
                stats += 1;
                let (_, reply) = fx.reply_to(entry).unwrap();
                let expected = decode(&reply, BackendServiceGetTabletStatResult::read_from_in_protocol).result_value.unwrap();
                let got = be.handler.handle_get_tablet_stat().unwrap();
                assert_eq!(got.tablets_stats, expected.tablets_stats);
                // Tablets the reference had loaded data into (the FE's audit log) are not
                // comparable; every other stat must be equal.
                let empty: Vec<_> = expected
                    .tablet_stat_list
                    .clone()
                    .unwrap()
                    .into_iter()
                    .filter(|s| s.row_count == Some(0) && s.visible_version == Some(1))
                    .collect();
                assert!(empty.len() >= 20);
                let mine = got.tablet_stat_list.unwrap();
                for e in empty {
                    let g = mine.iter().find(|s| s.tablet_id == e.tablet_id).expect("stat");
                    assert_eq!(g, &e);
                }
            }
            ("publish_topic_info", "call") => {
                topics += 1;
                let req = decode(&bytes, BackendServicePublishTopicInfoArgs::read_from_in_protocol).topic_request;
                let (_, reply) = fx.reply_to(entry).unwrap();
                let expected = decode(&reply, BackendServicePublishTopicInfoResult::read_from_in_protocol).result_value;
                assert_eq!(Some(be.handler.handle_publish_topic_info(req).unwrap()), expected);
            }
            ("get_dictionary_status", "call") => {
                dicts += 1;
                let ids = decode(&bytes, BackendServiceGetDictionaryStatusArgs::read_from_in_protocol).dictionary_ids;
                let (_, reply) = fx.reply_to(entry).unwrap();
                let expected = decode(&reply, BackendServiceGetDictionaryStatusResult::read_from_in_protocol).result_value;
                assert_eq!(Some(be.handler.handle_get_dictionary_status(ids).unwrap()), expected);
            }
            _ => {}
        }
    }
    assert!(stats >= 1 && topics >= 10 && dicts >= 5);
}
