use std::path::PathBuf;

use doris_be::config::BeConfig;

/// The `BE_CONFIG_EXTRA` block of the reference compose file: the be.conf lines the reference runs with.
fn reference_be_conf_extra() -> String {
    let compose = include_str!("../../../oracle/compose/reference-single.yml");
    let mut lines = compose.lines().skip_while(|l| !l.trim_start().starts_with("BE_CONFIG_EXTRA: |"));
    let header = lines.next().expect("BE_CONFIG_EXTRA block");
    let indent = header.len() - header.trim_start().len();
    lines
        .take_while(|l| l.len() - l.trim_start().len() > indent)
        .map(|l| format!("{}\n", l.trim()))
        .collect()
}

#[test]
fn defaults_are_the_cpp_be_defaults() {
    let cfg = BeConfig::parse("").unwrap();
    assert_eq!(cfg.heartbeat_service_port, 9050);
    assert_eq!(cfg.be_port, 9060);
    assert_eq!(cfg.brpc_port, 8060);
    assert_eq!(cfg.webserver_port, 8040);
    assert_eq!(cfg.report_task_interval_seconds, 10);
    assert_eq!(cfg.report_disk_state_interval_seconds, 30);
    assert_eq!(cfg.report_tablet_interval_seconds, 60);
    assert_eq!(cfg.host, None);
    assert_eq!(cfg.priority_networks, None);
    assert!(!cfg.sync_tablet_meta);
}

#[test]
fn sync_tablet_meta_is_read() {
    // config.cpp: DEFINE_mBool(sync_tablet_meta, "false"); strtox takes exactly true or false
    assert!(BeConfig::parse("sync_tablet_meta = true").unwrap().sync_tablet_meta);
    assert!(!BeConfig::parse("sync_tablet_meta = false").unwrap().sync_tablet_meta);
    assert!(BeConfig::parse("sync_tablet_meta = 1").is_err());
}

#[test]
fn reads_a_reference_be_conf() {
    let text = format!(
        "# a comment\n{}storage_root_path = /a;/b,medium:SSD\nbe_port = 19060\n",
        reference_be_conf_extra()
    );
    let cfg = BeConfig::parse(&text).unwrap();
    assert_eq!(cfg.storage_root_paths, vec![PathBuf::from("/a"), PathBuf::from("/b")]);
    assert_eq!(cfg.be_port, 19060);
    assert_eq!(cfg.heartbeat_service_port, 9050);
}

#[test]
fn rejects_a_non_numeric_port() {
    let err = BeConfig::parse("webserver_port = eighty").unwrap_err();
    assert!(err.to_string().contains("webserver_port"), "{err}");
}
