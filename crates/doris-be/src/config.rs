//! The BE configuration, read from a file in the C++ `be.conf` format (`key = value` lines, `#`
//! comments, the C++ key names), so an existing BE's `be.conf` works unchanged. Keys doris-rs does
//! not use are accepted and ignored, as the C++ BE ignores keys it does not know.

use std::path::PathBuf;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BeConfig {
    pub be_port: u16,
    pub heartbeat_service_port: u16,
    pub brpc_port: u16,
    pub webserver_port: u16,
    /// `storage_root_path`: `;`-separated roots; `,medium:..` and `,capacity:..` suffixes are
    /// accepted and not used yet.
    pub storage_root_paths: Vec<PathBuf>,
    pub priority_networks: Option<String>,
    pub report_task_interval_seconds: u64,
    pub report_disk_state_interval_seconds: u64,
    pub report_tablet_interval_seconds: u64,
    /// The address this BE registers as (a doris-rs key; the C++ BE derives it from
    /// `priority_networks`). Unset: see [`BeConfig::host_or_default`].
    pub host: Option<String>,
    /// `sync_tablet_meta`: sync the metadata store's log on every header change.
    pub sync_tablet_meta: bool,
    /// The trash sweep's interval (`min_garbage_sweep_interval`; `max_garbage_sweep_interval`
    /// is read by the C++ BE but its computed interval is never used).
    pub min_garbage_sweep_interval: u64,
    pub trash_file_expire_time_sec: i64,
    pub snapshot_expire_time_sec: i64,
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("be.conf line {line}: `{key}` must be {expected}, got `{value}`")]
    BadValue { line: usize, key: String, value: String, expected: &'static str },
    #[error("be.conf line {line}: expected `key = value`, got `{text}`")]
    BadLine { line: usize, text: String },
    #[error("cannot read {path}: {source}")]
    Read { path: PathBuf, source: std::io::Error },
}

impl Default for BeConfig {
    /// The C++ BE's defaults (`be/src/common/config.cpp`).
    fn default() -> Self {
        let home = std::env::var("DORIS_HOME").unwrap_or_else(|_| ".".into());
        BeConfig {
            be_port: 9060,
            heartbeat_service_port: 9050,
            brpc_port: 8060,
            webserver_port: 8040,
            storage_root_paths: vec![PathBuf::from(home).join("storage")],
            priority_networks: None,
            report_task_interval_seconds: 10,
            report_disk_state_interval_seconds: 30,
            report_tablet_interval_seconds: 60,
            host: None,
            sync_tablet_meta: false,
            min_garbage_sweep_interval: 180,
            trash_file_expire_time_sec: 0,
            snapshot_expire_time_sec: 172_800,
        }
    }
}

impl BeConfig {
    pub fn parse(text: &str) -> Result<BeConfig, ConfigError> {
        let mut cfg = BeConfig::default();
        for (i, raw) in text.lines().enumerate() {
            let line = i + 1;
            let trimmed = raw.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                continue;
            }
            let Some((key, value)) = trimmed.split_once('=') else {
                return Err(ConfigError::BadLine { line, text: trimmed.to_string() });
            };
            let (key, value) = (key.trim(), value.trim());
            let port = |k: &str| {
                value.parse::<u16>().map_err(|_| ConfigError::BadValue {
                    line,
                    key: k.to_string(),
                    value: value.to_string(),
                    expected: "a port number",
                })
            };
            let secs = |k: &str| {
                value.parse::<u64>().map_err(|_| ConfigError::BadValue {
                    line,
                    key: k.to_string(),
                    value: value.to_string(),
                    expected: "a number of seconds",
                })
            };
            // strtox(bool): exactly "true" or "false"
            let boolean = |key: &str| match value {
                "true" => Ok(true),
                "false" => Ok(false),
                _ => Err(ConfigError::BadValue { line, key: key.to_string(), value: value.to_string(), expected: "true or false" }),
            };
            match key {
                "be_port" => cfg.be_port = port(key)?,
                "heartbeat_service_port" => cfg.heartbeat_service_port = port(key)?,
                "brpc_port" => cfg.brpc_port = port(key)?,
                "webserver_port" => cfg.webserver_port = port(key)?,
                "storage_root_path" => cfg.storage_root_paths = storage_roots(value),
                "priority_networks" => cfg.priority_networks = Some(value.to_string()).filter(|v| !v.is_empty()),
                "report_task_interval_seconds" => cfg.report_task_interval_seconds = secs(key)?,
                "report_disk_state_interval_seconds" => cfg.report_disk_state_interval_seconds = secs(key)?,
                "report_tablet_interval_seconds" => cfg.report_tablet_interval_seconds = secs(key)?,
                "host" => cfg.host = Some(value.to_string()).filter(|v| !v.is_empty()),
                "sync_tablet_meta" => cfg.sync_tablet_meta = boolean(key)?,
                "min_garbage_sweep_interval" => cfg.min_garbage_sweep_interval = secs(key)?,
                "trash_file_expire_time_sec" => cfg.trash_file_expire_time_sec = secs(key)? as i64,
                "snapshot_expire_time_sec" => cfg.snapshot_expire_time_sec = secs(key)? as i64,
                _ => {}
            }
        }
        Ok(cfg)
    }

    pub fn load(path: &std::path::Path) -> Result<BeConfig, ConfigError> {
        let text = std::fs::read_to_string(path)
            .map_err(|source| ConfigError::Read { path: path.to_path_buf(), source })?;
        BeConfig::parse(&text)
    }

    /// The address this BE registers as: `host`, else `127.0.0.1`. Picking a local address
    /// from `priority_networks` as the C++ BE does is not implemented yet; set `host`.
    pub fn host_or_default(&self) -> String {
        self.host.clone().unwrap_or_else(|| "127.0.0.1".to_string())
    }
}

fn storage_roots(value: &str) -> Vec<PathBuf> {
    let home = std::env::var("DORIS_HOME").unwrap_or_else(|_| ".".into());
    value
        .split(';')
        .map(str::trim)
        .filter(|e| !e.is_empty())
        .map(|e| e.split(',').next().unwrap_or(e).trim())
        .map(|p| PathBuf::from(p.replace("${DORIS_HOME}", &home)))
        .collect()
}
