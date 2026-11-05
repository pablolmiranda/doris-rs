//! The load error log (`RuntimeState::append_error_msg_to_file`, `LoadPathMgr`): one line per
//! filtered row, `Reason: <msg>. src line [<line>]; `, at most 50 rows, in
//! `<root>/error_log/__shard_<n>/<name>`; the FE shows its URL to the user.
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::RowError;

/// `MAX_ERROR_NUM`: rows printed to the log (the count goes on).
pub const MAX_ERROR_ROWS: usize = 50;
/// `LoadPathMgr::MAX_SHARD_NUM`.
const SHARDS: u64 = 128;
static NEXT_SHARD: AtomicU64 = AtomicU64::new(0);

/// Writes the log under `dir` and returns its name relative to `dir`
/// (`__shard_<n>/error_log_<db>_<label>_<instance hi hex>_<instance lo hex>`).
pub fn write_error_log(dir: &Path, db: &str, label: &str, instance: (i64, i64), errors: &[RowError]) -> std::io::Result<String> {
    let shard = format!("__shard_{}", NEXT_SHARD.fetch_add(1, Ordering::Relaxed) % SHARDS);
    std::fs::create_dir_all(dir.join(&shard))?;
    let name = format!("{shard}/error_log_{db}_{label}_{:x}_{:x}", instance.0, instance.1);
    let mut text = String::new();
    for e in errors.iter().take(MAX_ERROR_ROWS + 1) {
        text.push_str(&format!("Reason: {}. src line [{}]; \n", e.msg, e.line));
    }
    std::fs::write(dir.join(&name), text)?;
    Ok(name)
}
