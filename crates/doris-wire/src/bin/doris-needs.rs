//! `doris-needs [CORPUS_DIR]`: the needs index of a per-suite traffic corpus as JSON on stdout,
//! `{suite: {kind: [names]}}`. The default corpus is `oracle/epic1/traffic`.
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::ExitCode;

use doris_wire::needs::needs_of;
use doris_wire::suites::{corpus_dir, read_suite, suite_files};

fn main() -> ExitCode {
    let dir = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(corpus_dir);
    let mut index = BTreeMap::new();
    let mut errors = 0;
    for path in suite_files(&dir) {
        let name = path.file_name().unwrap().to_string_lossy();
        let suite = name.trim_end_matches(".bin.gz").to_string();
        match read_suite(&path)
            .map_err(|e| e.to_string())
            .and_then(|r| needs_of(&r))
        {
            Ok(needs) => {
                index.insert(suite, needs);
            }
            Err(e) => {
                eprintln!("{suite}: {e}");
                errors += 1;
            }
        }
    }
    println!(
        "{}",
        serde_json::to_string_pretty(&index).expect("serializable")
    );
    if errors > 0 {
        eprintln!("{errors} suites could not be indexed");
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}
