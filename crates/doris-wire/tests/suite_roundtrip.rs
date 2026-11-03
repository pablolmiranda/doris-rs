//! Every FE↔BE message in the per-suite traffic corpus (`oracle/epic1/traffic`) decodes with the
//! Rust types and re-encodes to the captured bytes, under the same set/map-order rule as the
//! wire corpus. The fixture is written by the oracle's Python record writer, so the two sides
//! agree on the file format.
use std::path::Path;

use doris_wire::roundtrip::{check, Outcome};
use doris_wire::suites::{corpus_dir, read_suite, suite_files};

fn check_file(path: &Path, failures: &mut Vec<String>) -> (usize, usize) {
    let mut checked = 0;
    let mut reordered = 0;
    for (i, record) in read_suite(path)
        .expect("readable suite file")
        .iter()
        .enumerate()
    {
        match check(&record.header, &record.data) {
            Ok(Outcome::Identical) => checked += 1,
            Ok(Outcome::Reordered) => {
                checked += 1;
                reordered += 1;
            }
            Ok(Outcome::NotChecked) => {}
            Err(e) => failures.push(format!(
                "{} record {i} {}: {e}",
                path.display(),
                record.header
            )),
        }
    }
    (checked, reordered)
}

#[test]
fn fixture_written_by_python_reads_and_reencodes() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/suite.bin.gz");
    let records = read_suite(&path).expect("fixture");
    let methods: Vec<_> = records
        .iter()
        .map(|r| r.header["method"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(
        methods,
        [
            "heartbeat",
            "heartbeat",
            "exec_plan_fragment",
            "fetch_data",
            "submit_tasks",
            "stream_load"
        ]
    );
    let mut failures = Vec::new();
    let (checked, _) = check_file(&path, &mut failures);
    assert!(failures.is_empty(), "{failures:#?}");
    assert_eq!(
        checked, 5,
        "every Thrift and gRPC record is checked; HTTP is not"
    );
}

#[test]
fn every_suite_message_reencodes_identically() {
    let files = suite_files(&corpus_dir());
    if files.is_empty() {
        eprintln!(
            "no per-suite corpus in {} yet; run `make -C oracle epic-baseline`",
            corpus_dir().display()
        );
        return;
    }
    let mut failures = Vec::new();
    let (mut checked, mut reordered) = (0, 0);
    for f in &files {
        let (c, r) = check_file(f, &mut failures);
        checked += c;
        reordered += r;
    }
    eprintln!(
        "{checked} messages in {} suites re-encode ({reordered} up to set/map order)",
        files.len()
    );
    assert!(
        failures.is_empty(),
        "{} messages did not re-encode:\n{}",
        failures.len(),
        failures.join("\n")
    );
}
