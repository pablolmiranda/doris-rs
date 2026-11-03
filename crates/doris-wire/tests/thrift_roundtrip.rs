//! Every captured Thrift message decodes with the generated types and re-encodes to the same
//! bytes, header included. The one accepted difference is the order of set elements and map
//! entries (Java writes hash order, the Rust types write sorted), shown by equal canonical forms;
//! such messages are listed on stderr.
mod common;

use common::first_difference;
use doris_thrift::dispatch::{roundtrip_message, MessageKind};
use doris_wire::canonical::binary_message;
use doris_wire::{Kind, Manifest, Protocol};

fn manifest_kind(k: MessageKind) -> Kind {
    match k {
        MessageKind::Call => Kind::Call,
        MessageKind::Reply => Kind::Reply,
        MessageKind::Exception => Kind::Exception,
        MessageKind::OneWay => Kind::Oneway,
    }
}

#[test]
fn every_thrift_message_reencodes_identically() {
    let m = Manifest::load();
    let thrift: Vec<_> = m
        .messages()
        .iter()
        .filter(|e| e.protocol == Protocol::Thrift)
        .collect();
    assert!(!thrift.is_empty(), "no Thrift messages in the corpus");
    let mut failures = Vec::new();
    let mut reordered = Vec::new();
    for e in &thrift {
        let bytes = m.read(e);
        let what = format!(
            "{} {}.{} {:?}",
            e.file.display(),
            e.service,
            e.method,
            e.kind
        );
        match roundtrip_message(&e.service, &bytes) {
            Err(err) => failures.push(format!("{what}: {err}")),
            Ok(rt) => {
                let header = (rt.method.as_str(), manifest_kind(rt.kind), Some(rt.seqid));
                if header != (e.method.as_str(), e.kind, e.seqid) {
                    failures.push(format!("{what}: header decodes as {header:?}"));
                } else if let Some(at) = first_difference(&bytes, &rt.bytes) {
                    let canonical = binary_message(&bytes);
                    if canonical.is_ok() && canonical == binary_message(&rt.bytes) {
                        reordered.push(format!("{what}: first difference at byte {at}"));
                        continue;
                    }
                    failures.push(format!(
                        "{what}: re-encoding differs at byte {at} ({} bytes captured, {} re-encoded)",
                        bytes.len(),
                        rt.bytes.len()
                    ));
                }
            }
        }
    }
    if !reordered.is_empty() {
        eprintln!(
            "{} of {} Thrift messages re-encode identically up to set/map order:\n{}",
            reordered.len(),
            thrift.len(),
            reordered.join("\n")
        );
    }
    assert!(
        failures.is_empty(),
        "{} of {} Thrift messages did not re-encode identically:\n{}",
        failures.len(),
        thrift.len(),
        failures.join("\n")
    );
}
