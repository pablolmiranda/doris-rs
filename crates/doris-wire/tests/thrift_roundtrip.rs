//! Every captured Thrift message decodes with the generated types and re-encodes to the same
//! bytes, header included. The one accepted difference is the order of set elements and map
//! entries (Java writes hash order, the Rust types write sorted), shown by equal canonical forms;
//! such messages are listed on stderr.
use doris_thrift::dispatch::{roundtrip_message, MessageKind};
use doris_wire::roundtrip::{thrift, Outcome};
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
    let messages: Vec<_> = m
        .messages()
        .iter()
        .filter(|e| e.protocol == Protocol::Thrift)
        .collect();
    assert!(!messages.is_empty(), "no Thrift messages in the corpus");
    let mut failures = Vec::new();
    let mut reordered = Vec::new();
    for e in &messages {
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
            Ok(decoded) => {
                let header = (
                    decoded.method.as_str(),
                    manifest_kind(decoded.kind),
                    Some(decoded.seqid),
                );
                if header != (e.method.as_str(), e.kind, e.seqid) {
                    failures.push(format!("{what}: header decodes as {header:?}"));
                    continue;
                }
                match thrift(&e.service, &bytes) {
                    Ok(Outcome::Reordered) => reordered.push(what),
                    Ok(_) => {}
                    Err(err) => failures.push(format!("{what}: {err}")),
                }
            }
        }
    }
    if !reordered.is_empty() {
        eprintln!(
            "{} of {} Thrift messages re-encode identically up to set/map order:\n{}",
            reordered.len(),
            messages.len(),
            reordered.join("\n")
        );
    }
    assert!(
        failures.is_empty(),
        "{} of {} Thrift messages did not re-encode identically:\n{}",
        failures.len(),
        messages.len(),
        failures.join("\n")
    );
}
