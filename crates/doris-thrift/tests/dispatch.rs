//! The message codec table: a message built by the generated client or processor decodes and
//! re-encodes to the same bytes, and malformed input is an error, never a panic.
use std::cell::RefCell;
use std::io::Write;
use std::rc::Rc;

use doris_thrift::dispatch::{
    roundtrip_message, roundtrip_plan_compact, MessageKind, RoundTripError,
};
use doris_thrift::gen::heartbeat_service::{
    HeartbeatServiceSyncClient, HeartbeatServiceSyncHandler, HeartbeatServiceSyncProcessor,
    TBackendInfo, THeartbeatResult, THeartbeatServiceSyncClient, TMasterInfo,
};
use doris_thrift::gen::palo_internal_service::TPipelineFragmentParamsList;
use doris_thrift::gen::status::{TStatus, TStatusCode};
use doris_thrift::gen::types::TNetworkAddress;
use thrift::protocol::{
    TBinaryInputProtocol, TBinaryOutputProtocol, TCompactOutputProtocol, TMessageIdentifier,
    TMessageType, TOutputProtocol, TSerializable, TStructIdentifier,
};
use thrift::server::TProcessor;

/// A write transport whose bytes stay readable after the protocol that owns it is gone.
#[derive(Clone, Default)]
struct Capture(Rc<RefCell<Vec<u8>>>);

impl Write for Capture {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.borrow_mut().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn master_info() -> TMasterInfo {
    TMasterInfo::new(
        TNetworkAddress::new("fe-host".to_owned(), 9020),
        1_234_567,
        3,
        "token".to_owned(),
        None,
        None,
        None,
        10_001,
        None,
        None,
        None,
        None,
        None,
        None,
    )
}

/// The bytes the generated client puts on the wire for `heartbeat(master_info())`.
fn heartbeat_call() -> Vec<u8> {
    let sent = Capture::default();
    let i_prot = TBinaryInputProtocol::new(std::io::empty(), true);
    let o_prot = TBinaryOutputProtocol::new(sent.clone(), true);
    let mut client = HeartbeatServiceSyncClient::new(i_prot, o_prot);
    // nothing answers, so receiving the reply fails after the call has been written
    assert!(client.heartbeat(master_info()).is_err());
    let bytes = sent.0.borrow().clone();
    bytes
}

struct Backend;

impl HeartbeatServiceSyncHandler for Backend {
    fn handle_heartbeat(&self, _: TMasterInfo) -> thrift::Result<THeartbeatResult> {
        let info = TBackendInfo::new(
            9060,
            8040,
            None,
            8060,
            "4.1.5".to_owned(),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        );
        Ok(THeartbeatResult::new(
            TStatus::new(TStatusCode::OK, None),
            info,
        ))
    }
}

#[test]
fn heartbeat_call_from_the_generated_client_round_trips() {
    let call = heartbeat_call();
    // strict header: version|type, name length, "heartbeat", then the sequence id
    let seqid = i32::from_be_bytes(call[17..21].try_into().unwrap());
    let rt = roundtrip_message("HeartbeatService", &call).unwrap();
    assert_eq!(
        (rt.method.as_str(), rt.kind, rt.seqid),
        ("heartbeat", MessageKind::Call, seqid)
    );
    assert_eq!(rt.bytes, call);
}

#[test]
fn heartbeat_reply_from_the_generated_processor_round_trips() {
    let call = heartbeat_call();
    let reply = Capture::default();
    let mut i_prot = TBinaryInputProtocol::new(&call[..], true);
    let mut o_prot = TBinaryOutputProtocol::new(reply.clone(), true);
    HeartbeatServiceSyncProcessor::new(Backend)
        .process(&mut i_prot, &mut o_prot)
        .unwrap();
    let reply = reply.0.borrow().clone();
    let rt = roundtrip_message("HeartbeatService", &reply).unwrap();
    assert_eq!(
        (rt.method.as_str(), rt.kind),
        ("heartbeat", MessageKind::Reply)
    );
    assert_eq!(rt.bytes, reply);
}

#[test]
fn unknown_method_is_an_error() {
    let mut bytes = Vec::new();
    {
        let mut o = TBinaryOutputProtocol::new(&mut bytes, true);
        o.write_message_begin(&TMessageIdentifier::new(
            "no_such_method",
            TMessageType::Call,
            1,
        ))
        .unwrap();
        o.write_struct_begin(&TStructIdentifier::new("args"))
            .unwrap();
        o.write_field_stop().unwrap();
        o.write_struct_end().unwrap();
        o.write_message_end().unwrap();
    }
    let err = roundtrip_message("HeartbeatService", &bytes).unwrap_err();
    assert!(
        matches!(&err, RoundTripError::UnknownMethod { service, method }
            if service == "HeartbeatService" && method == "no_such_method"),
        "{err}"
    );
    // a known method under the wrong service is unknown too
    let err = roundtrip_message("FrontendService", &heartbeat_call()).unwrap_err();
    assert!(matches!(err, RoundTripError::UnknownMethod { .. }), "{err}");
}

#[test]
fn bytes_after_the_message_are_an_error() {
    let mut call = heartbeat_call();
    call.push(0);
    let err = roundtrip_message("HeartbeatService", &call).unwrap_err();
    assert!(matches!(err, RoundTripError::TrailingBytes(1)), "{err}");
}

#[test]
fn truncated_message_is_a_decode_error() {
    let call = heartbeat_call();
    let err = roundtrip_message("HeartbeatService", &call[..call.len() - 1]).unwrap_err();
    assert!(matches!(err, RoundTripError::Decode(_)), "{err}");
}

#[test]
fn compact_plan_round_trips() {
    let plan = TPipelineFragmentParamsList {
        coord: Some(TNetworkAddress::new("fe-host".to_owned(), 9020)),
        fragment_num_on_host: Some(3),
        is_nereids: Some(true),
        topn_filter_source_node_ids: Some(vec![1, 2]),
        ..Default::default()
    };
    let mut bytes = Vec::new();
    {
        let mut o = TCompactOutputProtocol::new(&mut bytes);
        plan.write_to_out_protocol(&mut o).unwrap();
        o.flush().unwrap();
    }
    assert_eq!(roundtrip_plan_compact(&bytes).unwrap(), bytes);
    bytes.push(0);
    let err = roundtrip_plan_compact(&bytes).unwrap_err();
    assert!(matches!(err, RoundTripError::TrailingBytes(1)), "{err}");
}
