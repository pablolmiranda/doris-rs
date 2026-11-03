//! Decode and re-encode whole FE↔BE Thrift messages with the generated types, to check that the
//! Rust codecs reproduce what the Java FE and C++ BE put on the wire.
//!
//! Messages are strict `TBinaryProtocol` (versioned header, no framing), as the reference
//! exchanges them on its Thrift ports.
use thrift::protocol::{
    TBinaryInputProtocol, TBinaryOutputProtocol, TCompactInputProtocol, TInputProtocol,
    TMessageType, TOutputProtocol, TSerializable,
};

use crate::compact::CompactOutputProtocol;
use crate::gen::methods::{Failure, METHODS};
use crate::gen::palo_internal_service::TPipelineFragmentParamsList;

/// The message type in a Thrift message header.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MessageKind {
    Call,
    Reply,
    Exception,
    OneWay,
}

impl From<TMessageType> for MessageKind {
    fn from(t: TMessageType) -> Self {
        match t {
            TMessageType::Call => Self::Call,
            TMessageType::Reply => Self::Reply,
            TMessageType::Exception => Self::Exception,
            TMessageType::OneWay => Self::OneWay,
        }
    }
}

/// A decoded and re-encoded message.
#[derive(Debug)]
pub struct RoundTrip {
    /// Method name from the message header.
    pub method: String,
    pub kind: MessageKind,
    pub seqid: i32,
    /// The message encoded again from the decoded value.
    pub bytes: Vec<u8>,
}

#[derive(Debug, thiserror::Error)]
pub enum RoundTripError {
    #[error("{service} has no method {method:?}")]
    UnknownMethod { service: String, method: String },
    #[error("decode: {0}")]
    Decode(thrift::Error),
    #[error("encode: {0}")]
    Encode(thrift::Error),
    #[error("{0} bytes after the end of the message")]
    TrailingBytes(usize),
}

impl From<Failure> for RoundTripError {
    fn from(f: Failure) -> Self {
        match f {
            Failure::Decode(e) => Self::Decode(e),
            Failure::Encode(e) => Self::Encode(e),
        }
    }
}

/// Decode one whole message (header and body) of `service`, an IDL service name such as
/// `FrontendService`, and encode it again. Calls decode the method's `*Args` struct, replies its
/// `*Result` struct, and exceptions a `TApplicationException`.
pub fn roundtrip_message(service: &str, message: &[u8]) -> Result<RoundTrip, RoundTripError> {
    let mut rest = message;
    let mut out = Vec::with_capacity(message.len());
    let mut i = TBinaryInputProtocol::new(&mut rest, true);
    let mut o = TBinaryOutputProtocol::new(&mut out, true);
    let ident = i.read_message_begin().map_err(RoundTripError::Decode)?;
    let kind = MessageKind::from(ident.message_type);
    o.write_message_begin(&ident)
        .map_err(RoundTripError::Encode)?;
    if kind == MessageKind::Exception {
        let e = thrift::Error::read_application_error_from_in_protocol(&mut i)
            .map_err(RoundTripError::Decode)?;
        thrift::Error::write_application_error_to_out_protocol(&e, &mut o)
            .map_err(RoundTripError::Encode)?;
    } else {
        let method = METHODS
            .iter()
            .find(|m| m.service == service && m.name == ident.name)
            .ok_or_else(|| RoundTripError::UnknownMethod {
                service: service.to_owned(),
                method: ident.name.clone(),
            })?;
        let body = if kind == MessageKind::Reply {
            method.result
        } else {
            method.args
        };
        body(&mut i, &mut o)?;
    }
    i.read_message_end().map_err(RoundTripError::Decode)?;
    o.write_message_end().map_err(RoundTripError::Encode)?;
    o.flush().map_err(RoundTripError::Encode)?;
    if !rest.is_empty() {
        return Err(RoundTripError::TrailingBytes(rest.len()));
    }
    Ok(RoundTrip {
        method: ident.name,
        kind,
        seqid: ident.sequence_number,
        bytes: out,
    })
}

/// Decode a `TPipelineFragmentParamsList` serialized with `TCompactProtocol` (the plan the FE
/// sends in `PExecPlanFragmentRequest.request`) and encode it again, with the reference's field
/// header rule (see [`crate::compact`]).
pub fn roundtrip_plan_compact(bytes: &[u8]) -> Result<Vec<u8>, RoundTripError> {
    let mut rest = bytes;
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = TCompactInputProtocol::new(&mut rest);
    let plan = TPipelineFragmentParamsList::read_from_in_protocol(&mut i)
        .map_err(RoundTripError::Decode)?;
    let mut o = CompactOutputProtocol::new(&mut out);
    plan.write_to_out_protocol(&mut o)
        .map_err(RoundTripError::Encode)?;
    o.flush().map_err(RoundTripError::Encode)?;
    // the compact protocols own buffers, so they must go before `rest` and `out` are read
    drop((i, o));
    if !rest.is_empty() {
        return Err(RoundTripError::TrailingBytes(rest.len()));
    }
    Ok(out)
}
