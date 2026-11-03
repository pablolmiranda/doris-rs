//! The RPC schemas (`internal_service.proto` and its imports) are generated alongside the segment
//! schemas, under the same `doris` package module.
use doris_proto::doris::PExecPlanFragmentRequest;
use prost::Message;

#[test]
fn exec_plan_fragment_request_default_encodes_to_nothing() {
    // every field of PExecPlanFragmentRequest is optional, so an empty message has no bytes
    assert!(PExecPlanFragmentRequest::default()
        .encode_to_vec()
        .is_empty());
}

#[test]
fn segment_v2_is_reachable_from_the_crate_root() {
    let footer = doris_proto::segment_v2::SegmentFooterPb::default();
    assert!(footer.encode_to_vec().is_empty());
}
