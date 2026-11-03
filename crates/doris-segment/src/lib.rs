//! Reader for Apache Doris segment (`.dat`) files.
//!
//! The on-disk format is defined by the Doris C++ Backend; this crate reads it and is verified
//! against the golden corpus in `oracle/corpus` (tablets written by the reference release).

pub mod binary;
pub mod bitshuffle;
pub mod column;
pub mod corpus;
pub mod error;
pub mod footer;
pub mod ordinal;
pub mod page;
pub mod rle;
pub mod values;
pub mod varint;

/// Protobuf types generated from the pinned Doris `.proto` files, from the `doris-proto` crate.
pub use doris_proto as pb;

pub use error::Error;
pub use footer::{read_footer, SegmentFile};
