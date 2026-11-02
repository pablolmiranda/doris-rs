//! Reader for Apache Doris segment (`.dat`) files.
//!
//! The on-disk format is defined by the Doris C++ Backend; this crate reads it and is verified
//! against the golden corpus in `oracle/corpus` (tablets written by the reference release).

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

/// Protobuf types generated from the pinned Doris `.proto` files (see `proto/README.md`).
/// Generated code is exempt from style lints; it mirrors the upstream schema verbatim.
#[allow(clippy::all, clippy::pedantic)]
pub mod pb {
    /// `package doris;` — tablet and rowset metadata (`olap_file.proto`, `types.proto`, ...).
    /// prost nests sub-packages as child modules, so `doris.segment_v2` lives inside here.
    pub mod doris {
        include!(concat!(env!("OUT_DIR"), "/doris.rs"));
        /// `package doris.segment_v2;` — segment footer, column meta, pages, indexes.
        pub mod segment_v2 {
            include!(concat!(env!("OUT_DIR"), "/doris.segment_v2.rs"));
        }
    }
    pub use doris::segment_v2;
}

pub use error::Error;
pub use footer::{read_footer, SegmentFile};
