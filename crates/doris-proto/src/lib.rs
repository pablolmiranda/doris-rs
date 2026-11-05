//! Protobuf types generated from the pinned Apache Doris `.proto` files (see `proto/README.md`).
//!
//! Generated code is exempt from style lints; it mirrors the upstream schema verbatim.

/// `package doris;` — tablet and rowset metadata (`olap_file.proto`, `types.proto`, ...).
/// prost nests sub-packages as child modules, so `doris.segment_v2` lives inside here.
#[allow(clippy::all, clippy::pedantic)]
pub mod doris {
    include!(concat!(env!("OUT_DIR"), "/doris.rs"));
    #[cfg(feature = "json")]
    include!(concat!(env!("OUT_DIR"), "/doris.serde.rs"));
    /// `package doris.segment_v2;` — segment footer, column meta, pages, indexes.
    pub mod segment_v2 {
        include!(concat!(env!("OUT_DIR"), "/doris.segment_v2.rs"));
        #[cfg(feature = "json")]
        include!(concat!(env!("OUT_DIR"), "/doris.segment_v2.serde.rs"));
    }
}

pub use doris::segment_v2;
