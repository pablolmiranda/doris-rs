//! Apache Doris Thrift types, generated from the pinned IDL in `idl/` by `regen.sh` (the changes
//! made to the compiler output are listed in `patches.md`), and a codec check that decodes and
//! re-encodes whole RPC messages with them.

pub mod dispatch;
pub mod gen;
