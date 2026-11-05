//! The C++ BE's `hash_of_path` (`be/src/util/string_util.cpp`), which names a disk to the FE
//! (`TDisk.path_hash`). It is libstdc++'s `std::hash<std::string>` (`_Hash_bytes`, seed
//! `0xc70f6907`) of the host, then a boost-style `hash_combine` (`be/src/util/hash_util.hpp`)
//! with each non-empty `/`-separated segment of the path.

const SEED: u64 = 0xc70f_6907;

fn shift_mix(v: u64) -> u64 {
    v ^ (v >> 47)
}

/// libstdc++ `std::_Hash_bytes` for 64-bit `size_t` (`libstdc++-v3/libsupc++/hash_bytes.cc`).
fn hash_bytes(bytes: &[u8], seed: u64) -> u64 {
    const MUL: u64 = (0xc6a4_a793_u64 << 32) + 0x5bd1_e995;
    let mut hash = seed ^ (bytes.len() as u64).wrapping_mul(MUL);
    let mut chunks = bytes.chunks_exact(8);
    for chunk in &mut chunks {
        let data = u64::from_le_bytes(chunk.try_into().expect("8 bytes"));
        hash ^= shift_mix(data.wrapping_mul(MUL)).wrapping_mul(MUL);
        hash = hash.wrapping_mul(MUL);
    }
    let rest = chunks.remainder();
    if !rest.is_empty() {
        let data = rest.iter().rev().fold(0u64, |acc, &b| (acc << 8) + u64::from(b));
        hash ^= data;
        hash = hash.wrapping_mul(MUL);
    }
    shift_mix(shift_mix(hash).wrapping_mul(MUL))
}

fn std_hash(s: &str) -> u64 {
    hash_bytes(s.as_bytes(), SEED)
}

pub fn hash_of_path(identifier: &str, path: &str) -> u64 {
    let mut hash = std_hash(identifier);
    for part in path.split('/').filter(|p| !p.is_empty()) {
        hash ^= std_hash(part).wrapping_add(0x9e37_79b9).wrapping_add(hash << 6).wrapping_add(hash >> 2);
    }
    hash
}
