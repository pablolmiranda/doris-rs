//! `ndv`'s HyperLogLog (`core/value/hll.cpp`) and its hash (`HashUtil::murmur_hash64A`). Expected
//! values come from an independent Python port of the C++ source.
use doris_exec::hll::{murmur64a, Hll};

const SEED: u64 = 0xadc83b19;

#[test]
fn murmur64a_matches_known_vectors() {
    let cases: [(&[u8], u64); 7] = [
        (b"", 0xd8dfea6585bc9732),
        (b"a", 0x53d2470a9b43b1a7),
        (b"hello", 0x0f656f01eecfe400),
        (b"abcdefgh", 0xf3a65df559914567),
        (b"abcdefghi", 0x834fba4d9152daf7),
        (&1i32.to_le_bytes(), 0x6c04eeb40912733e),
        (&1i64.to_le_bytes(), 0x900ef48b4182d8c2),
    ];
    for (key, want) in cases {
        assert_eq!(murmur64a(key, SEED), want, "{key:?}");
    }
    assert_eq!(murmur64a("中文".as_bytes(), SEED), 0xaf97054a6bbbf3c6);
}

fn hll_of(n: i64) -> Hll {
    let mut h = Hll::default();
    for i in 0..n {
        h.update(murmur64a(&i.to_le_bytes(), SEED));
    }
    h
}

#[test]
fn ndv_is_exact_up_to_160() {
    assert_eq!(Hll::default().estimate(), 0);
    assert_eq!(hll_of(100).estimate(), 100);
    assert_eq!(hll_of(160).estimate(), 160);
}

#[test]
fn ndv_switches_to_registers_past_160() {
    assert_eq!(hll_of(161).estimate(), 159);
    assert_eq!(hll_of(1000).estimate(), 986);
    assert_eq!(hll_of(5000).estimate(), 5005);
    assert_eq!(hll_of(100_000).estimate(), 101_458);
}

#[test]
fn a_duplicate_after_160_still_converts() {
    let mut h = hll_of(160);
    h.update(murmur64a(&0i64.to_le_bytes(), SEED));
    assert_eq!(h.estimate(), 158);
}

#[test]
fn explicit_merges_stay_explicit_up_to_160() {
    let (mut a, mut b) = (Hll::default(), Hll::default());
    for i in 0..100i64 {
        a.update(murmur64a(&i.to_le_bytes(), SEED));
        b.update(murmur64a(&(i + 60).to_le_bytes(), SEED));
    }
    let mut m = a.clone();
    m.merge(&b);
    assert_eq!(m.estimate(), 160);
    b.update(murmur64a(&160i64.to_le_bytes(), SEED));
    a.merge(&b);
    assert_eq!(a.estimate(), 159);
    // and through its encoding
    assert_eq!(Hll::decode(&a.encode()).unwrap().estimate(), 159);
}
