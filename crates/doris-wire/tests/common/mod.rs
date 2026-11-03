/// Offset of the first byte where two encodings differ (the shorter one's length when one is a
/// prefix of the other), or `None` when they are identical.
pub fn first_difference(captured: &[u8], reencoded: &[u8]) -> Option<usize> {
    if captured == reencoded {
        return None;
    }
    Some(
        captured
            .iter()
            .zip(reencoded)
            .position(|(a, b)| a != b)
            .unwrap_or(captured.len().min(reencoded.len())),
    )
}
