//! Where a compaction splits into parts merged side by side.

/// Up to `parts - 1` user keys at which a compaction splits, in order: keys at which one of its
/// input tables starts, other than the least, sampled evenly when there are more than needed.
pub(super) fn boundaries(starts: impl IntoIterator<Item = Vec<u8>>, parts: usize) -> Vec<Vec<u8>> {
    let mut starts: Vec<Vec<u8>> = starts.into_iter().collect();
    starts.sort_unstable();
    starts.dedup();
    let candidates = starts.get(1..).unwrap_or_default();
    let cuts = parts.saturating_sub(1);
    if candidates.len() <= cuts {
        return candidates.to_vec();
    }
    // With more candidates than cuts, the j-th cut is the candidate j/parts of the way along;
    // each step advances by at least one, so the cuts are distinct and in order.
    (1..parts)
        .filter_map(|cut| {
            let at = cut.saturating_mul(candidates.len()).checked_div(parts)?;
            candidates.get(at).cloned()
        })
        .collect()
}
