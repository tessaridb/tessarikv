//! The newest commit that wrote each key, coarsely: enough to tell a committing transaction
//! whether anything it writes was written after its snapshot.

#[cfg(all(test, not(loom)))]
mod tests;

#[cfg(all(test, loom))]
mod loom_tests;

use crossbeam_utils::CachePadded;

use crate::shim::{AtomicU64, Ordering};

/// For each bucket of key hashes, the highest sequence number of a commit that wrote a key in
/// it; and the highest sequence number of a commit that deleted a range.
///
/// Sequential specification: every value starts at 0. `record(h, s)` raises the bucket of `h` to
/// `max(bucket, s)`; `record_range(s)` raises the range value likewise. `newest(h)` returns the
/// bucket of `h` and `newest_range()` the range value. Values never decrease. Two hashes may share
/// a bucket, so `newest(h)` is an upper bound on the newest commit that wrote a key hashing to
/// `h`, never below it.
///
/// | Operation | Linearization point | Progress |
/// |---|---|---|
/// | `record`, `record_range` | the `fetch_max` | wait-free |
/// | `newest`, `newest_range` | the load | wait-free |
///
/// Ordering: every access is Relaxed. The table publishes nothing by itself: a committer records
/// its keys and only then publishes its commit through a Release (in the engine,
/// [`crate::Sequencer::publish`]); a validator reads the table after the Acquire that observes
/// that publication, so it reads a value at least as high as the one recorded (the
/// message-passing shape; coherence forbids reading an older value of a location written before
/// in happens-before order).
///
/// Reclamation: the buckets live as long as the table; values only grow, so there is no ABA.
pub struct ConflictTable {
    buckets: Box<[AtomicU64]>,
    /// Right shift keeping the top bits of a remixed hash: one bucket index.
    shift: u32,
    ranges: CachePadded<AtomicU64>,
}

/// Odd multiplier near 2^64 / phi: multiplying spreads every bit of a hash into the top bits.
const BUCKET_MIX: u64 = 0x9e37_79b9_7f4a_7c15;

impl std::fmt::Debug for ConflictTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConflictTable")
            .field("buckets", &self.buckets.len())
            .finish_non_exhaustive()
    }
}

impl ConflictTable {
    /// A table of `buckets` buckets, rounded up to a power of two, at least 2.
    pub fn new(buckets: usize) -> Self {
        let buckets = buckets.max(2).next_power_of_two();
        Self {
            buckets: (0..buckets).map(|_| AtomicU64::new(0)).collect(),
            shift: 64_u32.saturating_sub(buckets.trailing_zeros()),
            ranges: CachePadded::new(AtomicU64::new(0)),
        }
    }

    /// Records that the commit at `sequence` wrote a key hashing to `hash`.
    pub fn record(&self, hash: u64, sequence: u64) {
        if let Some(bucket) = self.bucket(hash) {
            // Relaxed: published by the committer's later Release (see the type's documentation).
            bucket.fetch_max(sequence, Ordering::Relaxed);
        }
    }

    /// Records that the commit at `sequence` deleted a range.
    pub fn record_range(&self, sequence: u64) {
        // Relaxed: as in `record`.
        self.ranges.fetch_max(sequence, Ordering::Relaxed);
    }

    /// The highest sequence recorded for the bucket of `hash`.
    pub fn newest(&self, hash: u64) -> u64 {
        // Relaxed: read after the caller's Acquire of the publication (see the type's
        // documentation).
        self.bucket(hash)
            .map_or(0, |bucket| bucket.load(Ordering::Relaxed))
    }

    /// The highest sequence recorded for a range delete.
    pub fn newest_range(&self) -> u64 {
        // Relaxed: as in `newest`.
        self.ranges.load(Ordering::Relaxed)
    }

    /// The bucket of `hash`: the top bits of its multiplicative remix.
    fn bucket(&self, hash: u64) -> Option<&AtomicU64> {
        let index = hash
            .wrapping_mul(BUCKET_MIX)
            .checked_shr(self.shift)
            .unwrap_or(0);
        self.buckets.get(usize::try_from(index).ok()?)
    }
}
