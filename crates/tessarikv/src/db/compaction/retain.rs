//! Which versions of a key a compaction writes out.
//!
//! A reader reads at the visible sequence or at a snapshot, so the sequences a version may be
//! read at fall into stripes: those up to the oldest snapshot, those up to the next one, and so
//! on up to the visible sequence. Within a stripe only the newest version can ever be read, so a
//! compaction keeps that one and drops the rest. Versions above the visible sequence are all
//! kept: no reader has chosen between them yet.
//!
//! A delete is itself a version and is kept like any other, except in the oldest stripe: there
//! it hides nothing any reader can still see once the compaction drops the versions below it,
//! so it is dropped too, unless an older version of the key may remain in a level below the
//! compaction's output, which the delete must go on hiding.
//!
//! A version a range delete hides is dropped when no reader can tell: the range delete is newer
//! and in the same stripe (see [`super::job`]). A range delete in the oldest stripe is dropped
//! once nothing outside the compaction can hold a version under it.

use tessarikv_types::ValueKind;

/// The read boundaries a compaction keeps versions for.
#[derive(Debug, Clone)]
pub(crate) struct Retention {
    /// The snapshots and the visible sequence, ascending, without repeats.
    boundaries: Vec<u64>,
    /// The newest snapshot given, including one at or above the visible sequence.
    newest_snapshot: Option<u64>,
}

/// The decisions for the versions of one key, newest first.
#[derive(Debug)]
pub(crate) struct KeyRetention<'a> {
    retention: &'a Retention,
    /// The stripe of the last version kept or dropped at or below the visible sequence.
    last_stripe: Option<usize>,
}

impl Retention {
    /// Boundaries at `visible` and every sequence in `snapshots`.
    pub(crate) fn new(visible: u64, snapshots: &[u64]) -> Self {
        let mut boundaries: Vec<u64> = snapshots
            .iter()
            .copied()
            .filter(|snapshot| *snapshot < visible)
            .collect();
        boundaries.push(visible);
        boundaries.sort_unstable();
        boundaries.dedup();
        Self {
            boundaries,
            newest_snapshot: snapshots.iter().copied().max(),
        }
    }

    /// Whether no snapshot can read the version at `sequence` and it is at or below the visible
    /// sequence: the versions a compaction filter may be shown.
    pub(crate) fn past_every_snapshot(&self, sequence: u64) -> bool {
        !self.above_visible(self.stripe(sequence))
            && self
                .newest_snapshot
                .is_none_or(|snapshot| sequence > snapshot)
    }

    /// The stripe of `sequence`: the index of the least boundary at or above it, which is the
    /// number of boundaries when it is above the visible sequence.
    pub(crate) fn stripe(&self, sequence: u64) -> usize {
        self.boundaries
            .partition_point(|boundary| *boundary < sequence)
    }

    /// Whether `stripe` lies above the visible sequence, where every version is kept.
    pub(crate) fn above_visible(&self, stripe: usize) -> bool {
        stripe >= self.boundaries.len()
    }

    /// The decisions for the next key.
    pub(crate) fn key(&self) -> KeyRetention<'_> {
        KeyRetention {
            retention: self,
            last_stripe: None,
        }
    }
}

impl KeyRetention<'_> {
    /// Whether the version at `sequence`, the next older version of the key, is the newest of
    /// its stripe at or below the visible sequence: the one [`Self::keeps`] writes out, unless
    /// it is a delete in the oldest stripe.
    pub(crate) fn newest_in_stripe(&self, sequence: u64) -> bool {
        let stripe = self.retention.stripe(sequence);
        !self.retention.above_visible(stripe) && self.last_stripe != Some(stripe)
    }

    /// Whether the version at `sequence` of kind `kind`, the next older version of the key, is
    /// written out. `deeper_holds_key` answers whether a level below the output may hold an
    /// older version of the key; it is asked only for a delete in the oldest stripe.
    pub(crate) fn keeps(
        &mut self,
        sequence: u64,
        kind: ValueKind,
        deeper_holds_key: impl FnOnce() -> bool,
    ) -> bool {
        let boundaries = &self.retention.boundaries;
        // The least boundary at or above the version; past the last one it is above visible.
        let stripe = boundaries.partition_point(|boundary| *boundary < sequence);
        if stripe == boundaries.len() {
            return true;
        }
        if self.last_stripe == Some(stripe) {
            return false;
        }
        self.last_stripe = Some(stripe);
        !(kind == ValueKind::Delete && stripe == 0 && !deeper_holds_key())
    }
}
