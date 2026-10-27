//! Range tombstones and how a read applies them.
//!
//! A range delete `[start, end)` at sequence `s` hides every version below `s` of every key in
//! the range. Tombstones from all of a read's sources may overlap, so a read cuts them into
//! fragments that do not: each fragment is a key range with the highest sequence of the
//! tombstones covering it. A version is deleted when the fragment covering its key carries a
//! higher sequence than the version.

use std::collections::BinaryHeap;
use std::sync::Arc;

use tessarikv_types::SequenceNumber;

/// One range delete.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RangeTombstone {
    pub(crate) start: Vec<u8>,
    /// Exclusive.
    pub(crate) end: Vec<u8>,
    pub(crate) sequence: SequenceNumber,
}

/// Non-overlapping fragments of a set of tombstones, sorted by key.
#[derive(Debug, Default)]
pub(crate) struct Fragments {
    /// `(start, end, sequence)`, starts ascending, each end at or before the next start.
    pieces: Vec<(Vec<u8>, Vec<u8>, SequenceNumber)>,
}

impl Fragments {
    /// The fragments of the tombstones at or below `snapshot`; later ones are not yet visible.
    pub(crate) fn new(tombstones: &[RangeTombstone], snapshot: SequenceNumber) -> Self {
        let visible: Vec<&RangeTombstone> = tombstones
            .iter()
            .filter(|tombstone| tombstone.sequence <= snapshot && tombstone.start < tombstone.end)
            .collect();
        if visible.is_empty() {
            return Self::default();
        }
        // Every start and end is a boundary; between two neighbouring boundaries the set of
        // covering tombstones does not change. A sweep over the boundaries keeps the tombstones
        // started so far in a heap by sequence, dropping those that ended, so the heap's top is
        // the highest sequence covering the piece.
        let mut boundaries: Vec<&[u8]> = visible
            .iter()
            .flat_map(|tombstone| [tombstone.start.as_slice(), tombstone.end.as_slice()])
            .collect();
        boundaries.sort_unstable();
        boundaries.dedup();
        let mut by_start = visible;
        by_start.sort_unstable_by(|left, right| left.start.cmp(&right.start));
        let mut starting = by_start.into_iter().peekable();
        let mut active: BinaryHeap<(SequenceNumber, &[u8])> = BinaryHeap::new();
        let mut pieces: Vec<(Vec<u8>, Vec<u8>, SequenceNumber)> = Vec::new();
        for pair in boundaries.windows(2) {
            let [from, to] = pair else { continue };
            while let Some(tombstone) =
                starting.next_if(|tombstone| tombstone.start.as_slice() <= *from)
            {
                active.push((tombstone.sequence, tombstone.end.as_slice()));
            }
            while active.peek().is_some_and(|(_, end)| *end <= *from) {
                active.pop();
            }
            // A tombstone ended below the top stays in the heap until it reaches the top; its
            // sequence is lower, so it never decides a piece it no longer covers.
            let Some(&(sequence, _)) = active.peek() else {
                continue;
            };
            match pieces.last_mut() {
                // A neighbour with the same sequence is one piece.
                Some(last) if last.1.as_slice() == *from && last.2 == sequence => {
                    last.1 = to.to_vec();
                }
                _ => pieces.push((from.to_vec(), to.to_vec(), sequence)),
            }
        }
        Self { pieces }
    }

    /// Whether the version `sequence` of `key` is deleted by a fragment.
    #[cfg(test)]
    pub(crate) fn deletes(&self, key: &[u8], sequence: SequenceNumber) -> bool {
        self.covering(key)
            .is_some_and(|covering| sequence < covering)
    }

    /// The highest sequence of the range deletes covering `key`: every version of it below
    /// that is deleted.
    pub(crate) fn covering(&self, key: &[u8]) -> Option<SequenceNumber> {
        // The last piece starting at or before the key.
        let after = self
            .pieces
            .partition_point(|(start, _, _)| start.as_slice() <= key);
        after
            .checked_sub(1)
            .and_then(|index| self.pieces.get(index))
            .filter(|(_, end, _)| key < end.as_slice())
            .map(|(_, _, covering)| *covering)
    }
}

/// What the range deletes a read sees hide: those of the table files, cut into fragments once
/// for every reader, and those of the in-memory tables, cut per read.
#[derive(Debug, Default)]
pub(crate) struct Deletions {
    files: Option<Arc<Fragments>>,
    recent: Fragments,
}

impl Deletions {
    /// The deletions of `files`, the table files' fragments, and `recent`, the in-memory tables'.
    pub(crate) fn new(files: Option<Arc<Fragments>>, recent: Fragments) -> Self {
        Self { files, recent }
    }

    /// Whether the version `sequence` of `key` is deleted by a range delete.
    pub(crate) fn deletes(&self, key: &[u8], sequence: SequenceNumber) -> bool {
        self.covering(key)
            .is_some_and(|covering| sequence < covering)
    }

    /// The highest sequence of the range deletes covering `key`.
    pub(crate) fn covering(&self, key: &[u8]) -> Option<SequenceNumber> {
        let files = self.files.as_deref().and_then(|files| files.covering(key));
        files.max(self.recent.covering(key))
    }
}

#[cfg(test)]
mod tests;
