//! Range tombstones and how a read applies them.
//!
//! A range delete `[start, end)` at sequence `s` hides every version below `s` of every key in
//! the range. Tombstones from all of a read's sources may overlap, so a read cuts them into
//! fragments that do not: each fragment is a key range with the highest sequence of the
//! tombstones covering it. A version is deleted when the fragment covering its key carries a
//! higher sequence than the version.

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
        // covering tombstones does not change.
        let mut boundaries: Vec<&[u8]> = visible
            .iter()
            .flat_map(|tombstone| [tombstone.start.as_slice(), tombstone.end.as_slice()])
            .collect();
        boundaries.sort_unstable();
        boundaries.dedup();
        let mut pieces: Vec<(Vec<u8>, Vec<u8>, SequenceNumber)> = Vec::new();
        for pair in boundaries.windows(2) {
            let [from, to] = pair else { continue };
            let covering = visible
                .iter()
                .filter(|tombstone| {
                    tombstone.start.as_slice() <= *from && *to <= tombstone.end.as_slice()
                })
                .map(|tombstone| tombstone.sequence)
                .max();
            let Some(sequence) = covering else { continue };
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

    /// Whether there is no fragment.
    pub(crate) fn is_empty(&self) -> bool {
        self.pieces.is_empty()
    }

    /// Whether the version `sequence` of `key` is deleted by a fragment.
    pub(crate) fn deletes(&self, key: &[u8], sequence: SequenceNumber) -> bool {
        // The last piece starting at or before the key.
        let after = self
            .pieces
            .partition_point(|(start, _, _)| start.as_slice() <= key);
        after
            .checked_sub(1)
            .and_then(|index| self.pieces.get(index))
            .is_some_and(|(_, end, covering)| key < end.as_slice() && sequence < *covering)
    }
}

#[cfg(test)]
mod tests;
