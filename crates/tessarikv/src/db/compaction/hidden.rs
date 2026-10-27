//! What a compaction's range deletes hide, and which of them it may leave out.
//!
//! A version is dropped when a range delete covering it is newer and lies in the same snapshot
//! stripe: every reader that sees the version also sees the delete. A version below the visible
//! sequence is never newer than one that may still be read, so this never drops a version some
//! reader could see.
//!
//! A range delete in the oldest stripe hides nothing any reader still sees once the versions it
//! covers are gone. Those inside the compaction are dropped by the first rule; one outside it —
//! in a table file the compaction does not read, at any level — may lie anywhere under the
//! range, because a range delete travels with its start key. So it is left out only when no
//! other table file of the keyspace overlaps its range. Every part of a split compaction judges
//! versions against all of the compaction's range deletes, so a delete starting in one part
//! still hides what it covers in the others.

use tessarikv_format::TableMeta;
use tessarikv_types::SequenceNumber;

use super::below::Outside;
use super::retain::Retention;
use crate::db::tables::user_key;
use crate::tombstones::{Fragments, RangeTombstone};

/// The range deletes a compaction judges versions by: its inputs' and those of every file outside it.
pub(super) struct Hidden {
    /// Every range delete.
    all: Fragments,
    /// Those in the oldest stripe.
    oldest: Fragments,
}

impl Hidden {
    /// The fragments of `tombstones`, and those of them the outputs keep: all but the deletes
    /// in the oldest stripe of `retention` whose range no table file in `outside` overlaps.
    pub(super) fn new(
        tombstones: &[RangeTombstone],
        retention: &Retention,
        outside: &Outside,
    ) -> (Self, Vec<RangeTombstone>) {
        // A reader applies every range delete of the keyspace, so those of the other files hide
        // versions of the inputs as much as the inputs' own.
        let every: Vec<RangeTombstone> = tombstones
            .iter()
            .chain(&outside.tombstones)
            .cloned()
            .collect();
        let in_oldest =
            |tombstone: &RangeTombstone| retention.stripe(tombstone.sequence.get()) == 0;
        let all = Fragments::new(&every, SequenceNumber::MAX);
        let oldest: Vec<RangeTombstone> = every.iter().filter(|t| in_oldest(t)).cloned().collect();
        let fragments = Fragments::new(&oldest, SequenceNumber::MAX);
        let kept = tombstones
            .iter()
            .filter(|tombstone| {
                !in_oldest(tombstone)
                    || outside
                        .tables
                        .iter()
                        .any(|table| overlaps(table, tombstone))
            })
            .cloned()
            .collect();
        (
            Self {
                all,
                oldest: fragments,
            },
            kept,
        )
    }

    /// Whether the version `sequence` of `key` is hidden from every reader by a range delete.
    pub(super) fn hides(&self, retention: &Retention, key: &[u8], sequence: u64) -> bool {
        let stripe = retention.stripe(sequence);
        if retention.above_visible(stripe) {
            return false;
        }
        let newer = |covering: SequenceNumber| covering.get() > sequence;
        self.oldest.covering(key).is_some_and(newer)
            || self.all.covering(key).is_some_and(|covering| {
                newer(covering) && retention.stripe(covering.get()) == stripe
            })
    }
}

/// Whether `table`'s keys may fall in `tombstone`'s range.
fn overlaps(table: &TableMeta, tombstone: &RangeTombstone) -> bool {
    user_key(&table.smallest) < tombstone.end.as_slice()
        && tombstone.start.as_slice() <= user_key(&table.largest)
}
