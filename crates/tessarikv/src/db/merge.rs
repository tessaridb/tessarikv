//! Walking the versions of several in-memory tables as one, in internal-key order.

use std::cmp::Ordering;

use crate::memtable::{MemtableEntry, MemtableIter};

/// The versions of every table of a view, by user key ascending and newest version first.
///
/// Each sequence number lives in exactly one table, so two tables never hold the same version
/// and the merge needs no tie rule. The tables of one view are few, so the next version is found
/// by comparing the current version of each table.
pub(super) struct MergedVersions<'a> {
    sources: Vec<MemtableIter<'a>>,
}

impl<'a> MergedVersions<'a> {
    pub(super) fn new(sources: Vec<MemtableIter<'a>>) -> Self {
        Self { sources }
    }

    /// Moves every table to the first version of the least key not less than `key`.
    pub(super) fn seek(&mut self, key: &[u8], snapshot: tessarikv_types::SequenceNumber) {
        for source in &mut self.sources {
            source.seek(key, snapshot);
        }
    }

    /// The least version among the tables, or `None` when every table is past its end.
    pub(super) fn entry(&self) -> Option<MemtableEntry<'a>> {
        self.least().map(|(_, entry)| entry)
    }

    /// Moves past the current version.
    pub(super) fn next(&mut self) {
        if let Some((index, _)) = self.least()
            && let Some(source) = self.sources.get_mut(index)
        {
            source.next();
        }
    }

    /// The table holding the least version, and that version.
    fn least(&self) -> Option<(usize, MemtableEntry<'a>)> {
        self.sources
            .iter()
            .enumerate()
            .filter_map(|(index, source)| source.entry().map(|entry| (index, entry)))
            .min_by(|(_, left), (_, right)| version_order(left, right))
    }
}

/// User key ascending, then the newer version first.
fn version_order(left: &MemtableEntry<'_>, right: &MemtableEntry<'_>) -> Ordering {
    left.user_key
        .cmp(right.user_key)
        .then_with(|| right.sequence.cmp(&left.sequence))
}
