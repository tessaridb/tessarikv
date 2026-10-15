//! Walking a table's versions in internal-key order.

use tessarikv_sync::Cursor;
use tessarikv_types::{InternalKeyRef, SequenceNumber, ValueKind, encode_internal, seek_trailer};

use super::InternalKeyComparator;

/// One version in a table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct MemtableEntry<'a> {
    pub(crate) user_key: &'a [u8],
    pub(crate) sequence: SequenceNumber,
    pub(crate) kind: ValueKind,
    /// The value of a put; empty for a delete.
    pub(crate) value: &'a [u8],
}

/// A position among a table's versions; weakly consistent under concurrent inserts.
pub(crate) struct MemtableIter<'a> {
    cursor: Cursor<'a, InternalKeyComparator>,
    seek_key: Vec<u8>,
}

impl<'a> MemtableIter<'a> {
    pub(super) fn new(cursor: Cursor<'a, InternalKeyComparator>) -> Self {
        Self {
            cursor,
            seek_key: Vec::new(),
        }
    }

    /// Moves to the first version.
    #[cfg(test)]
    pub(crate) fn seek_to_first(&mut self) {
        self.cursor.seek_to_first();
    }

    /// Moves to the first version of the least key not less than `key` that is visible at
    /// `snapshot`, or past it when `key` has no such version.
    pub(crate) fn seek(&mut self, key: &[u8], snapshot: SequenceNumber) {
        self.seek_key.clear();
        encode_internal(key, seek_trailer(snapshot), &mut self.seek_key);
        self.cursor.seek(&self.seek_key);
    }

    /// Moves to the next version.
    pub(crate) fn next(&mut self) {
        self.cursor.next();
    }

    /// The current version, or `None` past the end.
    pub(crate) fn entry(&self) -> Option<MemtableEntry<'a>> {
        if !self.cursor.valid() {
            return None;
        }
        let internal = InternalKeyRef::parse(self.cursor.key()).ok()?;
        Some(MemtableEntry {
            user_key: internal.user_key(),
            sequence: internal.trailer().sequence(),
            kind: internal.trailer().kind().ok()?,
            value: self.cursor.value(),
        })
    }
}
