//! Walking a table's versions in internal-key order.

use std::cmp::Ordering;
use std::sync::Arc;

use tessarikv_sync::{Cursor, CursorPosition};
use tessarikv_types::{
    InternalKeyRef, SequenceNumber, ValueKind, compare_internal, encode_internal, seek_trailer,
};

use super::{InternalKeyComparator, Memtable};

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

    /// Moves to the last version.
    pub(crate) fn seek_to_last(&mut self) {
        self.cursor.seek_to_last();
    }

    /// Moves to the last version whose internal key is not greater than `target`.
    pub(crate) fn seek_for_prev(&mut self, target: &[u8]) {
        self.cursor.seek(target);
        if !self.cursor.valid() {
            self.cursor.seek_to_last();
        } else if compare_internal(self.cursor.key(), target) == Ordering::Greater {
            self.cursor.prev();
        }
    }

    /// Moves to the previous version.
    pub(crate) fn prev(&mut self) {
        self.cursor.prev();
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

/// A position among a shared table's versions that owns the table instead of borrowing it, so
/// it can live as long as an iterator does; each step borrows a cursor from the position.
pub(crate) struct MemtableCursor {
    table: Arc<Memtable>,
    position: CursorPosition,
    seek_key: Vec<u8>,
}

impl MemtableCursor {
    pub(crate) fn new(table: Arc<Memtable>) -> Self {
        let position = table.entries.cursor().position();
        Self {
            table,
            position,
            seek_key: Vec::new(),
        }
    }

    /// As [`MemtableIter::seek`].
    pub(crate) fn seek(&mut self, key: &[u8], snapshot: SequenceNumber) {
        self.step(|versions| versions.seek(key, snapshot));
    }

    /// Moves to the next version.
    pub(crate) fn next(&mut self) {
        self.step(|versions| versions.next());
    }

    /// Moves to the last version.
    pub(crate) fn seek_to_last(&mut self) {
        self.step(|versions| versions.seek_to_last());
    }

    /// As [`MemtableIter::seek_for_prev`].
    pub(crate) fn seek_for_prev(&mut self, target: &[u8]) {
        self.step(|versions| versions.seek_for_prev(target));
    }

    /// Moves to the previous version.
    pub(crate) fn prev(&mut self) {
        self.step(|versions| versions.prev());
    }

    /// The current version, or `None` past the end.
    pub(crate) fn entry(&self) -> Option<MemtableEntry<'_>> {
        MemtableIter::new(self.table.entries.cursor_at(self.position)).entry()
    }

    /// Runs `move_to` on a cursor at the position and keeps where it lands.
    fn step(&mut self, move_to: impl FnOnce(&mut MemtableIter<'_>)) {
        let mut versions = MemtableIter {
            cursor: self.table.entries.cursor_at(self.position),
            seek_key: std::mem::take(&mut self.seek_key),
        };
        move_to(&mut versions);
        self.position = versions.cursor.position();
        self.seek_key = versions.seek_key;
    }
}
