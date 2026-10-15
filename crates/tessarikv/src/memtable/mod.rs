//! A keyspace's in-memory table: every recent version of every key, ordered for lookup.
//!
//! Each entry is stored under its internal key (the user key followed by the entry's sequence
//! number and kind), so versions of one key sit together, newest first, and a read at a
//! snapshot seeks straight to the newest version it may see. Entries are only ever added: a
//! delete is an entry, and nothing is removed until the whole table is dropped.
//!
//! The table is a lock-free skiplist: inserts from many writers run concurrently, and reads never
//! wait for them. Inserting the same version twice keeps the first, so a batch may be applied
//! again after an interruption without changing the table.

mod iter;

#[cfg(test)]
mod tests;

use std::cell::RefCell;
use std::cmp::Ordering;

use tessarikv_sync::{ArenaError, KeyComparator, SkipList};
use tessarikv_types::{
    InternalKeyRef, SequenceNumber, Trailer, ValueKind, compare_internal, encode_internal,
    seek_trailer,
};

use crate::{Error, Result};

pub(crate) use iter::MemtableIter;

/// Orders encoded internal keys: user key ascending, then newest version first.
pub(crate) struct InternalKeyComparator;

impl KeyComparator for InternalKeyComparator {
    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
        compare_internal(a, b)
    }
}

thread_local! {
    /// The encoding buffer for internal keys, reused so an insert or a read allocates nothing.
    static SCRATCH: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
}

/// What a table holds for a key at a snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Lookup<'a> {
    /// The newest visible version holds this value.
    Value(&'a [u8]),
    /// The newest visible version is a delete: older tables must not be consulted.
    Deleted,
    /// No version is visible here: older tables decide.
    Absent,
}

/// See the module documentation.
pub(crate) struct Memtable {
    entries: SkipList<InternalKeyComparator>,
}

impl Memtable {
    /// An empty table.
    pub(crate) fn new() -> Self {
        Self {
            entries: SkipList::new(InternalKeyComparator),
        }
    }

    /// Adds version `sequence` of `key`: a value, or a delete (whose `value` is ignored).
    ///
    /// # Errors
    ///
    /// [`Error::Memtable`] for a range delete, which tables do not hold, or when the arena
    /// cannot take the entry.
    pub(crate) fn insert(
        &self,
        sequence: SequenceNumber,
        kind: ValueKind,
        key: &[u8],
        value: &[u8],
    ) -> Result<()> {
        let value = match kind {
            ValueKind::Value => value,
            ValueKind::Delete => &[],
            ValueKind::RangeDelete => {
                return Err(Error::Memtable {
                    reason: "a range delete cannot be stored as a point entry",
                });
            }
        };
        SCRATCH
            .with_borrow_mut(|internal| {
                internal.clear();
                encode_internal(key, Trailer::new(sequence, kind), internal);
                self.entries.insert(internal, value)
            })
            .map(|_| ())
            .map_err(arena_error)
    }

    /// The newest version of `key` whose sequence is at most `snapshot`.
    pub(crate) fn get(&self, key: &[u8], snapshot: SequenceNumber) -> Lookup<'_> {
        let mut cursor = self.entries.cursor();
        SCRATCH.with_borrow_mut(|internal| {
            internal.clear();
            encode_internal(key, seek_trailer(snapshot), internal);
            cursor.seek(internal);
        });
        if !cursor.valid() {
            return Lookup::Absent;
        }
        let Ok(found) = InternalKeyRef::parse(cursor.key()) else {
            return Lookup::Absent;
        };
        if found.user_key() != key {
            return Lookup::Absent;
        }
        match found.trailer().kind() {
            Ok(ValueKind::Value) => Lookup::Value(cursor.value()),
            Ok(ValueKind::Delete) => Lookup::Deleted,
            Ok(ValueKind::RangeDelete) | Err(_) => Lookup::Absent,
        }
    }

    /// Every version in the table, in internal-key order.
    pub(crate) fn iter(&self) -> MemtableIter<'_> {
        MemtableIter::new(self.entries.cursor())
    }

    /// Bytes the table occupies, including arena waste; an estimate under concurrent inserts.
    #[cfg(test)]
    pub(crate) fn approximate_bytes(&self) -> u64 {
        self.entries.allocated_bytes()
    }
}

/// The engine error for an arena that could not take an entry.
fn arena_error(error: ArenaError) -> Error {
    Error::Memtable {
        reason: match error {
            ArenaError::Exhausted => "arena address space exhausted",
            ArenaError::TooLarge => "entry larger than a record can describe",
            _ => "internal arena error",
        },
    }
}
