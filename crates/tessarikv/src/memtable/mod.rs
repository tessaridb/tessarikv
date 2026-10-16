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
//!
//! A whole-key bloom filter beside the skiplist lets a read of a key the table does not hold
//! skip the walk. An insert sets the key's bits before it links the entry, and the entry becomes
//! visible to a read only through the publication of its sequence, which comes later still; so
//! a read whose snapshot can see an entry finds its key's bits. A table that outgrows its filter
//! answers correctly and only skips less.

mod iter;

#[cfg(test)]
mod tests;

use std::cell::RefCell;
use std::cmp::Ordering;

use tessarikv_format::key_hash;
use tessarikv_sync::{ArenaError, ConcurrentBloom, KeyComparator, SkipList};
use tessarikv_types::{
    InternalKeyRef, SequenceNumber, Trailer, ValueKind, compare_internal, encode_internal,
    seek_trailer,
};

use crate::tombstones::RangeTombstone;
use crate::{Error, Result};

pub(crate) use iter::{MemtableCursor, MemtableEntry, MemtableIter};

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
    /// Range deletes, apart from the point entries: key internal(start, sequence), value end.
    range_deletes: SkipList<InternalKeyComparator>,
    /// The user keys of the point entries, values and deletes alike.
    keys: ConcurrentBloom,
    /// The bytes it holds before it is frozen: its keyspace's in-memory table size.
    budget: u64,
}

/// Table bytes per filter bit: about 1.5 % of the table, some 19 bits per 150-byte entry.
const BYTES_PER_FILTER_BIT: u64 = 8;

impl Memtable {
    /// An empty table with a filter for a small table, as tests use.
    #[cfg(test)]
    pub(crate) fn new() -> Self {
        Self::sized(64 * 1024)
    }

    /// An empty table frozen once it holds `table_bytes`, with a key filter to suit.
    pub(crate) fn sized(table_bytes: u64) -> Self {
        let bits = usize::try_from(table_bytes / BYTES_PER_FILTER_BIT).unwrap_or(usize::MAX);
        Self::with(bits, table_bytes)
    }

    /// An empty table of this one's size and filter, to take its place.
    pub(crate) fn successor(&self) -> Self {
        Self::with(self.keys.bits(), self.budget)
    }

    fn with(bits: usize, budget: u64) -> Self {
        Self {
            entries: SkipList::new(InternalKeyComparator),
            range_deletes: SkipList::new(InternalKeyComparator),
            keys: ConcurrentBloom::new(bits),
            budget,
        }
    }

    /// Whether it holds its size's worth of bytes and should be frozen.
    pub(crate) fn is_full(&self) -> bool {
        self.approximate_bytes() >= self.budget
    }

    /// Adds version `sequence` of `key`: a value, a delete (whose `value` is ignored), or a
    /// range delete from `key` to `value` (exclusive).
    ///
    /// # Errors
    ///
    /// [`Error::Memtable`] when the arena cannot take the entry.
    pub(crate) fn insert(
        &self,
        sequence: SequenceNumber,
        kind: ValueKind,
        key: &[u8],
        value: &[u8],
    ) -> Result<()> {
        let (list, value) = match kind {
            ValueKind::Value => (&self.entries, value),
            ValueKind::Delete => (&self.entries, &[][..]),
            ValueKind::RangeDelete => (&self.range_deletes, value),
        };
        if kind != ValueKind::RangeDelete {
            // Before the entry is linked: see the module documentation.
            self.keys.insert(key_hash(key));
        }
        SCRATCH
            .with_borrow_mut(|internal| {
                internal.clear();
                encode_internal(key, Trailer::new(sequence, kind), internal);
                list.insert(internal, value)
            })
            .map(|_| ())
            .map_err(arena_error)
    }

    /// The newest version of `key` whose sequence is at most `snapshot`.
    pub(crate) fn get(&self, key: &[u8], snapshot: SequenceNumber) -> Lookup<'_> {
        if !self.keys.may_contain(key_hash(key)) {
            return Lookup::Absent;
        }
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

    /// The table's range deletes, appended to `out`.
    pub(crate) fn tombstones(&self, out: &mut Vec<RangeTombstone>) {
        let mut cursor = self.range_deletes.cursor();
        cursor.seek_to_first();
        while cursor.valid() {
            if let Ok(start) = InternalKeyRef::parse(cursor.key()) {
                out.push(RangeTombstone {
                    start: start.user_key().to_vec(),
                    end: cursor.value().to_vec(),
                    sequence: start.trailer().sequence(),
                });
            }
            cursor.next();
        }
    }

    /// Whether the table holds a range delete.
    pub(crate) fn has_tombstones(&self) -> bool {
        let mut cursor = self.range_deletes.cursor();
        cursor.seek_to_first();
        cursor.valid()
    }

    /// Bytes the table occupies, including arena waste; an estimate under concurrent inserts.
    pub(crate) fn approximate_bytes(&self) -> u64 {
        self.entries
            .allocated_bytes()
            .saturating_add(self.range_deletes.allocated_bytes())
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
