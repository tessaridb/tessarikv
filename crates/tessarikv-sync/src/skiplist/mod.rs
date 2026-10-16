//! The memtable's ordered store: an insert-only lock-free skiplist over arena-allocated entries.

mod cursor;
mod height;
mod insert;
mod node;
mod search;

#[cfg(all(test, not(loom)))]
mod tests;

#[cfg(all(test, not(loom), not(miri)))]
mod stress_tests;

#[cfg(all(test, not(loom), not(miri)))]
mod bench_tests;

#[cfg(all(test, loom))]
mod loom_tests;

use crossbeam_utils::CachePadded;
use tessarikv_constants::{ARENA_CHUNK_BYTES, SKIPLIST_MAX_HEIGHT};

use crate::arena::ByteArena;
use crate::error::ArenaError;
use crate::shim::AtomicU64;

pub use cursor::{Cursor, CursorPosition};

// The default chunk size satisfies the arena's check, which is what makes `SkipList::new` infallible.
const _: () = assert!(ARENA_CHUNK_BYTES.is_power_of_two() && ARENA_CHUNK_BYTES >= 64);

/// The order of keys in a [`SkipList`]. Must be a total order, and stable for the list's life.
pub trait KeyComparator: Send + Sync {
    /// How `a` compares with `b`.
    fn compare(&self, a: &[u8], b: &[u8]) -> std::cmp::Ordering;
}

/// Orders keys by their bytes.
#[derive(Debug, Clone, Copy, Default)]
pub struct BytewiseComparator;

impl KeyComparator for BytewiseComparator {
    fn compare(&self, a: &[u8], b: &[u8]) -> std::cmp::Ordering {
        a.cmp(b)
    }
}

/// What an insert did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Insert {
    /// The entry was added.
    Inserted,
    /// An equal key was already present; nothing changed.
    AlreadyPresent,
}

/// An insert-only concurrent skiplist whose entries live in a lock-free arena: the ordered store
/// of a memtable.
///
/// Sequential specification (an insert-only ordered map from key bytes to value bytes, ordered by
/// the [`KeyComparator`]): `insert(k, v)` adds `k ↦ v` when no key equal to `k` is present and
/// returns [`Insert::Inserted`], otherwise changes nothing and returns [`Insert::AlreadyPresent`];
/// `get(k)` returns the value of the key equal to `k`; a [`Cursor`] seeks to the least key not less
/// than a target, to the first or last key, and steps to the next or previous key. Entries are
/// never removed or changed.
///
/// | Operation | Outcome | Linearization point | Progress |
/// |---|---|---|---|
/// | `insert` | `Inserted` | the successful level-0 CAS that links the node | lock-free |
/// | `insert` | `AlreadyPresent` | the Acquire load at level 0 that reached the equal key | lock-free |
/// | `get`, `seek` | found / positioned | the Acquire load at level 0 that reached the node | lock-free; wait-free without concurrent inserts |
/// | `get`, `seek` | absent / past the end | the level-0 Acquire load returning a greater key or null | lock-free |
/// | `next` | none | the Acquire load of the node's level-0 link | wait-free |
/// | `prev`, `seek_to_last` | none | a fresh search, as `seek` | lock-free |
///
/// `insert`, `get` and `seek` are linearizable, where "precedes" is happens-before. Cursors are
/// weakly consistent: stepping may meet keys inserted after the cursor was created, never a key
/// out of order and never a partly written entry.
///
/// Ordering: a node's record and tower are written before the Release CAS that links it at level
/// 0 (and its tower slot `i` before the Release CAS linking it at level `i`); every traversal loads
/// links with Acquire; a lost CAS discards the value it read and searches again with Acquire
/// loads, so it needs no ordering of its own. `max_height` is Relaxed: it only chooses where a search starts. No store→load pairs.
///
/// Reclamation: no node is ever freed or rewritten while the list is shared. Records, towers
/// included, live in an arena dropped with the list, so a loaded reference stays valid and is never reused
/// (no ABA). A losing inserter's allocation is simply left unused.
///
/// # Example
///
/// ```
/// use tessarikv_sync::{BytewiseComparator, Insert, SkipList};
///
/// let list = SkipList::new(BytewiseComparator);
/// assert_eq!(list.insert(b"apple", b"red").unwrap(), Insert::Inserted);
/// assert_eq!(list.insert(b"apple", b"green").unwrap(), Insert::AlreadyPresent);
/// assert_eq!(list.get(b"apple"), Some(&b"red"[..]));
///
/// let mut cursor = list.cursor();
/// cursor.seek(b"a");
/// assert_eq!(cursor.key(), b"apple");
/// ```
pub struct SkipList<C> {
    comparator: C,
    head: [AtomicU64; SKIPLIST_MAX_HEIGHT],
    max_height: CachePadded<AtomicU64>,
    bytes: ByteArena,
    /// Never shared by two lists of one process: a [`CursorPosition`] resumes only in the list
    /// it came from.
    id: u64,
}

/// The source of list identities. Outside the loom model on purpose: it orders nothing, and
/// uniqueness needs only the atomicity of the increment.
static NEXT_LIST_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

impl<C: KeyComparator> SkipList<C> {
    /// An empty list ordered by `comparator`.
    pub fn new(comparator: C) -> Self {
        // The const assertions above prove the default sizes valid.
        match Self::with_chunk_size(comparator, ARENA_CHUNK_BYTES) {
            Ok(list) => list,
            Err(_) => unreachable!("the default arena chunk size is valid"),
        }
    }

    /// An empty list whose arena takes memory in chunks of `chunk_bytes`, a power of two of at
    /// least 64; an entry larger than a quarter chunk gets an allocation of its own.
    ///
    /// # Errors
    ///
    /// [`ArenaError::TooLarge`] for a size that is not an accepted power of two.
    pub fn with_chunk_size(comparator: C, chunk_bytes: usize) -> Result<Self, ArenaError> {
        Ok(Self {
            comparator,
            head: std::array::from_fn(|_| AtomicU64::new(0)),
            max_height: CachePadded::new(AtomicU64::new(1)),
            bytes: ByteArena::new(chunk_bytes)?,
            // Relaxed: only the increment's atomicity matters (2^64 lists are never made).
            id: NEXT_LIST_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        })
    }

    /// Bytes the list's records occupy, including arena waste; an estimate under concurrency.
    pub fn allocated_bytes(&self) -> u64 {
        self.bytes.allocated_bytes()
    }
}
