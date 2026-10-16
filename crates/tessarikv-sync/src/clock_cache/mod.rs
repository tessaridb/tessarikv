//! The byte-bounded CLOCK cache; its contract is documented on [`ClockCache`].

#[cfg(all(test, not(loom)))]
mod tests;

#[cfg(all(test, not(loom)))]
mod stress_tests;

#[cfg(all(test, loom))]
mod loom_tests;

mod evict;
mod slot;

use std::hash::{BuildHasher, Hash};

use crossbeam_utils::CachePadded;

use crate::shim::{AtomicU64, Ordering};
use slot::{Entry, Slot};

/// The most slots a lookup or an insert probes from a key's home slot.
const WINDOW: u64 = 16;
/// The largest table, in slots.
const MAX_SLOTS: u64 = 1 << 30;
/// The largest capacity honoured: far beyond any memory, small enough that the in-flight
/// charges of every inserting thread cannot overflow the count.
const MAX_CAPACITY: u64 = 1 << 48;

/// Hashes keys: randomly seeded, except under `loom`, whose executions must replay exactly.
#[cfg(not(loom))]
type KeyHasher = std::collections::hash_map::RandomState;
#[cfg(loom)]
type KeyHasher = std::hash::BuildHasherDefault<std::collections::hash_map::DefaultHasher>;

/// What [`ClockCache::insert`] did with an entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Admission {
    /// The entry is held and may be read back.
    Admitted,
    /// An entry for the key was already held; the new one was dropped.
    AlreadyCached,
    /// The entry was dropped: it is larger than the cache, the cache holds nothing, or no entry
    /// could be evicted to make room because readers or writers held every candidate.
    Refused,
}

/// A byte-bounded cache whose reads never wait: second-chance CLOCK eviction over an
/// open-addressed array of slots.
///
/// Sequential specification: the cache holds entries `(key, value, charge)` whose charges sum to
/// at most its capacity. `get(k)` returns the value of an entry for `k`, or nothing; `insert(k, v,
/// c)` adds the entry unless one for `k` is already held, first evicting entries the clock has
/// not seen read since its last pass. A cache may forget: a miss is always a permitted answer and
/// an insert may be refused, but a value returned is always one inserted for that key, and in a
/// single thread an insert that fits is always admitted and then read back. One value per key is
/// the caller's contract (it holds for immutable data such as table blocks); two racing inserts of
/// one key may leave two copies, which are counted and evicted independently.
///
/// | Operation | Outcome | Linearization point | Progress |
/// |---|---|---|---|
/// | `get` | hit | the reader's increment of the slot's word that saw it published, key checked after | wait-free: at most one probe window, three read-modify-writes per matching slot |
/// | `get` | miss | any instant (permitted outcome) | wait-free |
/// | `insert` | admitted | the read-modify-write that publishes the slot | wait-free: a bounded number of clock steps and slot claims |
/// | `insert` | already cached | the reader's increment that found the key | wait-free |
/// | `insert` | refused | its return (permitted outcome) | wait-free |
///
/// Each slot has one word holding its state (empty, busy, ready), a referenced bit and a count of
/// readers. Every write to that word is a read-modify-write, so every release on it heads a
/// release sequence the later ones extend. A reader increments the count with Acquire and touches
/// the entry only when the increment saw the slot ready; it decrements with Release. An evictor
/// takes a ready slot with no readers by an Acquire compare-and-swap to busy, so every reader's
/// access happens before the entry is dropped; an inserter writes the entry while the slot is busy
/// and publishes it with a Release increment to ready. Every handshake meets on one location, so
/// no sequentially consistent access is needed.
///
/// Reclamation: the entry lives inside its slot and is dropped only by the thread whose
/// compare-and-swap took the slot from ready with no readers; readers receive clones (an `Arc`
/// clone for shared data), so a value outlives its eviction. ABA cannot do harm: every
/// compare-and-swap is valid on the state it compares, whatever the slot held before. A reader
/// frozen mid-read pins one slot, which eviction then skips; nothing ever waits for it.
///
pub struct ClockCache<K, V> {
    slots: Box<[Slot<K, V>]>,
    /// The slot count minus one; slot counts are powers of two.
    mask: u64,
    /// Slots probed per lookup: `WINDOW`, or fewer in a smaller table.
    window: u64,
    /// The most clock steps one insert takes: two full sweeps and a window.
    sweep_limit: u64,
    capacity: u64,
    hasher: KeyHasher,
    /// Charge counted: every held entry, plus the reservations of inserts in flight.
    used: CachePadded<AtomicU64>,
    hand: CachePadded<AtomicU64>,
}

// SAFETY: entries are reached only through the slot protocol, which orders every write of an
// entry before any read of it and lets no two threads write one; keys and values are read through
// shared references from many threads (`Sync`) and dropped by whichever thread evicts (`Send`).
unsafe impl<K: Send + Sync, V: Send + Sync> Sync for ClockCache<K, V> {}

impl<K, V> std::fmt::Debug for ClockCache<K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClockCache")
            .field("capacity", &self.capacity)
            .field("slots", &self.slots.len())
            .finish_non_exhaustive()
    }
}

impl<K: Copy + Eq + Hash, V: Clone> ClockCache<K, V> {
    /// A cache holding at most `capacity` charge units (at most 2^48), sized for entries of
    /// about `typical_charge` each; a capacity of zero holds nothing.
    pub fn new(capacity: u64, typical_charge: u64) -> Self {
        let capacity = capacity.min(MAX_CAPACITY);
        let count = if capacity == 0 {
            0
        } else {
            // Twice the entries the capacity holds at the typical size keeps windows sparse.
            let entries = capacity
                .checked_div(typical_charge)
                .unwrap_or(capacity)
                .max(1);
            entries.saturating_mul(2).min(MAX_SLOTS).next_power_of_two()
        };
        let slots: Box<[Slot<K, V>]> = (0..count).map(|_| Slot::new()).collect();
        let window = WINDOW.min(count);
        Self {
            slots,
            mask: count.saturating_sub(1),
            window,
            sweep_limit: count.saturating_mul(2).saturating_add(window),
            capacity,
            hasher: KeyHasher::default(),
            used: CachePadded::new(AtomicU64::new(0)),
            hand: CachePadded::new(AtomicU64::new(0)),
        }
    }

    /// The value held for `key`, if the cache has it.
    pub fn get(&self, key: &K) -> Option<V> {
        let hash = self.hasher.hash_one(key);
        self.window(hash)
            .filter(|slot| slot.may_hold(hash))
            .find_map(|slot| slot.read(key, V::clone))
    }

    /// Offers `value` for `key`, charged `charge` units against the capacity, evicting entries
    /// not read since the clock last passed them to make room.
    pub fn insert(&self, key: K, value: V, charge: u64) -> Admission {
        let hash = self.hasher.hash_one(key);
        let present = self
            .window(hash)
            .filter(|slot| slot.may_hold(hash))
            .any(|slot| slot.read(&key, |_| ()).is_some());
        if present {
            return Admission::AlreadyCached;
        }
        if self.slots.is_empty() || charge > self.capacity {
            return Admission::Refused;
        }
        // Relaxed: accounting only; the reservation is made before the entry exists, so the
        // count is never below the charge of what is alive.
        self.used.fetch_add(charge, Ordering::Relaxed);
        let entry = Entry { key, value, charge };
        if !self.make_room() {
            self.refund(charge);
            return Admission::Refused;
        }
        let claimed = match self.window(hash).find_map(Slot::claim_empty) {
            Some(held) => Some(held),
            // Every slot of the window is in use: replace an entry nobody is reading, sparing
            // the ones read since the clock last passed (which this pass clears) when it can.
            None => self
                .window(hash)
                .find_map(|slot| slot.take_unread(true))
                .or_else(|| self.window(hash).find_map(|slot| slot.take_unread(false))),
        };
        let Some(held) = claimed else {
            self.refund(charge);
            return Admission::Refused;
        };
        let replaced = held.take();
        held.fill(hash, entry);
        self.discard(replaced);
        Admission::Admitted
    }

    /// Charge units held, counting inserts in flight: an estimate while other threads insert
    /// or evict, exact when none do.
    pub fn used(&self) -> u64 {
        // Relaxed: a statistic.
        self.used.load(Ordering::Relaxed)
    }

    /// The slot a position falls on.
    fn slot(&self, position: u64) -> Option<&Slot<K, V>> {
        usize::try_from(position & self.mask)
            .ok()
            .and_then(|index| self.slots.get(index))
    }

    /// The slots probed for a key of this hash, from its home slot on.
    fn window(&self, hash: u64) -> impl Iterator<Item = &Slot<K, V>> {
        (0..self.window).filter_map(move |offset| self.slot(hash.wrapping_add(offset)))
    }

    /// The keys and charges held. Only for tests, with no other thread using the cache.
    #[cfg(test)]
    fn entries(&self) -> Vec<(K, u64)> {
        self.slots.iter().filter_map(Slot::entry_if_ready).collect()
    }
}
