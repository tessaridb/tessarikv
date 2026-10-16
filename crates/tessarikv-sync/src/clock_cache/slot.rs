//! One slot of the cache: its word (state, referenced bit, reader count) and its entry.
//!
//! The word is only ever changed by read-modify-writes. The state moves EMPTY → BUSY (claimed by
//! one thread) → READY (published) → BUSY (taken by one thread, no readers) → EMPTY or READY
//! again. A reader adds itself to the count whatever the state and reads the entry only when the
//! value its increment returned shows READY; while any reader is counted the slot cannot be taken.

use crate::shim::{AtomicU64, Cell, Ordering};

/// The state bits of the word.
const STATE: u64 = 0b11;
/// No entry: the slot may be claimed.
const EMPTY: u64 = 0;
/// One thread owns the entry; readers back off.
const BUSY: u64 = 1;
/// The entry is published and may be read.
const READY: u64 = 2;
/// The entry was read since the clock last passed it.
const REFERENCED: u64 = 1 << 2;
/// One counted reader; the count occupies the bits from here up and cannot overflow, since a
/// thread holds at most one count at a time.
const READER: u64 = 1 << 3;

/// What a slot holds.
pub(super) struct Entry<K, V> {
    pub(super) key: K,
    pub(super) value: V,
    pub(super) charge: u64,
}

/// See the module documentation.
pub(super) struct Slot<K, V> {
    meta: AtomicU64,
    /// The hash of the entry's key, a filter readers check before counting themselves.
    hash: AtomicU64,
    entry: Cell<Option<Entry<K, V>>>,
}

/// A slot this thread holds BUSY: until it is filled or emptied no other thread touches the
/// entry.
pub(super) struct Held<'slot, K, V> {
    slot: &'slot Slot<K, V>,
}

impl<K: Eq, V> Slot<K, V> {
    /// An empty slot.
    pub(super) fn new() -> Self {
        Self {
            meta: AtomicU64::new(EMPTY),
            hash: AtomicU64::new(0),
            entry: Cell::new(None),
        }
    }

    /// Whether the slot looks published with an entry of this hash; a filter only.
    pub(super) fn may_hold(&self, hash: u64) -> bool {
        // Acquire: pairs with the publication, so the hash read next is that entry's or newer.
        let meta = self.meta.load(Ordering::Acquire);
        // Relaxed: ordered after the publication by the load above; a stale value costs a miss.
        meta & STATE == READY && self.hash.load(Ordering::Relaxed) == hash
    }

    /// Runs `read` over the value when the slot holds a published entry for `key`, marking it
    /// referenced.
    pub(super) fn read<R>(&self, key: &K, read: impl FnOnce(&V) -> R) -> Option<R> {
        // Acquire: reads a value in the release sequence of the publication, so the entry
        // written before it is visible; the count keeps the slot from being taken meanwhile.
        let seen = self.meta.fetch_add(READER, Ordering::Acquire);
        let found = if seen & STATE == READY {
            // SAFETY: the increment saw READY, so the entry's write happens-before this read;
            // the slot can be taken (and the entry changed) only by a compare-and-swap expecting
            // no readers, which fails until the decrement below.
            unsafe {
                self.entry.with(|entry| {
                    entry
                        .as_ref()
                        .filter(|entry| entry.key == *key)
                        .map(|entry| read(&entry.value))
                })
            }
        } else {
            None
        };
        if found.is_some() && seen & REFERENCED == 0 {
            // Relaxed: a policy bit; it publishes nothing.
            self.meta.fetch_or(REFERENCED, Ordering::Relaxed);
        }
        // Release: this read of the entry happens-before the drop by whoever takes the slot.
        self.meta.fetch_sub(READER, Ordering::Release);
        found
    }

    /// Claims the slot if it is empty.
    pub(super) fn claim_empty(&self) -> Option<Held<'_, K, V>> {
        // Relaxed: only a guess; the compare-and-swap decides.
        let seen = self.meta.load(Ordering::Relaxed);
        if seen & STATE != EMPTY {
            return None;
        }
        // Acquire on success: the previous occupant's removal happens-before the new write.
        // Readers counted on an empty slot never read the entry, so their count may stand.
        self.meta
            .compare_exchange(seen, seen | BUSY, Ordering::Acquire, Ordering::Relaxed)
            .ok()
            .map(|_| Held { slot: self })
    }

    /// Takes the slot if it is published and no reader is counted. With `second_chance`, an
    /// entry read since the clock last passed is spared once instead: its referenced bit is
    /// cleared.
    pub(super) fn take_unread(&self, second_chance: bool) -> Option<Held<'_, K, V>> {
        // Relaxed: only a guess; the compare-and-swap decides.
        let seen = self.meta.load(Ordering::Relaxed);
        if seen & STATE != READY || seen >= READER {
            return None;
        }
        if second_chance && seen & REFERENCED != 0 {
            // Relaxed: a policy bit; it publishes nothing.
            self.meta.fetch_and(!REFERENCED, Ordering::Relaxed);
            return None;
        }
        let busy = (seen & !(STATE | REFERENCED)) | BUSY;
        // Acquire on success: every counted reader's release and the publication of the entry
        // happen-before this thread drops or replaces it. The expected value has no readers.
        self.meta
            .compare_exchange(seen, busy, Ordering::Acquire, Ordering::Relaxed)
            .ok()
            .map(|_| Held { slot: self })
    }

    /// The published entry's key and charge. Only with no other thread using the slot.
    #[cfg(test)]
    pub(super) fn entry_if_ready(&self) -> Option<(K, u64)>
    where
        K: Copy,
    {
        if self.meta.load(Ordering::Acquire) & STATE != READY {
            return None;
        }
        // SAFETY: no other thread uses the slot, per this method's contract.
        unsafe {
            self.entry
                .with(|entry| entry.as_ref().map(|e| (e.key, e.charge)))
        }
    }
}

impl<K, V> Held<'_, K, V> {
    /// Takes the entry out of the slot.
    pub(super) fn take(&self) -> Option<Entry<K, V>> {
        // SAFETY: the slot is BUSY and held by this thread; the claim's Acquire ordered every
        // earlier access to the entry before this one.
        unsafe { self.slot.entry.with_mut(Option::take) }
    }

    /// Stores `entry` and publishes the slot.
    pub(super) fn fill(self, hash: u64, entry: Entry<K, V>) {
        // SAFETY: as in `take`.
        unsafe { self.slot.entry.with_mut(|slot| *slot = Some(entry)) };
        // Relaxed: published by the increment below.
        self.slot.hash.store(hash, Ordering::Relaxed);
        // Release: publishes the entry and the hash; readers' counts extend this sequence.
        self.slot.meta.fetch_add(READY - BUSY, Ordering::Release);
    }

    /// Releases the slot empty.
    pub(super) fn empty(self) {
        // Release: the removal of the entry happens-before the next claimant's write.
        self.slot.meta.fetch_sub(BUSY, Ordering::Release);
    }
}
