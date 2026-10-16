//! The clock: a shared hand that sweeps the slots, sparing an entry read since its last pass
//! once and evicting entries that were not.

use super::ClockCache;
use super::slot::{Entry, Held};
use crate::shim::Ordering;

impl<K: Copy + Eq + std::hash::Hash, V: Clone> ClockCache<K, V> {
    /// Sweeps until the charge held is within the capacity, or the sweep limit runs out (every
    /// candidate read, pinned by a reader or held by a writer). Returns whether it fits.
    pub(super) fn make_room(&self) -> bool {
        let mut steps = self.sweep_limit;
        // Relaxed: accounting only; a stale value costs one step more or less.
        while self.used.load(Ordering::Relaxed) > self.capacity {
            let Some(left) = steps.checked_sub(1) else {
                return false;
            };
            steps = left;
            self.hand_step();
        }
        true
    }

    /// Moves the hand one slot and evicts or spares what it finds there.
    fn hand_step(&self) {
        // Relaxed: the hand only spreads sweepers over the slots; it publishes nothing.
        let position = self.hand.fetch_add(1, Ordering::Relaxed);
        if let Some(held) = self.slot(position).and_then(|slot| slot.take_unread(true)) {
            self.evict(held);
        }
    }

    /// Empties a slot this thread took and gives back its entry's charge.
    pub(super) fn evict(&self, held: Held<'_, K, V>) {
        let entry = held.take();
        held.empty();
        self.discard(entry);
    }

    /// Drops an evicted entry, then stops counting its charge, so the count never falls below
    /// what is still alive.
    pub(super) fn discard(&self, entry: Option<Entry<K, V>>) {
        if let Some(entry) = entry {
            let charge = entry.charge;
            drop(entry);
            self.refund(charge);
        }
    }

    /// Stops counting `charge`.
    pub(super) fn refund(&self, charge: u64) {
        // Relaxed: accounting only.
        self.used.fetch_sub(charge, Ordering::Relaxed);
    }
}
