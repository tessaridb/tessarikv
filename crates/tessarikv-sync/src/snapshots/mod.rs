//! Registered snapshots, and the horizon a compaction keeps versions for.
//!
//! Sequential specification: `register(visible)` records a snapshot at a sequence `visible()`
//! returned and hands back its registration; `release(r)` forgets it; `horizon(v)` returns `v`
//! and every snapshot registered and not released. Linearizable, with one addition that makes a
//! compaction safe: a registration that a concurrent `horizon(v)` does not return has a sequence
//! of at least `v`, so a compaction keeping the newest version at or below `v` (and every
//! version above it) keeps what that snapshot reads.
//!
//! | Operation | Linearization point | Progress |
//! |---|---|---|
//! | `register` | its last read-modify-write of `announced` (the check that passes) | lock-free: it retries only when a horizon was announced between its store and its check |
//! | `release` | the Release store freeing the slot | wait-free |
//! | `horizon` | its `fetch_max` on `announced` | lock-free in its slot scan |
//!
//! Protocol: a registration stores its sequence in a slot, then reads `announced` with a
//! read-modify-write. A horizon raises `announced` to its visible sequence with `fetch_max`, then
//! reads the slots. Both sides write `announced`, so they are ordered in its modification order:
//! if the registration's read-modify-write comes first, the horizon's `fetch_max` reads its value
//! and synchronizes with it, so the slot store happens before the slot scan and is seen; if the
//! `fetch_max` comes first, the registration reads a value at least the horizon's, and when that
//! is above its sequence it takes the visible sequence again (now at least the horizon, since it
//! is read after the announcement) and checks once more. No sequentially consistent access is
//! needed: the two sides meet on one location.
//!
//! Slots live in a lock-free growable directory and are never freed while the registry is
//! shared; a released slot is reused by a later registration, so the scan visits as many slots
//! as were ever registered at once.

#[cfg(all(test, not(loom)))]
mod tests;

#[cfg(all(test, loom))]
mod loom_tests;

use crossbeam_utils::CachePadded;

use crate::shim::{AtomicU64, Ordering};
use crate::slot_directory::SlotDirectory;

/// A slot holding no snapshot; no sequence number reaches it.
const FREE: u64 = u64::MAX;

/// See the module documentation.
pub struct SnapshotRegistry {
    slots: SlotDirectory<AtomicU64>,
    /// Slots ever handed out; every slot below it exists.
    len: CachePadded<AtomicU64>,
    /// The highest visible sequence a horizon has announced.
    announced: CachePadded<AtomicU64>,
}

/// A registered snapshot; release it with [`SnapshotRegistry::release`].
#[derive(Debug, PartialEq, Eq)]
#[must_use = "a registration that is never released keeps old versions forever"]
pub struct Registration {
    index: u64,
    sequence: u64,
}

impl Registration {
    /// The snapshot's sequence number.
    pub fn sequence(&self) -> u64 {
        self.sequence
    }
}

/// What a compaction must keep: everything a reader at `visible` or at any snapshot can see.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Horizon {
    visible: u64,
    snapshots: Vec<u64>,
}

impl Horizon {
    /// The visible sequence the horizon was taken at.
    pub fn visible(&self) -> u64 {
        self.visible
    }

    /// The registered snapshots, ascending.
    pub fn snapshots(&self) -> &[u64] {
        &self.snapshots
    }

    /// The oldest sequence any reader may see.
    pub fn earliest(&self) -> u64 {
        self.snapshots.first().copied().unwrap_or(self.visible)
    }
}

impl Default for SnapshotRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl SnapshotRegistry {
    /// An empty registry.
    pub fn new() -> Self {
        Self {
            slots: SlotDirectory::new(),
            len: CachePadded::new(AtomicU64::new(0)),
            announced: CachePadded::new(AtomicU64::new(0)),
        }
    }

    /// Registers a snapshot at the sequence `visible()` returns, taking it again when a horizon
    /// announced meanwhile is above it. `None` only when the slot index space is exhausted.
    pub fn register(&self, visible: impl Fn() -> u64) -> Option<Registration> {
        let mut sequence = visible();
        let (index, slot) = self.claim_slot(sequence)?;
        loop {
            // AcqRel read-modify-write: reads the newest announcement. A later `fetch_max` reads
            // this write and so sees the slot store before it; otherwise this reads its value.
            let announced = self.announced.fetch_add(0, Ordering::AcqRel);
            if announced <= sequence {
                return Some(Registration { index, sequence });
            }
            sequence = visible();
            // Relaxed: published by the read-modify-write of `announced` that follows.
            slot.store(sequence, Ordering::Relaxed);
        }
    }

    /// Forgets `registration`; its slot may be reused.
    pub fn release(&self, registration: Registration) {
        if let Some(slot) = self.slots.get(registration.index) {
            // Release: hands the slot over to the registration that claims it next.
            slot.store(FREE, Ordering::Release);
        }
    }

    /// Announces `visible` and returns it with every registered snapshot.
    pub fn horizon(&self, visible: u64) -> Horizon {
        // AcqRel: reads every earlier registration's announcement check, so their slot stores
        // happen before the scan below.
        let _ = self.announced.fetch_max(visible, Ordering::AcqRel);
        // Relaxed: a registration the chain above orders before this scan raised `len` before
        // its check, so coherence alone shows the raise here; later ones are not needed.
        let len = self.len.load(Ordering::Relaxed);
        let mut snapshots: Vec<u64> = (0..len)
            .filter_map(|index| self.slots.get(index))
            // Relaxed: ordered after the slot store by the `announced` chain above.
            .map(|slot| slot.load(Ordering::Relaxed))
            .filter(|sequence| *sequence != FREE)
            .collect();
        snapshots.sort_unstable();
        Horizon { visible, snapshots }
    }

    /// Slots ever handed out.
    #[cfg(all(test, not(loom)))]
    fn slots_in_use(&self) -> u64 {
        self.len.load(Ordering::Relaxed)
    }

    /// Takes a free slot (or a new one) and stores `sequence` in it.
    fn claim_slot(&self, sequence: u64) -> Option<(u64, &AtomicU64)> {
        // Relaxed: a free slot missed here only costs a new one.
        let len = self.len.load(Ordering::Relaxed);
        for index in 0..len {
            let Some(slot) = self.slots.get(index) else {
                continue;
            };
            // Acquire on success: orders this use after the release that freed the slot.
            if slot
                .compare_exchange(FREE, sequence, Ordering::Acquire, Ordering::Relaxed)
                .is_ok()
            {
                return Some((index, slot));
            }
        }
        let index = self.len_reserve()?;
        let slot = self
            .slots
            .get_or_install(index, || AtomicU64::new(sequence))?;
        Some((index, slot))
    }

    /// Reserves the next slot index.
    fn len_reserve(&self) -> Option<u64> {
        // Relaxed: the read-modify-write makes the index unique; the slot installed for it is
        // published by the directory's own Release install and found through `announced`.
        let index = self.len.fetch_add(1, Ordering::Relaxed);
        (index != u64::MAX).then_some(index)
    }
}
