//! Sequence numbers for commits, and the highest sequence readers may see.

mod advance;

#[cfg(all(test, not(loom)))]
mod tests;

#[cfg(all(test, loom))]
mod loom_tests;

use crossbeam_utils::CachePadded;
use tessarikv_constants::MAX_SEQUENCE;

use crate::shim::{AtomicU64, Ordering};

/// Hands each commit a range of sequence numbers and tracks which prefix of them readers may see.
///
/// A writer claims as many sequence numbers as its batch has entries, applies the batch, then
/// completes the claim. [`Sequencer::visible`] is the largest sequence such that every sequence up
/// to it belongs to a completed batch, so a reader whose snapshot is that value sees whole batches
/// only, never a batch some of whose entries are still being applied.
///
/// Specification: `claim(n)` assigns the next `n` sequences to a new batch; `complete(c)` marks
/// it done; `visible()` returns the longest completed prefix. `visible` never decreases, and
/// everything a writer did before completing its claim happens before any read of a `visible`
/// that covers it.
///
/// | Operation | Linearization point | Progress |
/// |---|---|---|
/// | `claim` | the `fetch_add` on the next sequence | wait-free |
/// | `switch` | the `fetch_add` that starts the next epoch | wait-free |
/// | `complete` | the store marking the batch's ring slot ready; the batch becomes visible at the compare-and-swap on `visible` that passes it, by this thread or a later completer | lock-free once the slot is free |
/// | `visible` | the Acquire load of `visible` | wait-free |
///
/// Two windows block. A completion waits while its ring slot still holds a batch that started at
/// least `capacity` sequences earlier and is not yet visible (backpressure on a writer far ahead).
/// And visibility is a prefix: a writer stalled between claiming and completing holds back every
/// later batch until it completes.
///
/// Epochs: the counter carries an epoch next to the next sequence, so a claim learns in the same
/// read-modify-write which epoch its sequences belong to. [`Sequencer::switch`] starts the next
/// epoch: every claim before it in the counter's order has the old epoch and sequences below the
/// switch's boundary, every claim after it has the new epoch and sequences from the boundary on.
/// Epochs count modulo 128, so no more than 128 epochs may be in use at once. The switch is
/// Release and a claim is Acquire; claims are read-modify-writes, so they continue the switch's
/// release sequence, and a writer whose claim carries the new epoch sees everything the switcher
/// did before switching. The sequence field is one bit wider than the largest sequence, so the
/// claims refused after exhaustion cannot carry into the epoch for another 2^56 sequences.
///
/// Ordering: a completer stores its slot (Release) and then reads `visible` with a
/// read-modify-write, which always reads the newest value. An advancer moves `visible` with a
/// compare-and-swap and then probes the next slot (Acquire). Because both sides write `visible`
/// with read-modify-writes, whichever comes second in its modification order synchronizes with
/// the first: either the advancer sees the slot, or the completer sees the advance and probes
/// the slot itself, so no completed batch is left behind. The compare-and-swap also publishes the
/// completed batches to readers, whose Acquire load of `visible` makes everything those writers
/// did before completing visible to them.
///
/// Reclamation: the ring is fixed at construction and freed with the sequencer. A slot is reused
/// only after its previous batch is visible, and the starts stored in one slot strictly increase,
/// so a stale slot never matches the sequence being probed.
pub struct Sequencer {
    next: CachePadded<AtomicU64>,
    visible: CachePadded<AtomicU64>,
    slots: Box<[Slot]>,
    mask: u64,
}

/// A completed-batch record in the ring.
struct Slot {
    /// The batch's first sequence; stored last, when the batch is done.
    start: AtomicU64,
    /// The batch's last sequence.
    end: AtomicU64,
}

/// A range of sequence numbers claimed for one batch, to be completed exactly once.
#[derive(Debug, PartialEq, Eq)]
#[must_use = "a claim that is never completed holds back every later commit"]
pub struct Claim {
    epoch: u8,
    first: u64,
    last: u64,
}

/// The start of an epoch, as returned by [`Sequencer::switch`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Switch {
    epoch: u8,
    boundary: u64,
}

impl Switch {
    /// The epoch the switch started.
    pub fn epoch(&self) -> u8 {
        self.epoch
    }

    /// The first sequence number of the new epoch; above the largest sequence number once the
    /// sequence space is exhausted.
    pub fn boundary(&self) -> u64 {
        self.boundary
    }
}

/// Bits of the counter below the epoch.
const SEQUENCE_FIELD_BITS: u32 = 57;
/// Masks the next sequence out of the counter.
const SEQUENCE_FIELD: u64 = (1 << SEQUENCE_FIELD_BITS) - 1;
/// Adds one to the counter's epoch; the epoch wraps with the counter.
const EPOCH_STEP: u64 = 1 << SEQUENCE_FIELD_BITS;
/// Keeps an epoch inside the counter's seven epoch bits.
const EPOCH_MASK: u8 = 0x7f;

/// The epoch a switch out of `epoch` starts; epochs count modulo 128.
pub fn next_epoch(epoch: u8) -> u8 {
    epoch.wrapping_add(1) & EPOCH_MASK
}

/// The epoch and the next sequence held in one counter value.
fn unpack(word: u64) -> (u8, u64) {
    let epoch = u8::try_from(word >> SEQUENCE_FIELD_BITS).unwrap_or(u8::MAX);
    (epoch, word & SEQUENCE_FIELD)
}

impl Claim {
    /// The epoch the batch's sequences belong to.
    pub fn epoch(&self) -> u8 {
        self.epoch
    }

    /// The batch's first sequence number.
    pub fn first(&self) -> u64 {
        self.first
    }

    /// The batch's last sequence number.
    pub fn last(&self) -> u64 {
        self.last
    }
}

impl Sequencer {
    /// A sequencer whose last committed sequence is `last`, with a ring of `capacity` slots
    /// (rounded up to a power of two, at least 2).
    pub fn new(last: u64, capacity: usize) -> Self {
        let capacity = capacity.max(2).next_power_of_two();
        Self {
            next: CachePadded::new(AtomicU64::new(last.saturating_add(1))),
            visible: CachePadded::new(AtomicU64::new(last)),
            slots: (0..capacity)
                .map(|_| Slot {
                    start: AtomicU64::new(0),
                    end: AtomicU64::new(0),
                })
                .collect(),
            mask: u64::try_from(capacity).map_or(1, |capacity| capacity.wrapping_sub(1)),
        }
    }

    /// Claims the next `count` sequence numbers; `None` when `count` is zero or the claim would
    /// pass the largest sequence number (the counter then stays exhausted).
    pub fn claim(&self, count: u64) -> Option<Claim> {
        if count == 0 {
            return None;
        }
        // Acquire: continues the release sequence of the latest switch, so a claim in its epoch
        // sees what the switcher did before switching. The range itself is disjoint by the RMW.
        let (epoch, first) = unpack(self.next.fetch_add(count, Ordering::Acquire));
        let last = first.checked_add(count.wrapping_sub(1))?;
        (last <= MAX_SEQUENCE).then_some(Claim { epoch, first, last })
    }

    /// Starts the next epoch at the next unclaimed sequence. One caller at a time: the epoch it
    /// returns is the one later claims carry, so whatever must exist for that epoch is published
    /// before the call.
    pub fn switch(&self) -> Switch {
        // Release: publishes what the switcher did before switching to every claim of the new
        // epoch (their Acquire RMWs read this value or a later one in its release sequence).
        let (epoch, boundary) = unpack(self.next.fetch_add(EPOCH_STEP, Ordering::Release));
        Switch {
            epoch: next_epoch(epoch),
            boundary,
        }
    }

    /// The largest sequence number every earlier sequence of which is in a completed batch.
    pub fn visible(&self) -> u64 {
        // Acquire: pairs with the advancing compare-and-swap; the completed batches' effects
        // happen before what the caller reads next.
        self.visible.load(Ordering::Acquire)
    }

    /// The ring slot for a batch starting at `start`; always present, since the mask keeps the
    /// index inside the ring.
    fn slot(&self, start: u64) -> Option<&Slot> {
        self.slots.get(usize::try_from(start & self.mask).ok()?)
    }
}
