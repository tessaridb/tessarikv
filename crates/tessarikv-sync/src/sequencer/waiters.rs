//! Writers waiting for their batch to become visible.

use super::Sequencer;
use crate::shim::Ordering;

impl Sequencer {
    /// Counts one more writer waiting for its batch to become visible; called before the
    /// writer's first [`Sequencer::covers`].
    pub fn add_waiter(&self) {
        // Relaxed: published by the Release half of the waiter's next `covers`.
        self.waiters.fetch_add(1, Ordering::Relaxed);
    }

    /// Counts one fewer waiting writer.
    pub fn remove_waiter(&self) {
        // Relaxed: a completer that still reads the old count wakes nobody in particular, which
        // costs a spurious wake-up and nothing else.
        self.waiters.fetch_sub(1, Ordering::Relaxed);
    }

    /// Whether `visible` has reached `last`, reading its LATEST value (see the type's
    /// documentation on waiting writers).
    pub fn covers(&self, last: u64) -> bool {
        // AcqRel read-modify-write: joins the chain every advancing compare-and-swap reads, so an
        // advance after it sees the waiter count, and reads the latest advance before it (whose
        // batches' effects are then visible to this thread).
        self.visible.fetch_add(0, Ordering::AcqRel) >= last
    }

    /// Whether a writer waits for visibility; asked by a completer after its completion.
    pub fn has_waiters(&self) -> bool {
        // Relaxed: the completer's advancing compare-and-swap comes before this load, and either
        // synchronizes with a waiter's `covers` (making its count visible here) or the waiter
        // reads the advance and does not need waking.
        self.waiters.load(Ordering::Relaxed) > 0
    }
}
