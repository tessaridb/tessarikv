//! Completing a claim and advancing the visible prefix.

use crossbeam_utils::Backoff;

use super::{Claim, Sequencer};
use crate::shim::Ordering;

impl Sequencer {
    /// Marks `claim`'s batch done and advances the visible prefix as far as completed batches
    /// allow. Everything this thread did before the call happens before any read of a `visible`
    /// value that covers the batch. Returns `true` when this call moved `visible` to or past the
    /// watched target (see [`Sequencer::watch`]).
    pub fn complete(&self, claim: Claim) -> bool {
        let capacity = self.mask.saturating_add(1);
        let backoff = Backoff::new();
        // Acquire: pairs with the advance that made the slot's previous batch visible.
        while self
            .visible
            .load(Ordering::Acquire)
            .saturating_add(capacity)
            < claim.first
        {
            wait(&backoff);
        }
        let Some(slot) = self.slot(claim.first) else {
            return false;
        };
        // Relaxed: read only after the Acquire load of `start` that sees the store below.
        slot.end.store(claim.last, Ordering::Relaxed);
        // Release: publishes the batch (and `end`) to whichever thread advances over it.
        slot.start.store(claim.first, Ordering::Release);
        // AcqRel read-modify-write: reads the LATEST `visible`. Either an advancer's later
        // compare-and-swap reads this write and so sees the slot stored above, or this thread
        // reads that advancer's value and probes the slot itself; no completion is left behind.
        let visible = self.visible.fetch_add(0, Ordering::AcqRel);
        self.advance(visible)
    }

    /// Moves `visible` over every completed batch that directly follows it; returns whether a
    /// move reached the watched target.
    fn advance(&self, mut visible: u64) -> bool {
        let mut reached = false;
        loop {
            let probe = visible.saturating_add(1);
            let Some(slot) = self.slot(probe) else {
                return reached;
            };
            // Acquire: pairs with the slot's Release store; the batch's writes and `end` are
            // then visible to this thread.
            if slot.start.load(Ordering::Acquire) != probe {
                return reached;
            }
            // Relaxed: ordered after the store it reads by the Acquire load of `start` above.
            let end = slot.end.load(Ordering::Relaxed);
            // AcqRel: publishes the batches to readers (Release) and reads the latest `visible`
            // (Acquire, continuing every release chain on it). Failure Acquire: the value read
            // is the next probe's start and may come from another advancer's publication.
            visible = match self.visible.compare_exchange(
                visible,
                end,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    // Relaxed: a target stored before a watcher's read-modify-write that this
                    // compare-and-swap follows is ordered before it by that chain.
                    reached |= self.target.load(Ordering::Relaxed) <= end;
                    end
                }
                Err(current) => current,
            };
        }
    }
}

/// Lets the batch that owns the slot finish: spins with a pause hint, then yields the thread.
fn wait(backoff: &Backoff) {
    #[cfg(loom)]
    {
        let _ = backoff;
        loom::thread::yield_now();
    }
    #[cfg(not(loom))]
    backoff.snooze();
}
