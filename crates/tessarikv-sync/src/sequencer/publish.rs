//! Announcing a claim before it completes, and waiting for earlier claims to announce.

use crossbeam_utils::Backoff;

use super::advance::wait;
use super::{Claim, Sequencer};
use crate::shim::Ordering;

impl Sequencer {
    /// Announces `claim`: everything this thread did before the call happens before whatever a
    /// [`Sequencer::wait_published`] that passes the claim does next. Called once per claim,
    /// before the claim is completed.
    pub fn publish(&self, claim: &Claim) {
        let capacity = self.mask.saturating_add(1);
        let backoff = Backoff::new();
        // Acquire: as in `complete`; the slot's previous range is visible, so no reader needs it.
        while self.visible().saturating_add(capacity) < claim.first {
            wait(&backoff);
        }
        let Some(slot) = self.published_slot(claim.first) else {
            return;
        };
        // Release: a reader that reads this `end` while still seeing the slot's previous `start`
        // also sees, through the Acquire load of `visible` above, that the previous range is
        // visible, and so does not use this `end` for it (see `wait_published`).
        slot.end.store(claim.last, Ordering::Release);
        // Release: publishes what this thread did before (and `end`) to the reader that reads it.
        slot.start.store(claim.first, Ordering::Release);
    }

    /// Waits until every claim whose first sequence is above `after` and at most `until` has
    /// announced itself. `after` must end a batch: a value `visible` returned, or the last
    /// sequence of a claim.
    pub fn wait_published(&self, after: u64, until: u64) {
        let backoff = Backoff::new();
        let mut next = after.max(self.visible()).saturating_add(1);
        while next <= until {
            let Some(slot) = self.published_slot(next) else {
                return;
            };
            // Acquire: pairs with the Release store in `publish`.
            if slot.start.load(Ordering::Acquire) == next {
                // Acquire: pairs with the Release store of `end`. The value is this claim's end,
                // or the end of a later claim that reused the slot, which happens only once this
                // claim is visible; the load of `visible` after it then says so, and the end is
                // not used.
                let end = slot.end.load(Ordering::Acquire);
                let visible = self.visible();
                next = if visible >= next { visible } else { end }.saturating_add(1);
                continue;
            }
            // The slot holds an older range (the claim has not announced yet) or a newer one
            // (the claim is visible already, so its slot was reused): `visible` tells which.
            let visible = self.visible();
            if visible >= next {
                next = visible.saturating_add(1);
            } else {
                wait(&backoff);
            }
        }
    }

    /// The publication slot for a claim starting at `start`.
    fn published_slot(&self, start: u64) -> Option<&super::Slot> {
        self.published.get(usize::try_from(start & self.mask).ok()?)
    }
}
