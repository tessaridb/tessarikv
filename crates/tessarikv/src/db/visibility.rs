//! Completing a write's claim, and returning only once the write is readable.
//!
//! Visibility is a prefix: a batch becomes visible once every batch claimed before it has
//! completed. A writer whose own batch completed while an earlier one is still being applied
//! therefore waits for that earlier batch before returning, so a read issued after the write
//! returns always sees it. When nothing earlier is pending (the common case) the check is one
//! load. Otherwise the writer spins and yields in short rounds, since the earlier batch is
//! usually about to complete, and only then counts itself as waiting and parks; every completion
//! wakes the parked writers, which recheck. Only earlier batches are waited for, and an earlier
//! batch never waits for a later one, so the wait cannot form a cycle.

use tessarikv_constants::{VISIBILITY_SPINS, VISIBILITY_YIELDS};
use tessarikv_sync::{Claim, Sequencer};
use tessarikv_types::SequenceNumber;
use tokio::sync::Notify;

use super::Db;
use crate::{Error, Result};

impl Db {
    /// Passes `committed` through once its last sequence is visible to reads.
    pub(super) async fn readable(
        &self,
        committed: Result<SequenceNumber>,
    ) -> Result<SequenceNumber> {
        let last = committed?;
        // Spin, then yield so an earlier writer queued on this worker can run, and repeat: the
        // earlier batch is usually a few entries from completing.
        for round in 0..=VISIBILITY_YIELDS {
            if round > 0 {
                tokio::task::yield_now().await;
            }
            for _ in 0..VISIBILITY_SPINS {
                // Acquire: pairs with the advancing compare-and-swap.
                if self.sequencer.visible() >= last.get() {
                    return Ok(last);
                }
                std::hint::spin_loop();
            }
        }
        let _waiting = Waiting::new(&self.sequencer);
        loop {
            // Created before the check, so a completion after the check wakes it.
            let woken = self.visibility.notified();
            if self.sequencer.covers(last.get()) {
                return Ok(last);
            }
            woken.await;
        }
    }
}

/// A writer counted as waiting for visibility until dropped, also when its write is dropped
/// while it waits.
struct Waiting<'a>(&'a Sequencer);

impl<'a> Waiting<'a> {
    fn new(sequencer: &'a Sequencer) -> Self {
        sequencer.add_waiter();
        Self(sequencer)
    }
}

impl Drop for Waiting<'_> {
    fn drop(&mut self) {
        self.0.remove_waiter();
    }
}

/// A claim that is completed when dropped, so a write abandoned while it waits on the log does
/// not hold back every later batch.
pub(super) struct Completion<'a> {
    sequencer: &'a Sequencer,
    /// Woken when completing reaches the sequence a flush waits for.
    completed: Option<&'a Notify>,
    /// Woken after every completion while writers wait for visibility.
    visibility: &'a Notify,
    claim: Option<Claim>,
}

impl<'a> Completion<'a> {
    pub(super) fn new(
        sequencer: &'a Sequencer,
        completed: Option<&'a Notify>,
        visibility: &'a Notify,
        claim: Claim,
    ) -> Self {
        Self {
            sequencer,
            completed,
            visibility,
            claim: Some(claim),
        }
    }

    /// Completes the claim, once, waking a waiting flush when it reached its sequence and the
    /// writers waiting for visibility, which recheck.
    fn finish(&mut self) {
        let Some(claim) = self.claim.take() else {
            return;
        };
        if self.sequencer.complete(claim)
            && let Some(completed) = self.completed
        {
            completed.notify_one();
        }
        if self.sequencer.has_waiters() {
            self.visibility.notify_waiters();
        }
    }

    pub(super) fn epoch(&self) -> u8 {
        self.claim.as_ref().map_or(0, Claim::epoch)
    }

    pub(super) fn first(&self) -> u64 {
        self.claim.as_ref().map_or(0, Claim::first)
    }

    pub(super) fn last(&self) -> u64 {
        self.claim.as_ref().map_or(0, Claim::last)
    }

    /// Completes the claim whatever `applied` says, so later batches are not held back, and
    /// returns the batch's last sequence number.
    pub(super) fn complete(mut self, applied: Result<()>) -> Result<SequenceNumber> {
        let last = self.last();
        self.finish();
        applied?;
        SequenceNumber::new(last).map_err(|_| Error::SequenceExhausted)
    }
}

impl Drop for Completion<'_> {
    fn drop(&mut self) {
        self.finish();
    }
}
