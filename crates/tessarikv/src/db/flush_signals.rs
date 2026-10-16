//! What writers and the flush installer tell each other.
//!
//! A writer whose batch fills the newest in-memory table asks for a switch; the installer clears
//! the request when it looks, so requests made while it is busy fold into one. A writer is
//! admitted while the newest table has room or another table may still be frozen; otherwise it
//! waits for the next install, or is refused when it asked not to wait. Level 0 holding its
//! slowdown trigger's worth of tables paces writes to the delayed-write rate, each waiting for
//! its own bytes, so compaction catches up; holding its stop trigger's worth stops writes until
//! an install takes it back under. The bytes compaction owes, estimated after every install,
//! slow writes the same way past the soft pending limit and stop them past the hard one. A
//! failed flush or compaction is published once and refuses every later write.

use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use tokio::sync::Notify;
use tokio::sync::futures::Notified;

use super::rate_limiter::RateLimiter;
use super::view::View;
use crate::memtable::Memtable;
use crate::{Error, KeyspaceOptions, Result};

/// Requests, wake-ups and the failure flag shared by writers and the installer.
pub(super) struct FlushSignals {
    /// A switch is asked for; later requests fold into it until the installer looks.
    pending: AtomicBool,
    /// Wakes the installer for a switch.
    wake: Notify,
    /// Wakes a flush when the batches it waits for have completed.
    pub(super) visible: Notify,
    /// Wakes stalled writers after every install, and on failure.
    installed: Notify,
    /// What failed, as one of the `FAILED_` codes: writes are refused until the database is reopened.
    failed: AtomicU8,
    /// Bytes the newest in-memory table holds before it is frozen.
    memtable_bytes: u64,
    /// Frozen tables allowed to wait for flushes.
    max_frozen: usize,
    /// Level-0 tables at which writes slow down, and at which they stop.
    level0_slowdown: usize,
    level0_stop: usize,
    /// Paces writes while level 0 is past its slowdown trigger; none paces nothing.
    delay: Option<RateLimiter>,
    /// The bytes compaction owed at the last install, and the owed bytes at which writes slow
    /// down and stop.
    owed: AtomicU64,
    soft_owed: u64,
    hard_owed: u64,
    /// The most compaction has owed, for tests to read.
    #[cfg(test)]
    peak_owed: AtomicU64,
}

/// How far ahead of the delayed-write rate a slowed write may go: a millisecond's worth.
const SLOWDOWN_BURST: Duration = Duration::from_millis(1);

/// No failure; a flush failed; a compaction failed.
const FAILED_NONE: u8 = 0;
const FAILED_FLUSH: u8 = 1;
const FAILED_COMPACTION: u8 = 2;

impl FlushSignals {
    /// Signals for a keyspace, slowing writes to `delayed_writes` bytes a second.
    pub(super) fn new(keyspace: &KeyspaceOptions, delayed_writes: u64) -> Self {
        Self {
            pending: AtomicBool::new(false),
            wake: Notify::new(),
            visible: Notify::new(),
            installed: Notify::new(),
            failed: AtomicU8::new(FAILED_NONE),
            memtable_bytes: keyspace.memtable_bytes(),
            max_frozen: usize::try_from(keyspace.max_immutable_memtables()).unwrap_or(usize::MAX),
            level0_slowdown: usize::try_from(keyspace.level0_slowdown_trigger())
                .unwrap_or(usize::MAX),
            level0_stop: usize::try_from(keyspace.level0_stop_trigger()).unwrap_or(usize::MAX),
            delay: RateLimiter::new(delayed_writes, SLOWDOWN_BURST),
            owed: AtomicU64::new(0),
            soft_owed: keyspace.soft_pending_compaction_bytes(),
            hard_owed: keyspace.hard_pending_compaction_bytes(),
            #[cfg(test)]
            peak_owed: AtomicU64::new(0),
        }
    }

    /// Asks for a switch when `newest` has reached its size.
    pub(super) fn check(&self, newest: &Memtable) {
        if self.is_full(newest)
            // AcqRel: one requester wins; pairs with the installer clearing the flag.
            && !self.pending.swap(true, Ordering::AcqRel)
        {
            self.wake.notify_one();
        }
    }

    /// The error that stopped the installer, if any: writes must stop.
    pub(super) fn failure(&self) -> Option<Error> {
        // Acquire: pairs with the Release store when the installer gives up.
        match self.failed.load(Ordering::Acquire) {
            FAILED_NONE => None,
            FAILED_COMPACTION => Some(Error::CompactionFailed),
            _ => Some(Error::FlushFailed),
        }
    }

    /// Returns once a write of `bytes` may go ahead: the newest table has room or another table
    /// may be frozen, and level 0 is under its stop trigger; past its slowdown trigger the write
    /// first waits for its bytes at the delayed-write rate.
    ///
    /// # Errors
    ///
    /// [`Error::WriteStalled`] instead of waiting or being delayed when `no_slowdown`, and
    /// [`Error::FlushFailed`] or [`Error::CompactionFailed`] after one failed.
    pub(super) async fn admit(
        &self,
        view: &ArcSwap<View>,
        no_slowdown: bool,
        bytes: u64,
    ) -> Result<()> {
        loop {
            // Created before the check: a later `notify_waiters` reaches it even unpolled.
            let installed = self.installed.notified();
            if let Some(failure) = self.failure() {
                return Err(failure);
            }
            let (stalled, level0) = {
                let current = view.load();
                (self.stalls(&current), current.level0_files(0))
            };
            // Relaxed: a figure read on its own; a held writer reads it again after each install.
            let owed = self.owed.load(Ordering::Relaxed);
            let reason = if stalled {
                "the in-memory table is full and the frozen ones wait for flushes"
            } else if level0 >= self.level0_stop {
                "level 0 holds its stop trigger's worth of tables, waiting for a compaction"
            } else if owed >= self.hard_owed {
                "compaction owes its hard limit's worth of bytes, waiting for a compaction"
            } else if level0 >= self.level0_slowdown || owed >= self.soft_owed {
                if no_slowdown {
                    return Err(Error::WriteStalled {
                        reason: if level0 >= self.level0_slowdown {
                            "level 0 is past its slowdown trigger"
                        } else {
                            "compaction owes its soft limit's worth of bytes"
                        },
                    });
                }
                let wait = self
                    .delay
                    .as_ref()
                    .map_or(Duration::ZERO, |delay| delay.reserve(bytes, Instant::now()));
                if !wait.is_zero() {
                    tokio::time::sleep(wait).await;
                }
                return Ok(());
            } else {
                return Ok(());
            };
            if no_slowdown {
                return Err(Error::WriteStalled { reason });
            }
            installed.await;
        }
    }

    /// Records the bytes compaction owes after an install; writers held by it look again at the
    /// next [`FlushSignals::installed`].
    pub(super) fn set_owed(&self, owed: u64) {
        // Relaxed: see `admit`.
        self.owed.store(owed, Ordering::Relaxed);
        #[cfg(test)]
        self.peak_owed.fetch_max(owed, Ordering::Relaxed);
    }

    /// The most compaction has owed, for tests to read.
    #[cfg(test)]
    pub(super) fn peak_owed(&self) -> u64 {
        self.peak_owed.load(Ordering::Relaxed)
    }

    /// The delayed-write rate, for tests to read.
    #[cfg(test)]
    pub(super) fn delayed_rate(&self) -> Option<u64> {
        self.delay.as_ref().map(RateLimiter::rate)
    }

    /// Resolves when a writer asks for a switch.
    pub(super) fn requested(&self) -> Notified<'_> {
        self.wake.notified()
    }

    /// Marks the request seen; a writer that found it set before this asked for nothing, so the
    /// installer checks the size itself afterwards.
    pub(super) fn take_request(&self) {
        // AcqRel RMW, not a store: it reads the last requester's swap, so the size that writer
        // saw is visible to the check that follows; a plain store leaves that store→load pair
        // unordered and the request lost until the next write.
        self.pending.swap(false, Ordering::AcqRel);
    }

    /// Wakes stalled writers after an install.
    pub(super) fn installed(&self) {
        self.installed.notify_waiters();
    }

    /// Publishes the installer's failure, `error`, and wakes stalled writers to see it.
    pub(super) fn fail(&self, error: &Error) {
        let failure = match error {
            Error::CompactionFailed => FAILED_COMPACTION,
            _ => FAILED_FLUSH,
        };
        // Release: writers that see the flag see the failure's effects.
        self.failed.store(failure, Ordering::Release);
        self.installed.notify_waiters();
    }

    /// Whether the newest table of `view` should be frozen, with `frozen` tables already waiting.
    pub(super) fn should_switch(&self, view: &View, frozen: usize) -> bool {
        frozen < self.max_frozen && self.newest_is_full(view)
    }

    /// Whether `view` has no room for another write: its newest table is full and no more
    /// tables may be frozen.
    fn stalls(&self, view: &View) -> bool {
        let frozen = view.memtables().count().saturating_sub(1);
        frozen >= self.max_frozen && self.newest_is_full(view)
    }

    fn newest_is_full(&self, view: &View) -> bool {
        view.memtables()
            .next()
            .is_some_and(|newest| self.is_full(newest))
    }

    fn is_full(&self, table: &Memtable) -> bool {
        table.approximate_bytes() >= self.memtable_bytes
    }
}
