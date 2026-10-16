//! What writers and the flush installer tell each other.
//!
//! A writer whose batch fills the newest in-memory table asks for a switch; the installer clears
//! the request when it looks, so requests made while it is busy fold into one. A writer is
//! admitted while the newest table has room or another table may still be frozen; otherwise it
//! waits for the next install, or is refused when it asked not to wait. Level 0 holding its
//! slowdown trigger's worth of tables delays each write by [`SLOWDOWN`] so compaction catches
//! up; holding its stop trigger's worth stops writes until an install takes it back under. A
//! failed flush or compaction is published once and refuses every later write.

use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};

use arc_swap::ArcSwap;
use tokio::sync::Notify;
use tokio::sync::futures::Notified;

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
}

/// How long a write waits while level 0 is past its slowdown trigger.
const SLOWDOWN: std::time::Duration = std::time::Duration::from_millis(1);

/// No failure; a flush failed; a compaction failed.
const FAILED_NONE: u8 = 0;
const FAILED_FLUSH: u8 = 1;
const FAILED_COMPACTION: u8 = 2;

impl FlushSignals {
    pub(super) fn new(keyspace: &KeyspaceOptions) -> Self {
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

    /// Returns once a write may go ahead: the newest table has room or another table may be
    /// frozen, and level 0 is under its stop trigger; past its slowdown trigger the write is
    /// delayed first.
    ///
    /// # Errors
    ///
    /// [`Error::WriteStalled`] instead of waiting or being delayed when `no_slowdown`, and
    /// [`Error::FlushFailed`] or [`Error::CompactionFailed`] after one failed.
    pub(super) async fn admit(&self, view: &ArcSwap<View>, no_slowdown: bool) -> Result<()> {
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
            let reason = if stalled {
                "the in-memory table is full and the frozen ones wait for flushes"
            } else if level0 >= self.level0_stop {
                "level 0 holds its stop trigger's worth of tables, waiting for a compaction"
            } else if level0 >= self.level0_slowdown {
                if no_slowdown {
                    return Err(Error::WriteStalled {
                        reason: "level 0 is past its slowdown trigger",
                    });
                }
                tokio::time::sleep(SLOWDOWN).await;
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
