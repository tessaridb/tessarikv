//! What writers and the flush installer tell each other.
//!
//! A writer whose batch fills the newest in-memory table asks for a switch; the installer clears
//! the request when it looks, so requests made while it is busy fold into one. A writer is
//! admitted while the newest table has room or another table may still be frozen; otherwise it
//! waits for the next install, or is refused when it asked not to wait. A failed flush is
//! published once and refuses every later write.

use std::sync::atomic::{AtomicBool, Ordering};

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
    /// A flush failed: writes are refused until the database is reopened.
    failed: AtomicBool,
    /// Bytes the newest in-memory table holds before it is frozen.
    memtable_bytes: u64,
    /// Frozen tables allowed to wait for flushes.
    max_frozen: usize,
}

impl FlushSignals {
    pub(super) fn new(keyspace: &KeyspaceOptions) -> Self {
        Self {
            pending: AtomicBool::new(false),
            wake: Notify::new(),
            visible: Notify::new(),
            installed: Notify::new(),
            failed: AtomicBool::new(false),
            memtable_bytes: keyspace.memtable_bytes(),
            max_frozen: usize::try_from(keyspace.max_immutable_memtables()).unwrap_or(usize::MAX),
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

    /// Whether a flush failed, so writes must stop.
    pub(super) fn failed(&self) -> bool {
        // Acquire: pairs with the Release store when the installer gives up.
        self.failed.load(Ordering::Acquire)
    }

    /// Returns once a write may go ahead: the newest table has room, or another table may be
    /// frozen.
    ///
    /// # Errors
    ///
    /// [`Error::WriteStalled`] instead of waiting when `no_slowdown`, and
    /// [`Error::FlushFailed`] after a flush failed.
    pub(super) async fn admit(&self, view: &ArcSwap<View>, no_slowdown: bool) -> Result<()> {
        loop {
            // Created before the check: a later `notify_waiters` reaches it even unpolled.
            let installed = self.installed.notified();
            if self.failed() {
                return Err(Error::FlushFailed);
            }
            if !self.stalls(&view.load()) {
                return Ok(());
            }
            if no_slowdown {
                return Err(Error::WriteStalled {
                    reason: "the in-memory table is full and the frozen ones wait for flushes",
                });
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

    /// Publishes a failed flush and wakes stalled writers to see it.
    pub(super) fn fail(&self) {
        // Release: writers that see the flag see the failure's effects.
        self.failed.store(true, Ordering::Release);
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
