//! What writers and the flush installer tell each other.
//!
//! A writer whose batch fills a newest in-memory table (each holds its keyspace's size) asks for a
//! switch; the installer clears the request when it looks, so requests made while it is busy fold
//! into one. A writer is admitted while the newest tables have room or another epoch may still be
//! frozen; otherwise it waits for the next install, or is refused when it asked not to wait. The
//! installer publishes what the levels ask of writers after every change to them (see
//! [`super::pressure`]): pacing writes to the delayed-write rate, each waiting for its own bytes,
//! so compaction catches up, or holding them until an install relieves the keyspace concerned. A
//! failed flush or compaction is published once and refuses every later write.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use tokio::sync::Notify;
use tokio::sync::futures::Notified;

use super::pressure::{Load, Pressure};
use super::rate_limiter::RateLimiter;
use super::view::View;
use crate::memtable::Memtable;
use crate::options::Reporter;
use crate::{Error, Result};

mod reports;

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
    /// The highest sequence a caller of `Db::flush` wants on table files.
    forced: AtomicU64,
    /// The highest sequence on table files, raised after each install.
    flushed: AtomicU64,
    /// What failed, as one of the `FAILED_` codes: writes are refused until the database is reopened.
    failed: AtomicU8,
    /// Frozen epochs allowed to wait for flushes.
    max_frozen: usize,
    /// The most bytes the newest tables may hold together; zero sets no ceiling.
    total_bytes: u64,
    /// Paces writes while the levels ask for it; none paces nothing.
    delay: Option<RateLimiter>,
    /// What the levels ask of writers, as a [`Pressure`] code, published by the installer.
    pressure: AtomicU8,
    /// What compaction owes each keyspace, by keyspace id, as of the installer's last step.
    owed: ArcSwap<Vec<(u32, u64)>>,
    /// Told when the pressure changes and when the installer fails.
    events: Option<Arc<Reporter>>,
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
    /// Signals letting `max_frozen` epochs wait for flushes, pacing writes to `delayed_writes`
    /// bytes a second, and treating the newest tables as full once they hold `total_bytes`
    /// together (zero: never).
    pub(super) fn new(max_frozen: u32, delayed_writes: u64, total_bytes: u64) -> Self {
        Self {
            pending: AtomicBool::new(false),
            wake: Notify::new(),
            visible: Notify::new(),
            installed: Notify::new(),
            forced: AtomicU64::new(0),
            flushed: AtomicU64::new(0),
            failed: AtomicU8::new(FAILED_NONE),
            max_frozen: usize::try_from(max_frozen).unwrap_or(usize::MAX),
            total_bytes,
            delay: RateLimiter::new(delayed_writes, SLOWDOWN_BURST),
            pressure: AtomicU8::new(Pressure::None.code()),
            owed: ArcSwap::from_pointee(Vec::new()),
            events: None,
            #[cfg(test)]
            peak_owed: AtomicU64::new(0),
        }
    }

    /// Asks for a switch when a newest table of `view` has reached its size.
    pub(super) fn check(&self, view: &View) {
        if self.newest_is_full(view) {
            self.request();
        }
    }

    /// Asks the installer to look for a switch.
    fn request(&self) {
        // AcqRel: one requester wins; pairs with the installer clearing the flag, which then
        // sees what the requester did before (a forced target included).
        if !self.pending.swap(true, Ordering::AcqRel) {
            self.wake.notify_one();
        }
    }

    /// Returns once every sequence up to `target` is on table files, asking for the in-memory
    /// table holding it to be frozen and flushed.
    ///
    /// # Errors
    ///
    /// [`Error::FlushFailed`] or [`Error::CompactionFailed`] after one failed.
    pub(super) async fn flush_through(&self, target: u64) -> Result<()> {
        loop {
            let installed = self.installed.notified();
            tokio::pin!(installed);
            // Registered before the checks, so an install between them still wakes this wait.
            installed.as_mut().enable();
            if let Some(error) = self.failure() {
                return Err(error);
            }
            // Acquire: pairs with the Release in `set_flushed`.
            if self.flushed.load(Ordering::Acquire) >= target {
                return Ok(());
            }
            // Relaxed: published to the installer by the request's read-modify-write.
            self.forced.fetch_max(target, Ordering::Relaxed);
            self.request();
            installed.await;
        }
    }

    /// Records that every sequence up to `flushed` is on table files and wakes waiters.
    pub(super) fn set_flushed(&self, flushed: u64) {
        // Release: a waiter that reads it sees the install that wrote the file.
        self.flushed.fetch_max(flushed, Ordering::Release);
        self.installed.notify_waiters();
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

    /// Returns once a write of `bytes` may go ahead: the newest tables have room or another epoch
    /// may be frozen, and the levels do not hold writes; when they ask for pacing the write first
    /// waits for its bytes at the delayed-write rate.
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
            let stalled = self.stalls(&view.load());
            // Relaxed: read after `installed` is created. A store the installer made before
            // waking waiters is ordered before this read by the wake-up's read-modify-write
            // whenever this waiter missed that wake-up; otherwise the wake-up reaches it.
            let pressure = Pressure::from_code(self.pressure.load(Ordering::Relaxed));
            let reason = if stalled {
                "the in-memory table is full and the frozen ones wait for flushes"
            } else if pressure >= Pressure::StopOwed {
                pressure.reason()
            } else if pressure > Pressure::None {
                if no_slowdown {
                    return Err(Error::WriteStalled {
                        reason: pressure.reason(),
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

    /// Publishes what the levels ask of writers; when that changed, held writers look again.
    pub(super) fn set_load(&self, load: Load) {
        // Relaxed: see `admit`.
        let code = load.pressure.code();
        let before = self.pressure.swap(code, Ordering::Relaxed);
        if before != code {
            if let Some(events) = &self.events {
                events.listener().stall_changed(load.pressure.condition());
            }
            self.installed.notify_waiters();
        }
        #[cfg(test)]
        self.peak_owed.fetch_max(load.owed, Ordering::Relaxed);
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
        if let Some(events) = &self.events {
            events.background_error(error);
        }
        self.installed.notify_waiters();
    }

    /// Whether the newest table of `view` should be frozen, with `frozen` tables already waiting
    /// and every sequence up to `switched` in older tables: it is full, or a flush wants a
    /// sequence it holds.
    pub(super) fn should_switch(&self, view: &View, frozen: usize, switched: u64) -> bool {
        // Relaxed: read after the installer's read-modify-write of the request flag.
        let forced = self.forced.load(Ordering::Relaxed) > switched;
        frozen < self.max_frozen && (forced || self.newest_is_full(view))
    }

    /// Whether `view` has no room for another write: its newest table is full and no more
    /// tables may be frozen.
    fn stalls(&self, view: &View) -> bool {
        let frozen = view.epochs().saturating_sub(1);
        frozen >= self.max_frozen && self.newest_is_full(view)
    }

    /// Whether a newest table of `view` is full, or the newest tables together have reached
    /// the database's ceiling.
    fn newest_is_full(&self, view: &View) -> bool {
        view.newest_tables().any(Memtable::is_full)
            || (self.total_bytes != 0
                && view
                    .newest_tables()
                    .map(Memtable::approximate_bytes)
                    .fold(0_u64, u64::saturating_add)
                    >= self.total_bytes)
    }
}
