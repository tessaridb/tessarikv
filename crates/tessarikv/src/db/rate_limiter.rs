//! Pacing of compaction writes, so compaction leaves the device bandwidth that reads and
//! writes need.
//!
//! A limiter holds one number: the moment, in nanoseconds after its origin, from which the next
//! byte is owed. Reserving `bytes` moves that moment forward by the time the rate needs for them,
//! starting no earlier than now. A writer may run ahead of the rate by one second's worth of
//! bytes, so an idle limiter lets a burst through at once; past that it waits until the debt
//! falls back within the second.
//!
//! Ordering: the moment is moved by a compare-and-swap retry loop with relaxed ordering on both
//! sides. Nothing else is published through it; each reservation only computes its own wait, and
//! reservations are serialised by the swap itself. A failed swap means another reservation took
//! effect, so the loop is lock-free. Its callers are the few threads writing compaction output,
//! so the loop is not a contended hot spot.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Nanoseconds in a second, the burst a limiter allows.
const SECOND: u64 = 1_000_000_000;

/// Paces writes to a rate.
#[derive(Debug)]
pub(crate) struct RateLimiter {
    origin: Instant,
    bytes_per_second: u64,
    /// Nanoseconds after `origin` from which the next reserved byte is owed.
    next: AtomicU64,
    /// Bytes reserved so far, for tests to read.
    #[cfg(test)]
    charged: AtomicU64,
}

impl RateLimiter {
    /// A limiter of `bytes_per_second` starting now; none for zero.
    pub(crate) fn new(bytes_per_second: u64) -> Option<Self> {
        Self::starting(bytes_per_second, Instant::now())
    }

    /// A limiter of `bytes_per_second` whose clock starts at `origin`; none for zero.
    pub(crate) fn starting(bytes_per_second: u64, origin: Instant) -> Option<Self> {
        (bytes_per_second > 0).then_some(Self {
            origin,
            bytes_per_second,
            next: AtomicU64::new(0),
            #[cfg(test)]
            charged: AtomicU64::new(0),
        })
    }

    /// Reserves `bytes` at `now` and returns how long the caller waits before writing them.
    pub(crate) fn reserve(&self, bytes: u64, now: Instant) -> Duration {
        // Relaxed: a count read on its own.
        #[cfg(test)]
        self.charged.fetch_add(bytes, Ordering::Relaxed);
        let cost = u64::try_from(
            u128::from(bytes)
                .saturating_mul(u128::from(SECOND))
                .checked_div(u128::from(self.bytes_per_second))
                .unwrap_or(u128::MAX),
        )
        .unwrap_or(u64::MAX);
        let now = u64::try_from(now.saturating_duration_since(self.origin).as_nanos())
            .unwrap_or(u64::MAX);
        // Relaxed: see the module documentation.
        let mut owed = self.next.load(Ordering::Relaxed);
        loop {
            let paid = owed.max(now).saturating_add(cost);
            match self
                .next
                .compare_exchange_weak(owed, paid, Ordering::Relaxed, Ordering::Relaxed)
            {
                Ok(_) => {
                    return Duration::from_nanos(paid.saturating_sub(SECOND).saturating_sub(now));
                }
                Err(current) => owed = current,
            }
        }
    }

    /// Reserves `bytes` and sleeps until they may be written. Only for threads that may block.
    pub(crate) fn pace(&self, bytes: u64) {
        let wait = self.reserve(bytes, Instant::now());
        if !wait.is_zero() {
            std::thread::sleep(wait);
        }
    }

    /// The bytes reserved so far.
    #[cfg(test)]
    pub(crate) fn charged(&self) -> u64 {
        // Relaxed: see `reserve`.
        self.charged.load(Ordering::Relaxed)
    }
}
