//! Pacing of writes to a rate: compaction output, so compaction leaves the device bandwidth that
//! reads and writes need, and writes while level 0 is past its slowdown trigger.
//!
//! A limiter holds one number: the moment, in nanoseconds after its origin, from which the next
//! byte is owed. Reserving `bytes` moves that moment forward by the time the rate needs for them,
//! starting no earlier than now. A writer may run ahead of the rate by the limiter's burst, so
//! an idle limiter lets that much through at once; past it a writer waits until the debt falls
//! back within the burst.
//!
//! Ordering: the moment is moved by a compare-and-swap retry loop with relaxed ordering on both
//! sides. Nothing else is published through it; each reservation only computes its own wait, and
//! reservations are serialised by the swap itself. A failed swap means another reservation took
//! effect, so the loop is lock-free. Its callers are the few threads writing compaction output,
//! or writers already being slowed down, so the loop is not a contended hot spot.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Nanoseconds in a second.
const SECOND: u64 = 1_000_000_000;

/// Paces writes to a rate.
#[derive(Debug)]
pub(crate) struct RateLimiter {
    origin: Instant,
    bytes_per_second: u64,
    /// Nanoseconds a writer may run ahead of the rate.
    burst: u64,
    /// Nanoseconds after `origin` from which the next reserved byte is owed.
    next: AtomicU64,
    /// Bytes reserved so far, for tests to read.
    #[cfg(test)]
    charged: AtomicU64,
}

impl RateLimiter {
    /// A limiter of `bytes_per_second` letting `burst`'s worth through at once, starting now;
    /// none for zero.
    pub(crate) fn new(bytes_per_second: u64, burst: Duration) -> Option<Self> {
        Self::starting(bytes_per_second, burst, Instant::now())
    }

    /// A limiter of `bytes_per_second` letting `burst`'s worth through at once, whose clock
    /// starts at `origin`; none for zero.
    pub(crate) fn starting(
        bytes_per_second: u64,
        burst: Duration,
        origin: Instant,
    ) -> Option<Self> {
        (bytes_per_second > 0).then_some(Self {
            origin,
            bytes_per_second,
            burst: u64::try_from(burst.as_nanos()).unwrap_or(u64::MAX),
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
                    return Duration::from_nanos(
                        paid.saturating_sub(self.burst).saturating_sub(now),
                    );
                }
                Err(current) => owed = current,
            }
        }
    }

    /// Reserves `bytes` and sleeps until they may be written. Only for threads that may block;
    /// async code awaits the wait [`RateLimiter::reserve`] returns instead.
    pub(crate) fn pace(&self, bytes: u64) {
        let wait = self.reserve(bytes, Instant::now());
        if !wait.is_zero() {
            std::thread::sleep(wait);
        }
    }

    /// The rate, in bytes a second.
    #[cfg(test)]
    pub(crate) const fn rate(&self) -> u64 {
        self.bytes_per_second
    }

    /// The bytes reserved so far.
    #[cfg(test)]
    pub(crate) fn charged(&self) -> u64 {
        // Relaxed: see `reserve`.
        self.charged.load(Ordering::Relaxed)
    }
}
