//! Counters that many threads add to and that are read rarely; the contract is documented on
//! [`StripedCounters`].

#[cfg(all(test, not(loom)))]
mod tests;

#[cfg(all(test, not(loom)))]
mod stress_tests;

#[cfg(all(test, loom))]
mod loom_tests;

use crate::shim::{AtomicU64, Ordering};

/// How many stripes a set has: threads are spread over them so that adders rarely share a line.
pub(crate) const STRIPES: usize = 16;

/// Counters per line: 16 words fill one 128-byte block.
const PER_LINE: usize = 16;

/// One 128-byte block of counters: the unit two stripes never share (two adjacent 64-byte lines
/// travel together on the targets this engine runs on).
#[repr(align(128))]
pub(crate) struct Line([AtomicU64; PER_LINE]);

impl Line {
    fn zeroed() -> Self {
        Self(std::array::from_fn(|_| AtomicU64::new(0)))
    }
}

/// A fixed set of `u64` counters, each split over 16 stripes so that threads adding to
/// one counter do not contend for one cache line.
///
/// Sequential specification: every counter starts at 0. `add(c, n)` adds `n` to counter `c`
/// (wrapping); `sum(c)` returns it; `take(c)` returns it and sets it to 0. A counter outside the
/// set is ignored: it stays 0.
///
/// | Operation | Linearization point | Progress |
/// |---|---|---|
/// | `add` | the `fetch_add` on the caller's stripe | wait-free |
/// | `sum` | none: not linearizable | wait-free |
/// | `take` | none for the whole; each stripe's `swap` | wait-free |
///
/// Consistency: `sum` loads the stripes one after another, so while adders run it may return a
/// total that no single moment held. It is quiescently consistent: once adds stop, it is exact.
/// `take` swaps each stripe to 0, so every add is counted by exactly one `take` or remains after
/// it: the takes plus what remains always equal everything added. That makes it right for
/// metrics and interval histograms, and wrong for anything that must be unique (an id).
///
/// Ordering: every access is Relaxed. A counter publishes no other memory, and atomicity alone
/// makes each read-modify-write count once; no reader infers anything about other data from a
/// value it reads.
///
/// Reclamation: the lines live as long as the set; nothing is freed or reused.
pub struct StripedCounters {
    lines: Box<[Line]>,
    len: usize,
    /// Lines each stripe owns: `len` divided by [`PER_LINE`], rounded up.
    lines_per_stripe: usize,
}

impl std::fmt::Debug for StripedCounters {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StripedCounters")
            .field("len", &self.len)
            .finish_non_exhaustive()
    }
}

impl StripedCounters {
    /// A set of `len` counters, all 0.
    pub fn new(len: usize) -> Self {
        let lines_per_stripe = len.div_ceil(PER_LINE);
        let lines = lines_per_stripe.saturating_mul(STRIPES);
        Self {
            lines: (0..lines).map(|_| Line::zeroed()).collect(),
            len,
            lines_per_stripe,
        }
    }

    /// How many counters the set holds.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether the set holds no counter.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Adds `amount` to `counter`, on the calling thread's stripe.
    pub fn add(&self, counter: usize, amount: u64) {
        self.add_at(thread_stripe(), counter, amount);
    }

    /// Adds `amount` to `counter` on `stripe`.
    fn add_at(&self, stripe: usize, counter: usize, amount: u64) {
        if let Some(slot) = self.slot(stripe, counter) {
            // Relaxed: a counter publishes nothing; the atomic add alone counts it once.
            slot.fetch_add(amount, Ordering::Relaxed);
        }
    }

    /// The total of `counter` over every stripe; exact once adds have stopped.
    pub fn sum(&self, counter: usize) -> u64 {
        self.stripes(counter).fold(0, |total, slot| {
            // Relaxed: see `add`.
            total.wrapping_add(slot.load(Ordering::Relaxed))
        })
    }

    /// The total of `counter`, set back to 0: each add is counted by exactly one `take`, or
    /// remains for a later one.
    pub fn take(&self, counter: usize) -> u64 {
        self.stripes(counter).fold(0, |total, slot| {
            // Relaxed: see `add`; the swap is atomic, so no add falls between read and reset.
            total.wrapping_add(slot.swap(0, Ordering::Relaxed))
        })
    }

    /// `counter`'s word on every stripe; none for a counter outside the set.
    fn stripes(&self, counter: usize) -> impl Iterator<Item = &AtomicU64> {
        (0..STRIPES).filter_map(move |stripe| self.slot(stripe, counter))
    }

    /// `counter`'s word on `stripe`.
    fn slot(&self, stripe: usize, counter: usize) -> Option<&AtomicU64> {
        if counter >= self.len || stripe >= STRIPES {
            return None;
        }
        let line = stripe
            .checked_mul(self.lines_per_stripe)?
            .checked_add(counter.checked_div(PER_LINE)?)?;
        self.lines.get(line)?.0.get(counter.checked_rem(PER_LINE)?)
    }
}

/// The stripe of the calling thread: threads take stripes in the order they first add, so the
/// first [`STRIPES`] threads of a process each have their own.
fn thread_stripe() -> usize {
    /// The next stripe to hand out. A plain counter, not part of any protocol: a stripe is only
    /// a hint to spread adders, and any stripe is correct.
    static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    std::thread_local! {
        static STRIPE: usize =
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed) & (STRIPES - 1);
    }
    STRIPE.with(|stripe| *stripe)
}
