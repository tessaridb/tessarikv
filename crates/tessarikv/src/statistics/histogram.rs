//! Latency distributions in power-of-two buckets of microseconds.

/// Buckets per histogram: bucket `b` holds values whose bit length is `b` (0 holds 0, 1 holds 1,
/// 2 holds 2..=3, and so on up to 2^63 and above).
pub(super) const BUCKETS: usize = 65;

/// Counters per histogram: its buckets and the sum of its values.
pub(super) const SLOTS: usize = BUCKETS + 1;

/// A latency distribution the engine keeps.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Histogram {
    /// Microseconds a point read took, from the call to its answer.
    GetMicros,
    /// Microseconds a write took, from the call until it was applied as durably as it asked.
    WriteMicros,
}

impl Histogram {
    /// Every histogram, in a stable order.
    pub const ALL: [Self; 2] = [Self::GetMicros, Self::WriteMicros];

    /// The histogram's stable name, for exporting.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::GetMicros => "get.micros",
            Self::WriteMicros => "write.micros",
        }
    }

    pub(super) const fn index(self) -> usize {
        match self {
            Self::GetMicros => 0,
            Self::WriteMicros => 1,
        }
    }

    /// The histogram's first counter.
    pub(super) const fn base(self) -> usize {
        match self {
            Self::GetMicros => 0,
            Self::WriteMicros => SLOTS,
        }
    }
}

/// The bucket of `micros`: its bit length.
pub(super) fn bucket(micros: u64) -> usize {
    // At most 64, so the conversion never fails.
    usize::try_from(u64::BITS.saturating_sub(micros.leading_zeros())).unwrap_or(BUCKETS)
}

/// The largest value bucket `bucket` holds.
fn upper(bucket: usize) -> u64 {
    match u32::try_from(bucket) {
        Ok(0) => 0,
        Ok(bits) if bits < u64::BITS => (1_u64 << bits).saturating_sub(1),
        _ => u64::MAX,
    }
}

/// A distribution observed over one interval.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistogramSnapshot {
    pub(super) buckets: [u64; BUCKETS],
    pub(super) sum: u64,
}

impl Default for HistogramSnapshot {
    fn default() -> Self {
        Self {
            buckets: [0; BUCKETS],
            sum: 0,
        }
    }
}

impl HistogramSnapshot {
    /// How many values were observed.
    #[must_use]
    pub fn count(&self) -> u64 {
        self.buckets
            .iter()
            .fold(0, |total, count| total.wrapping_add(*count))
    }

    /// The sum of the values observed, in microseconds.
    #[must_use]
    pub const fn sum_micros(&self) -> u64 {
        self.sum
    }

    /// Values per bucket: bucket `b` holds values of bit length `b`, so its largest value is
    /// `2^b - 1` microseconds.
    #[must_use]
    pub const fn bucket_counts(&self) -> &[u64] {
        &self.buckets
    }

    /// An upper bound on the `numerator / denominator` quantile, in microseconds: the largest
    /// value of the bucket the quantile falls in (`quantile(99, 100)` for p99). Zero when
    /// nothing was observed or `denominator` is zero.
    #[must_use]
    pub fn quantile(&self, numerator: u64, denominator: u64) -> u64 {
        let count = u128::from(self.count());
        let Some(rank) = (count.saturating_mul(u128::from(numerator.min(denominator))))
            .checked_div(u128::from(denominator))
        else {
            return 0;
        };
        // The first bucket whose running count reaches the rank (at least one value).
        let rank = rank.max(1);
        let mut seen = 0_u128;
        for (bucket, values) in self.buckets.iter().enumerate() {
            seen = seen.saturating_add(u128::from(*values));
            if seen >= rank {
                return upper(bucket);
            }
        }
        0
    }
}
