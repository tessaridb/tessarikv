//! Engine statistics: counters of what the engine did and latency distributions, kept when
//! [`crate::DbOptions::statistics`] is set and read with [`crate::Db::statistics`].
//!
//! Every increment is one wait-free add on the calling thread's stripe
//! ([`tessarikv_sync::StripedCounters`]); nothing on a read or write path walks engine state to
//! report a statistic. Tickers are counters since the database opened; histograms cover the
//! interval since the previous snapshot, so a percentile describes now and not the whole run.

mod histogram;

#[cfg(test)]
mod tests;

pub use histogram::{Histogram, HistogramSnapshot};
use tessarikv_sync::StripedCounters;

use histogram::{BUCKETS, SLOTS};

/// A counter the engine keeps, each answering one question about the engine's work.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Ticker {
    /// Point reads asked for: the denominator of every per-read ratio.
    GetCalls,
    /// Point reads an in-memory table answered (a value or a deletion).
    MemtableHit,
    /// Point reads no in-memory table answered, which went on to the table files.
    MemtableMiss,
    /// Data blocks found in the block cache.
    BlockCacheHit,
    /// Data blocks not in the block cache, read from storage.
    BlockCacheMiss,
    /// Bytes of data blocks read from storage.
    BlockBytesRead,
    /// Table files a point read skipped because their filter excluded the key.
    BloomUseful,
    /// Table files whose filter said the key may be present, so their blocks were read.
    BloomMaybe,
}

impl Ticker {
    /// Every ticker, in a stable order.
    pub const ALL: [Self; 8] = [
        Self::GetCalls,
        Self::MemtableHit,
        Self::MemtableMiss,
        Self::BlockCacheHit,
        Self::BlockCacheMiss,
        Self::BlockBytesRead,
        Self::BloomUseful,
        Self::BloomMaybe,
    ];

    /// The ticker's stable name, for exporting.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::GetCalls => "get.calls",
            Self::MemtableHit => "memtable.hit",
            Self::MemtableMiss => "memtable.miss",
            Self::BlockCacheHit => "block.cache.hit",
            Self::BlockCacheMiss => "block.cache.miss",
            Self::BlockBytesRead => "block.bytes.read",
            Self::BloomUseful => "bloom.useful",
            Self::BloomMaybe => "bloom.maybe",
        }
    }

    /// The ticker's counter.
    const fn index(self) -> usize {
        match self {
            Self::GetCalls => 0,
            Self::MemtableHit => 1,
            Self::MemtableMiss => 2,
            Self::BlockCacheHit => 3,
            Self::BlockCacheMiss => 4,
            Self::BlockBytesRead => 5,
            Self::BloomUseful => 6,
            Self::BloomMaybe => 7,
        }
    }
}

/// The counters of one database.
#[derive(Debug)]
pub(crate) struct Statistics {
    tickers: StripedCounters,
    /// [`SLOTS`] counters per histogram: its buckets, then the sum of what it observed.
    histograms: StripedCounters,
}

impl Statistics {
    pub(crate) fn new() -> Self {
        Self {
            tickers: StripedCounters::new(Ticker::ALL.len()),
            histograms: StripedCounters::new(Histogram::ALL.len().saturating_mul(SLOTS)),
        }
    }

    /// Adds `amount` to `ticker`.
    pub(crate) fn record(&self, ticker: Ticker, amount: u64) {
        self.tickers.add(ticker.index(), amount);
    }

    /// Counts one observation of `micros` microseconds in `histogram`.
    pub(crate) fn observe(&self, histogram: Histogram, micros: u64) {
        let base = histogram.base();
        self.histograms
            .add(base.saturating_add(histogram::bucket(micros)), 1);
        self.histograms.add(base.saturating_add(BUCKETS), micros);
    }

    /// Every ticker since open, and every histogram since the previous snapshot.
    pub(crate) fn snapshot(&self) -> StatisticsSnapshot {
        StatisticsSnapshot {
            tickers: Ticker::ALL.map(|ticker| self.tickers.sum(ticker.index())),
            histograms: Histogram::ALL.map(|histogram| {
                let base = histogram.base();
                let mut snapshot = HistogramSnapshot::default();
                for (bucket, count) in snapshot.buckets.iter_mut().enumerate() {
                    *count = self.histograms.take(base.saturating_add(bucket));
                }
                snapshot.sum = self.histograms.take(base.saturating_add(BUCKETS));
                snapshot
            }),
        }
    }
}

/// What [`crate::Db::statistics`] read: tickers since the database opened, histograms since the
/// previous read.
///
/// Read while the engine works, so its values are not one instant's: each is exact for its
/// counter, and counters read together may be a few operations apart.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatisticsSnapshot {
    tickers: [u64; Ticker::ALL.len()],
    histograms: [HistogramSnapshot; Histogram::ALL.len()],
}

impl StatisticsSnapshot {
    /// The value of `ticker` since the database opened.
    #[must_use]
    pub fn ticker(&self, ticker: Ticker) -> u64 {
        self.tickers.get(ticker.index()).copied().unwrap_or(0)
    }

    /// The distribution `histogram` observed since the previous snapshot.
    #[must_use]
    pub fn histogram(&self, histogram: Histogram) -> &HistogramSnapshot {
        &self.histograms[histogram.index()]
    }
}
