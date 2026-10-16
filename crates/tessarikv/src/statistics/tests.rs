use super::{Histogram, Statistics, Ticker};

#[test]
fn tickers_count_exactly_and_independently() {
    let statistics = Statistics::new();
    statistics.record(Ticker::GetCalls, 3);
    statistics.record(Ticker::BlockCacheMiss, 2);
    statistics.record(Ticker::GetCalls, 1);
    let snapshot = statistics.snapshot();
    assert_eq!(snapshot.ticker(Ticker::GetCalls), 4);
    assert_eq!(snapshot.ticker(Ticker::BlockCacheMiss), 2);
    for ticker in Ticker::ALL {
        if ticker != Ticker::GetCalls && ticker != Ticker::BlockCacheMiss {
            assert_eq!(snapshot.ticker(ticker), 0, "{ticker:?}");
        }
    }
}

/// Tickers are counters: a second snapshot still holds what the first one read.
#[test]
fn tickers_are_cumulative_across_snapshots() {
    let statistics = Statistics::new();
    statistics.record(Ticker::MemtableHit, 5);
    assert_eq!(statistics.snapshot().ticker(Ticker::MemtableHit), 5);
    statistics.record(Ticker::MemtableHit, 1);
    assert_eq!(statistics.snapshot().ticker(Ticker::MemtableHit), 6);
}

/// Histograms are per interval: a snapshot holds what was observed since the previous one.
#[test]
fn histograms_cover_the_interval_since_the_last_snapshot() {
    let statistics = Statistics::new();
    for micros in [0, 1, 2, 3, 900, 1_000_000] {
        statistics.observe(Histogram::GetMicros, micros);
    }
    let first = statistics.snapshot();
    let get = first.histogram(Histogram::GetMicros);
    assert_eq!(get.count(), 6);
    assert_eq!(get.sum_micros(), 1_000_906);
    assert_eq!(get.bucket_counts()[0], 1, "0 µs");
    assert_eq!(get.bucket_counts()[1], 1, "1 µs");
    assert_eq!(get.bucket_counts()[2], 2, "2..=3 µs");
    assert_eq!(get.bucket_counts()[10], 1, "512..=1023 µs");
    assert_eq!(get.bucket_counts()[20], 1, "2^19..2^20 µs");
    let second = statistics.snapshot();
    assert_eq!(second.histogram(Histogram::GetMicros).count(), 0);
}

#[test]
fn a_quantile_is_the_upper_bound_of_its_bucket() {
    let statistics = Statistics::new();
    for _ in 0..99 {
        statistics.observe(Histogram::GetMicros, 5);
    }
    statistics.observe(Histogram::GetMicros, 3_000);
    let snapshot = statistics.snapshot();
    let get = snapshot.histogram(Histogram::GetMicros);
    assert_eq!(get.quantile(1, 2), 7, "the median lies in 4..=7 µs");
    assert_eq!(get.quantile(99, 100), 7);
    assert_eq!(
        get.quantile(1, 1),
        4_095,
        "the maximum lies in 2048..=4095 µs"
    );
    assert_eq!(super::HistogramSnapshot::default().quantile(1, 2), 0);
}

#[test]
fn every_ticker_has_a_distinct_stable_name() {
    let names: std::collections::HashSet<&str> = Ticker::ALL.iter().map(|t| t.name()).collect();
    assert_eq!(names.len(), Ticker::ALL.len());
    assert_eq!(Ticker::GetCalls.name(), "get.calls");
}
