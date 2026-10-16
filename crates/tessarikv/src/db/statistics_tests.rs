//! The read-path statistics a database keeps when asked to: each ticker moves by exactly what
//! the reads did.

use std::sync::Arc;

use super::Db;
use super::flush_tests::PATH;
use crate::log::fault::FaultStorage;
use crate::{DbOptions, Histogram, KeyspaceId, ReadOptions, Ticker, WriteBatch, WriteOptions};

fn options(statistics: bool) -> DbOptions {
    DbOptions::builder()
        .statistics(statistics)
        .build()
        .expect("valid")
}

async fn open(storage: &FaultStorage) -> Db {
    Db::open_with_storage(PATH, options(true), Arc::new(storage.clone()))
        .await
        .expect("opened")
}

async fn put(db: &Db, keys: &[&[u8]]) {
    let mut batch = WriteBatch::new();
    for key in keys {
        batch
            .put(KeyspaceId::DEFAULT, key, b"value")
            .expect("entry");
    }
    db.write(&batch, &WriteOptions::default())
        .await
        .expect("written");
}

async fn get(db: &Db, key: &[u8]) -> Option<Vec<u8>> {
    db.get(KeyspaceId::DEFAULT, key, &ReadOptions::default())
        .await
        .expect("read")
}

/// The tickers that moved since `before`, as (ticker, delta), for exact assertions.
fn moved(db: &Db, before: &crate::StatisticsSnapshot) -> Vec<(Ticker, u64)> {
    let after = db.statistics().expect("statistics on");
    Ticker::ALL
        .into_iter()
        .map(|ticker| (ticker, after.ticker(ticker) - before.ticker(ticker)))
        .filter(|(_, delta)| *delta > 0)
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_database_without_statistics_has_none() {
    let db = Db::open("plain", DbOptions::in_memory())
        .await
        .expect("opened");
    assert!(db.statistics().is_none());
    let storage = FaultStorage::default();
    let db = Db::open_with_storage(PATH, options(false), Arc::new(storage))
        .await
        .expect("opened");
    assert!(db.statistics().is_none());
    db.close().await.expect("closed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reads_answered_by_the_memtable_count_hits_and_misses() {
    let storage = FaultStorage::default();
    let db = open(&storage).await;
    put(&db, &[b"apple"]).await;
    let before = db.statistics().expect("statistics on");
    assert!(get(&db, b"apple").await.is_some());
    assert_eq!(
        moved(&db, &before),
        [(Ticker::GetCalls, 1), (Ticker::MemtableHit, 1)]
    );
    let before = db.statistics().expect("statistics on");
    assert!(get(&db, b"zebra").await.is_none());
    assert_eq!(
        moved(&db, &before),
        [(Ticker::GetCalls, 1), (Ticker::MemtableMiss, 1)]
    );
    let interval = db.statistics().expect("statistics on");
    assert_eq!(interval.histogram(Histogram::GetMicros).count(), 0);
    db.close().await.expect("closed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reads_of_a_table_file_count_the_cache_and_the_filter() {
    let storage = FaultStorage::default();
    let db = open(&storage).await;
    put(&db, &[b"apple", b"cherry"]).await;
    db.flush().await.expect("flushed");
    let _ = db.statistics();
    let before = db.statistics().expect("statistics on");
    assert!(get(&db, b"apple").await.is_some());
    let first = moved(&db, &before);
    let read = first
        .iter()
        .find(|(ticker, _)| *ticker == Ticker::BlockBytesRead)
        .map(|(_, bytes)| *bytes);
    assert!(read.is_some_and(|bytes| bytes > 0), "{first:?}");
    let expected = [
        (Ticker::GetCalls, 1),
        (Ticker::MemtableMiss, 1),
        (Ticker::BlockCacheMiss, 1),
        (Ticker::BlockBytesRead, read.unwrap_or(0)),
        (Ticker::BloomMaybe, 1),
    ];
    assert_eq!(first, expected);
    let before = db.statistics().expect("statistics on");
    assert!(get(&db, b"apple").await.is_some());
    assert_eq!(
        moved(&db, &before),
        [
            (Ticker::GetCalls, 1),
            (Ticker::MemtableMiss, 1),
            (Ticker::BlockCacheHit, 1),
            (Ticker::BloomMaybe, 1),
        ]
    );
    let interval = db.statistics().expect("statistics on");
    assert_eq!(interval.histogram(Histogram::GetMicros).count(), 0);
    db.close().await.expect("closed");
}

/// Keys inside a table file's range that it does not hold: the filter excludes nearly all of
/// them without a block read, and every lookup is either excluded or a maybe.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_filter_excludes_absent_keys_within_a_files_range() {
    let storage = FaultStorage::default();
    let db = open(&storage).await;
    put(&db, &[b"a", b"z"]).await;
    db.flush().await.expect("flushed");
    let before = db.statistics().expect("statistics on");
    for index in 0..100_u32 {
        assert!(get(&db, format!("m{index}").as_bytes()).await.is_none());
    }
    let after = db.statistics().expect("statistics on");
    let useful = after.ticker(Ticker::BloomUseful) - before.ticker(Ticker::BloomUseful);
    let maybe = after.ticker(Ticker::BloomMaybe) - before.ticker(Ticker::BloomMaybe);
    assert_eq!(useful + maybe, 100);
    assert!(useful >= 90, "useful {useful}, maybe {maybe}");
    assert_eq!(after.histogram(Histogram::GetMicros).count(), 100);
    db.close().await.expect("closed");
}
