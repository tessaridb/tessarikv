//! The write-path and background statistics: each ticker moves by exactly what the writes, the
//! flushes and the compactions did, measured against what is on storage.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use super::Db;
use super::flush_tests::PATH;
use crate::log::fault::FaultStorage;
use crate::storage::Storage as _;
use crate::{
    DbOptions, Durability, Histogram, KeyspaceId, StatisticsSnapshot, Ticker, WriteBatch,
    WriteOptions,
};

async fn open(storage: &FaultStorage) -> Db {
    let options = DbOptions::builder()
        .statistics(true)
        .build()
        .expect("valid");
    Db::open_with_storage(PATH, options, Arc::new(storage.clone()))
        .await
        .expect("opened")
}

fn batch(keys: impl IntoIterator<Item = u32>) -> WriteBatch {
    let mut batch = WriteBatch::new();
    for key in keys {
        batch
            .put(KeyspaceId::DEFAULT, &key.to_be_bytes(), b"value")
            .expect("entry");
    }
    batch
}

fn read(db: &Db) -> StatisticsSnapshot {
    db.statistics().expect("statistics on")
}

fn delta(after: &StatisticsSnapshot, before: &StatisticsSnapshot, ticker: Ticker) -> u64 {
    after.ticker(ticker) - before.ticker(ticker)
}

/// The table files on storage and their sizes.
fn tables(storage: &FaultStorage) -> BTreeMap<String, u64> {
    storage
        .list(Path::new(PATH))
        .expect("listable")
        .into_iter()
        .filter(|name| name.ends_with(".sst"))
        .map(|name| {
            let size = u64::try_from(storage.bytes(&Path::new(PATH).join(&name)).len()).unwrap();
            (name, size)
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn writes_count_entries_bytes_log_and_time() {
    let storage = FaultStorage::default();
    let db = open(&storage).await;
    let before = read(&db);
    let synced = batch(0..3);
    db.write(&synced, &WriteOptions::default())
        .await
        .expect("written");
    let after = read(&db);
    assert_eq!(delta(&after, &before, Ticker::KeysWritten), 3);
    let bytes = u64::try_from(synced.as_bytes().len()).unwrap();
    assert_eq!(delta(&after, &before, Ticker::BytesWritten), bytes);
    assert!(
        delta(&after, &before, Ticker::WalBytes) > bytes,
        "frame and header"
    );
    assert_eq!(delta(&after, &before, Ticker::WalSyncs), 1);
    assert_eq!(after.histogram(Histogram::WriteMicros).count(), 1);
    let buffered = WriteOptions {
        durability: Durability::Buffered,
        ..WriteOptions::default()
    };
    db.write(&batch(3..4), &buffered).await.expect("written");
    let last = read(&db);
    assert_eq!(delta(&last, &after, Ticker::KeysWritten), 1);
    assert_eq!(
        delta(&last, &after, Ticker::WalSyncs),
        0,
        "buffered: no sync"
    );
    db.close().await.expect("closed");
}

/// A refused write counts no entries and no bytes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refused_write_counts_nothing_written() {
    let storage = FaultStorage::default();
    let db = open(&storage).await;
    let before = read(&db);
    let mut unknown = WriteBatch::new();
    unknown
        .put(KeyspaceId::new(9), b"key", b"value")
        .expect("entry");
    assert!(db.write(&unknown, &WriteOptions::default()).await.is_err());
    let after = read(&db);
    assert_eq!(delta(&after, &before, Ticker::KeysWritten), 0);
    assert_eq!(delta(&after, &before, Ticker::BytesWritten), 0);
    db.close().await.expect("closed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flushes_and_compactions_count_the_bytes_on_storage() {
    let storage = FaultStorage::default();
    let db = open(&storage).await;
    let options = WriteOptions::default();
    db.write(&batch(0..100), &options).await.expect("written");
    let before = read(&db);
    db.flush().await.expect("flushed");
    let first = tables(&storage);
    assert_eq!(first.len(), 1);
    let after = read(&db);
    assert_eq!(
        delta(&after, &before, Ticker::FlushBytes),
        first.values().sum::<u64>()
    );
    // Every key again: the compaction keeps the newer version of each.
    db.write(&batch(0..100), &options).await.expect("written");
    db.flush().await.expect("flushed");
    let inputs = tables(&storage);
    assert_eq!(inputs.len(), 2);
    let before = read(&db);
    db.compact_range(KeyspaceId::DEFAULT, b"", None)
        .await
        .expect("compacted");
    let after = read(&db);
    let outputs: BTreeMap<_, _> = tables(&storage)
        .into_iter()
        .filter(|(name, _)| !inputs.contains_key(name))
        .collect();
    assert_eq!(
        delta(&after, &before, Ticker::CompactionBytesRead),
        inputs.values().sum::<u64>()
    );
    assert_eq!(
        delta(&after, &before, Ticker::CompactionBytesWritten),
        outputs.values().sum::<u64>()
    );
    assert_eq!(delta(&after, &before, Ticker::CompactionKeysDropped), 100);
    assert_eq!(delta(&after, &before, Ticker::FlushBytes), 0);
    db.close().await.expect("closed");
}

/// Writes held while level 0 is at its stop trigger count the time they waited.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn writes_held_by_level_zero_count_their_stall() {
    use super::flush_tests::{key, value};
    let keyspace = crate::KeyspaceOptions::builder()
        .memtable_bytes(64 * 1024)
        .level0_compaction_trigger(2)
        .level0_slowdown_trigger(2)
        .level0_stop_trigger(2)
        .level_base_bytes(64 * 1024)
        .level_size_multiplier(2)
        .target_file_bytes(16 * 1024)
        .build()
        .expect("valid");
    let options = DbOptions::builder()
        .default_keyspace(keyspace)
        .statistics(true)
        .build()
        .expect("valid");
    let storage = FaultStorage::default();
    let db = Db::open_with_storage(PATH, options, Arc::new(storage))
        .await
        .expect("opened");
    let before = read(&db);
    for index in 0..300 {
        let mut batch = WriteBatch::new();
        batch
            .put(KeyspaceId::DEFAULT, &key(index), &value(index))
            .expect("small entry");
        db.write(&batch, &WriteOptions::default())
            .await
            .expect("written");
    }
    let after = read(&db);
    assert!(delta(&after, &before, Ticker::StallMicros) > 0);
    assert_eq!(delta(&after, &before, Ticker::KeysWritten), 300);
    db.close().await.expect("closed");
}
