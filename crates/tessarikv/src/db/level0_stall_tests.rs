//! Writes slow down and then stop while level 0 holds more tables than compaction keeps up
//! with, and go on once an install takes it back under the limit.

use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use tessarikv_types::{SequenceNumber, ValueKind};

use super::block_cache::BlockCache;
use super::flush_signals::FlushSignals;
use super::read_tests::{file, user_key};
use super::view::View;
use crate::memtable::Memtable;
use crate::{Error, KeyspaceOptions};

/// A mebibyte a second.
const RATE: u64 = 1024 * 1024;

fn keyspace() -> KeyspaceOptions {
    KeyspaceOptions::builder()
        .level0_compaction_trigger(2)
        .level0_slowdown_trigger(3)
        .level0_stop_trigger(4)
        .build()
        .expect("valid")
}

/// A view whose level 0 holds `tables` tables.
fn view_with(tables: u64) -> View {
    let cache = Arc::new(BlockCache::new(0, 256));
    let files = (1..=tables)
        .rev()
        .map(|id| {
            let table = Memtable::new();
            table
                .insert(
                    SequenceNumber::new(id).unwrap(),
                    ValueKind::Value,
                    &user_key(1),
                    b"v",
                )
                .unwrap();
            file(&table, id, &cache)
        })
        .collect();
    View::recovered(Memtable::new(), files)
}

fn refusal(result: crate::Result<()>) -> &'static str {
    match result {
        Err(Error::WriteStalled { reason }) => reason,
        other => panic!("expected a stalled write, got {other:?}"),
    }
}

#[tokio::test]
async fn level_zero_past_its_slowdown_trigger_refuses_a_write_that_must_not_wait() {
    let signals = FlushSignals::new(&keyspace(), RATE);
    let below = ArcSwap::from_pointee(view_with(2));
    signals
        .admit(&below, true, 1)
        .await
        .expect("under the slowdown trigger");
    let slowed = ArcSwap::from_pointee(view_with(3));
    let reason = refusal(signals.admit(&slowed, true, 1).await);
    assert!(reason.contains("slowdown"), "{reason}");
    signals
        .admit(&slowed, false, 1)
        .await
        .expect("a write that may wait goes after its delay");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn level_zero_at_its_stop_trigger_holds_writes_until_an_install_takes_it_under() {
    let signals = Arc::new(FlushSignals::new(&keyspace(), RATE));
    let view = Arc::new(ArcSwap::from_pointee(view_with(4)));
    let reason = refusal(signals.admit(&view, true, 1).await);
    assert!(reason.contains("stop"), "{reason}");
    let waiting = {
        let (signals, view) = (Arc::clone(&signals), Arc::clone(&view));
        tokio::spawn(async move { signals.admit(&view, false, 1).await })
    };
    for _ in 0..1000 {
        tokio::task::yield_now().await;
    }
    assert!(
        !waiting.is_finished(),
        "a write went through at the stop trigger"
    );
    signals.installed();
    for _ in 0..1000 {
        tokio::task::yield_now().await;
    }
    assert!(
        !waiting.is_finished(),
        "an install that left level 0 full released a write"
    );
    view.store(Arc::new(view_with(2)));
    signals.installed();
    tokio::time::timeout(std::time::Duration::from_secs(10), waiting)
        .await
        .expect("released")
        .expect("joined")
        .expect("admitted");
}

#[test]
fn a_failed_compaction_is_reported_as_one() {
    let signals = FlushSignals::new(&keyspace(), RATE);
    signals.fail(&Error::CompactionFailed);
    let refused = futures_lite_block(signals.admit(&ArcSwap::from_pointee(view_with(0)), true, 1));
    assert!(
        matches!(refused, Err(Error::CompactionFailed)),
        "{refused:?}"
    );
}

fn futures_lite_block<T>(future: impl std::future::Future<Output = T>) -> T {
    super::read_tests::block_on(future)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn writes_stopped_by_level_zero_go_on_once_compactions_take_it_under() {
    use super::Db;
    use super::flush_tests::{PATH, key, value};
    use crate::log::fault::FaultStorage;
    use crate::{DbOptions, KeyspaceId, ReadOptions, WriteBatch, WriteOptions};

    // Writes stop at the very count that starts a compaction, so every stop is lifted by a
    // compaction rather than by a flush.
    let keyspace = KeyspaceOptions::builder()
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
        .wal_segment_bytes(1024 * 1024)
        .build()
        .expect("valid");
    let storage = FaultStorage::default();
    let db = Db::open_with_storage(PATH, options, Arc::new(storage))
        .await
        .expect("opened");
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        for index in 0..300 {
            let mut batch = WriteBatch::new();
            batch
                .put(KeyspaceId::DEFAULT, &key(index), &value(index))
                .expect("small entry");
            db.write(&batch, &WriteOptions::default())
                .await
                .expect("written");
        }
    })
    .await
    .expect("writes held by level 0 were released by compactions");
    for index in 0..300 {
        let read = db
            .get(KeyspaceId::DEFAULT, &key(index), &ReadOptions::default())
            .await
            .expect("readable");
        assert_eq!(read, Some(value(index)), "key {index}");
    }
    db.close().await.expect("closed");
}

#[tokio::test(start_paused = true)]
async fn a_write_past_the_slowdown_trigger_waits_for_its_bytes_at_the_delayed_rate() {
    let signals = FlushSignals::new(&keyspace(), RATE);
    let slowed = ArcSwap::from_pointee(view_with(3));
    let start = tokio::time::Instant::now();
    signals.admit(&slowed, false, 512).await.expect("admitted");
    assert_eq!(
        start.elapsed(),
        Duration::ZERO,
        "within a millisecond's worth"
    );
    let start = tokio::time::Instant::now();
    signals.admit(&slowed, false, RATE).await.expect("admitted");
    let paid = start.elapsed();
    assert!(
        paid >= Duration::from_millis(990) && paid <= Duration::from_millis(1010),
        "a mebibyte at a mebibyte a second, less the burst: {paid:?}"
    );
    let start = tokio::time::Instant::now();
    let below = ArcSwap::from_pointee(view_with(2));
    signals.admit(&below, false, RATE).await.expect("admitted");
    assert_eq!(
        start.elapsed(),
        Duration::ZERO,
        "under the trigger nothing waits"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_delayed_write_rate_reaches_the_installer() {
    let options = crate::DbOptions::builder()
        .delayed_write_bytes_per_second(3 * RATE)
        .build()
        .expect("valid");
    let db = super::Db::open_with_storage(
        super::flush_tests::PATH,
        options,
        Arc::new(crate::log::fault::FaultStorage::default()),
    )
    .await
    .expect("opened");
    let installer = db.installer.as_ref().expect("on storage");
    assert_eq!(installer.signals.delayed_rate(), Some(3 * RATE));
    db.close().await.expect("closed");
}
