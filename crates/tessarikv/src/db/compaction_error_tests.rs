//! A compaction that fails leaves the database writable: its inputs stay, the files it began are
//! removed, and it runs again once the cause is gone.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use super::Db;
use super::flush_tests::{PATH, key};
use crate::log::fault::FaultStorage;
use crate::storage::Storage;
use crate::{
    DbOptions, Error, KeyspaceId, KeyspaceOptions, ReadOptions, Ticker, WriteBatch, WriteOptions,
};

const KEYS: u32 = 64;
const TABLES: u32 = 4;

fn options(auto: bool) -> DbOptions {
    let keyspace = KeyspaceOptions::builder()
        .level0_compaction_trigger(TABLES)
        .disable_auto_compactions(!auto)
        .build()
        .unwrap();
    DbOptions::builder()
        .default_keyspace(keyspace)
        .statistics(true)
        .build()
        .unwrap()
}

async fn open(storage: &FaultStorage, auto: bool) -> Db {
    Db::open_with_storage(PATH, options(auto), Arc::new(storage.clone()))
        .await
        .unwrap()
}

async fn put(db: &Db, index: u32) -> crate::Result<()> {
    let mut batch = WriteBatch::new();
    batch
        .put(KeyspaceId::DEFAULT, &key(index), &key(index))
        .unwrap();
    db.write(&batch, &WriteOptions::default()).await.map(|_| ())
}

/// Level 0 holds `TABLES` tables of `KEYS` keys each.
async fn fill(db: &Db) {
    for table in 0..TABLES {
        for index in 0..KEYS {
            put(db, table * KEYS + index).await.unwrap();
        }
        db.flush().await.unwrap();
    }
}

/// The table files on disk.
fn tables_on_disk(storage: &FaultStorage) -> usize {
    storage
        .list(Path::new(PATH))
        .unwrap()
        .iter()
        .filter(|name| name.ends_with(".sst"))
        .count()
}

/// Waits, within ten seconds, until `done` holds.
async fn until(what: &str, mut done: impl FnMut() -> bool) {
    let waited = tokio::time::timeout(Duration::from_secs(10), async {
        while !done() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    assert!(waited.is_ok(), "{what} within ten seconds");
}

async fn assert_every_key(db: &Db) {
    for index in 0..TABLES * KEYS {
        let read = db
            .get(KeyspaceId::DEFAULT, &key(index), &ReadOptions::default())
            .await
            .unwrap();
        assert_eq!(read, Some(key(index)), "key {index}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failing_compaction_neither_stops_writes_nor_leaves_its_files() {
    let storage = FaultStorage::default();
    let db = open(&storage, false).await;
    fill(&db).await;
    db.close().await.unwrap();
    storage.fail_table_writes(true);
    // Level 0 is at its trigger, so the compaction starts as the database opens, and fails.
    let db = open(&storage, true).await;
    let failures = || db.statistics().unwrap().ticker(Ticker::CompactionFailures);
    until("a failed compaction counted", || failures() > 0).await;
    // Failed jobs wait before they run again (100 ms, then 200 ms, ...): a quarter of a second
    // holds three at most, however slow the machine.
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert!(failures() <= 3, "{} failures in 250 ms", failures());
    put(&db, TABLES * KEYS).await.expect("writes go on");
    assert!(db.background_error().is_none(), "no background error");
    assert_eq!(db.view.load().level0_files(0), 4, "the inputs stay");
    storage.fail_table_writes(false);
    until("the compaction", || db.view.load().level0_files(0) == 0).await;
    until("the inputs and failed outputs removed", || {
        tables_on_disk(&storage) == db.view.load().files(0).count()
    })
    .await;
    assert_every_key(&db).await;
    db.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_range_compaction_that_fails_reports_why_and_the_next_one_runs() {
    let storage = FaultStorage::default();
    let db = open(&storage, false).await;
    fill(&db).await;
    storage.fail_table_writes(true);
    let failed = db.compact_range(KeyspaceId::DEFAULT, b"", None).await;
    assert!(
        matches!(
            failed,
            Err(Error::Io {
                operation: "append",
                ..
            })
        ),
        "the write failure: {failed:?}"
    );
    put(&db, TABLES * KEYS).await.expect("writes go on");
    assert!(db.background_error().is_none(), "no background error");
    storage.fail_table_writes(false);
    tokio::time::timeout(
        Duration::from_secs(10),
        db.compact_range(KeyspaceId::DEFAULT, b"", None),
    )
    .await
    .expect("answered within ten seconds")
    .expect("compacted");
    assert_eq!(db.view.load().level0_files(0), 0, "level 0 compacted");
    assert_every_key(&db).await;
    db.close().await.unwrap();
}
