//! Reopening a keyspace with another compaction style: every switch that keeps the levels'
//! order opens and reads what was written, and first-in-first-out over tables below level 0,
//! which it could never remove, is refused by name.

use std::path::Path;
use std::sync::Arc;

use crate::db::Db;
use crate::db::flush_tests::{PATH, key, value};
use crate::log::fault::FaultStorage;
use crate::{
    CompactionStyle, DbOptions, Error, KeyspaceId, KeyspaceOptions, ReadOptions, WriteBatch,
    WriteOptions,
};

const KEYS: u32 = 50;

fn options(style: CompactionStyle) -> DbOptions {
    let keyspace = KeyspaceOptions::builder()
        .compaction(style)
        .build()
        .expect("valid");
    DbOptions::builder()
        .default_keyspace(keyspace)
        .build()
        .expect("valid")
}

async fn open(storage: &FaultStorage, style: CompactionStyle) -> crate::Result<Db> {
    Db::open_with_storage(PATH, options(style), Arc::new(storage.clone())).await
}

/// Writes every key and flushes; with `compact`, moves the tables to the bottom level.
async fn fill(db: &Db, compact: bool) {
    let mut batch = WriteBatch::new();
    for index in 0..KEYS {
        batch
            .put(KeyspaceId::DEFAULT, &key(index), &value(index))
            .expect("entry");
    }
    db.write(&batch, &WriteOptions::default())
        .await
        .expect("written");
    db.flush().await.expect("flushed");
    if compact {
        db.compact_range(KeyspaceId::DEFAULT, b"", None)
            .await
            .expect("compacted");
    }
}

async fn reads_back(db: &Db) {
    for index in 0..KEYS {
        let read = db
            .get(KeyspaceId::DEFAULT, &key(index), &ReadOptions::default())
            .await
            .expect("read");
        assert_eq!(read, Some(value(index)), "key {index}");
    }
}

/// A store written with `from`, its tables in the bottom level when `deep`, reopened with `to`.
async fn reopened(from: CompactionStyle, deep: bool, to: CompactionStyle) -> crate::Result<Db> {
    let storage = FaultStorage::default();
    let db = open(&storage, from).await.expect("opened");
    fill(&db, deep).await;
    db.close().await.expect("closed");
    open(&storage.crash(Path::new(PATH), false), to).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn first_in_first_out_over_tables_below_level_zero_is_refused() {
    let refused = reopened(CompactionStyle::Leveled, true, CompactionStyle::fifo()).await;
    assert!(
        matches!(
            &refused,
            Err(Error::InvalidOptions {
                option: "keyspace.compaction",
                ..
            })
        ),
        "{:?}",
        refused.as_ref().err()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn switches_that_keep_the_order_of_the_levels_open_and_read_back() {
    let cases = [
        (CompactionStyle::Leveled, true, CompactionStyle::universal()),
        (CompactionStyle::universal(), true, CompactionStyle::Leveled),
        (CompactionStyle::fifo(), false, CompactionStyle::Leveled),
        (CompactionStyle::Leveled, false, CompactionStyle::fifo()),
    ];
    for (from, deep, to) in cases {
        let db = reopened(from, deep, to)
            .await
            .unwrap_or_else(|error| panic!("{from:?} -> {to:?}: {error}"));
        reads_back(&db).await;
        fill(&db, to != CompactionStyle::fifo()).await;
        reads_back(&db).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_named_keyspace_is_checked_too() {
    let named = |style| {
        let keyspace = KeyspaceOptions::builder()
            .compaction(style)
            .build()
            .expect("valid");
        DbOptions::builder()
            .keyspace("logs", keyspace)
            .create_missing_keyspaces(true)
            .build()
            .expect("valid")
    };
    let storage = FaultStorage::default();
    let db = Db::open_with_storage(
        PATH,
        named(CompactionStyle::Leveled),
        Arc::new(storage.clone()),
    )
    .await
    .expect("opened");
    let logs = db.keyspace("logs").expect("named keyspace");
    let mut batch = WriteBatch::new();
    batch.put(logs, b"k", b"v").expect("entry");
    db.write(&batch, &WriteOptions::default())
        .await
        .expect("written");
    db.flush().await.expect("flushed");
    db.compact_range(logs, b"", None).await.expect("compacted");
    db.close().await.expect("closed");
    let refused = Db::open_with_storage(
        PATH,
        named(CompactionStyle::fifo()),
        Arc::new(storage.crash(Path::new(PATH), false)),
    )
    .await;
    assert!(
        matches!(&refused, Err(Error::InvalidOptions { option: "keyspace.compaction", reason })
            if reason.contains("`logs`")),
        "{:?}",
        refused.as_ref().err()
    );
}
