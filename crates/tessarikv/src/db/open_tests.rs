//! Opening a database: what the replay of its log leaves behind.

use std::path::Path;

use super::flush_tests::{PATH, files_on_storage, key, open, table_files, until, value};
use crate::log::fault::FaultStorage;
use crate::storage::Storage as _;
use crate::{KeyspaceId, ReadOptions, WriteBatch, WriteOptions};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reopened_database_flushes_what_it_replayed_and_keeps_none_of_that_log() {
    let storage = FaultStorage::default();
    let db = open(&storage).await;
    // Fewer writes than fill a memtable: they reach the reopen only through the log.
    for index in 0..20 {
        let mut batch = WriteBatch::new();
        batch
            .put(KeyspaceId::DEFAULT, &key(index), &value(index))
            .expect("entry");
        db.write(&batch, &WriteOptions::default())
            .await
            .expect("written");
    }
    db.close().await.expect("closed");
    let replayed: Vec<String> = storage
        .list(Path::new(PATH))
        .expect("listable")
        .into_iter()
        .filter(|name| name.ends_with(".log"))
        .collect();
    assert!(!replayed.is_empty(), "the writes are in the log");
    assert_eq!(files_on_storage(&storage, ".sst"), 0, "and nowhere else");

    let db = open(&storage).await;
    assert_eq!(
        table_files(&db),
        1,
        "the replayed writes are in a table file"
    );
    until("the replayed segments are removed", || {
        let names = storage.list(Path::new(PATH)).expect("listable");
        replayed.iter().all(|old| !names.contains(old))
    })
    .await;
    db.close().await.expect("closed");
    let db = open(&storage).await;
    for index in 0..20 {
        let read = db
            .get(KeyspaceId::DEFAULT, &key(index), &ReadOptions::default())
            .await
            .expect("readable");
        assert_eq!(read, Some(value(index)), "key {index}");
    }
    db.close().await.expect("closed");
}

/// An explicit flush writes what the in-memory table holds to a table file before it returns,
/// even though the table is far from full; with nothing new since, it writes nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_flush_writes_the_in_memory_table_to_a_table_file_before_it_returns() {
    let storage = FaultStorage::default();
    let db = open(&storage).await;
    for index in 0..5 {
        let mut batch = WriteBatch::new();
        batch
            .put(KeyspaceId::DEFAULT, &key(index), &value(index))
            .expect("entry");
        db.write(&batch, &WriteOptions::default())
            .await
            .expect("written");
    }
    assert_eq!(table_files(&db), 0);
    db.flush().await.expect("flushed");
    assert_eq!(
        table_files(&db),
        1,
        "the table is on file when flush returns"
    );
    db.flush().await.expect("flushed again");
    assert_eq!(table_files(&db), 1, "nothing new, nothing written");
    let read = db
        .get(KeyspaceId::DEFAULT, &key(3), &ReadOptions::default())
        .await
        .expect("read");
    assert_eq!(read, Some(value(3)));
}

#[tokio::test]
async fn a_flush_in_memory_has_nothing_to_do() {
    let db = crate::Db::open("memory", crate::DbOptions::in_memory())
        .await
        .expect("opened");
    db.flush().await.expect("nothing to flush");
}
