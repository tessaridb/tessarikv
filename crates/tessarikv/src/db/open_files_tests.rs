//! A bound on open table files: reads reopen files let go, and retired files let go of theirs.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use super::Db;
use super::flush_tests::{PATH, until, value};
use crate::storage::counted::CountedStorage;
use crate::{
    CompactionStyle, DbOptions, KeyspaceId, KeyspaceOptions, ReadOptions, WriteBatch, WriteOptions,
};

const TABLES: u32 = 8;

/// First-in-first-out keyspaces never merge tables, so every flush leaves one more file.
fn options(max_open_files: u32) -> DbOptions {
    let keyspace = KeyspaceOptions::builder()
        .compaction(CompactionStyle::Fifo {
            max_table_bytes: 1 << 30,
        })
        .build()
        .expect("valid");
    DbOptions::builder()
        .default_keyspace(keyspace.clone())
        .keyspace("users", keyspace)
        .create_missing_keyspaces(true)
        .max_open_files(max_open_files)
        .build()
        .expect("valid")
}

/// `TABLES` table files in `keyspace`, one key each, flushed one by one.
async fn tables(db: &Db, keyspace: KeyspaceId) {
    for index in 0..TABLES {
        let mut batch = WriteBatch::new();
        batch
            .put(keyspace, &index.to_be_bytes(), &value(index))
            .expect("entry");
        db.write(&batch, &WriteOptions::default())
            .await
            .expect("written");
        db.flush().await.expect("flushed");
    }
}

/// Reads every key of `keyspace`, past any block cache.
async fn read_all(db: &Db, keyspace: KeyspaceId) {
    let options = ReadOptions {
        fill_cache: false,
        ..ReadOptions::default()
    };
    for index in 0..TABLES {
        let found = db
            .get(keyspace, &index.to_be_bytes(), &options)
            .await
            .expect("read");
        assert_eq!(found, Some(value(index)), "key {index}");
    }
}

fn open_files(storage: &CountedStorage) -> u64 {
    storage.reads.open.load(Ordering::Relaxed)
}

/// With a bound of two, reading eight table files keeps at most two open, every key still read;
/// without one, every table file stays open.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reads_keep_at_most_the_bound_of_table_files_open() {
    for (bound, expected) in [(0, u64::from(TABLES)), (2, 2)] {
        let storage = CountedStorage::new(false);
        let db = Db::open_with_storage(PATH, options(bound), Arc::new(storage.clone()))
            .await
            .expect("opened");
        tables(&db, KeyspaceId::DEFAULT).await;
        read_all(&db, KeyspaceId::DEFAULT).await;
        assert_eq!(
            db.view.load().files(0).count(),
            usize::try_from(TABLES).expect("small")
        );
        if bound == 0 {
            assert_eq!(open_files(&storage), expected, "every table file open");
        } else {
            assert!(
                open_files(&storage) <= expected,
                "{} files open over a bound of {bound}",
                open_files(&storage)
            );
        }
    }
}

/// A dropped keyspace's table files let go of their handles once removed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retired_table_files_let_go_of_their_handles() {
    let storage = CountedStorage::new(false);
    let db = Db::open_with_storage(PATH, options(64), Arc::new(storage.clone()))
        .await
        .expect("opened");
    let users = db.keyspace("users").expect("users");
    tables(&db, users).await;
    read_all(&db, users).await;
    assert!(open_files(&storage) > 0, "the read files are kept open");
    db.drop_keyspace(users).await.expect("dropped");
    until("the dropped keyspace's files are let go", || {
        open_files(&storage) == 0
    })
    .await;
}
