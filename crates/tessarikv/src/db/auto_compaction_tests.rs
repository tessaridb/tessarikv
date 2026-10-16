//! A keyspace with automatic compactions turned off still compacts on request (the choice to
//! start none by itself is tested where it is made, in `compaction::pick_tests`).

use std::path::Path;
use std::sync::Arc;

use super::Db;
use super::flush_tests::{PATH, key, value};
use crate::log::fault::FaultStorage;
use crate::storage::Storage as _;
use crate::{DbOptions, KeyspaceId, KeyspaceOptions, ReadOptions, WriteBatch, WriteOptions};

fn options(disabled: bool) -> DbOptions {
    let keyspace = KeyspaceOptions::builder()
        .level0_compaction_trigger(2)
        .level0_slowdown_trigger(20)
        .level0_stop_trigger(30)
        .disable_auto_compactions(disabled)
        .build()
        .expect("valid");
    DbOptions::builder()
        .default_keyspace(keyspace)
        .build()
        .expect("valid")
}

fn tables(storage: &FaultStorage) -> usize {
    storage
        .list(Path::new(PATH))
        .expect("listable")
        .iter()
        .filter(|name| name.ends_with(".sst"))
        .count()
}

/// Six flushes past a trigger of two with automatic compactions off; a manual compaction merges
/// them and every value reads back.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flushes_stay_uncompacted_until_a_manual_compaction() {
    assert!(!KeyspaceOptions::default().disable_auto_compactions());
    let storage = FaultStorage::default();
    let db = Db::open_with_storage(PATH, options(true), Arc::new(storage.clone()))
        .await
        .expect("opened");
    for index in 0..6 {
        let mut batch = WriteBatch::new();
        batch
            .put(KeyspaceId::DEFAULT, &key(index), &value(index))
            .expect("entry");
        db.write(&batch, &WriteOptions::default())
            .await
            .expect("written");
        db.flush().await.expect("flushed");
    }
    db.close().await.expect("closed");
    assert_eq!(tables(&storage), 6);
    let db = Db::open_with_storage(PATH, options(true), Arc::new(storage.clone()))
        .await
        .expect("reopened");
    db.compact_range(KeyspaceId::DEFAULT, b"", None)
        .await
        .expect("compacted");
    db.close().await.expect("closed");
    assert!(tables(&storage) < 6, "{} tables", tables(&storage));
    let db = Db::open_with_storage(PATH, options(true), Arc::new(storage))
        .await
        .expect("reopened");
    for index in 0..6 {
        let read = db
            .get(KeyspaceId::DEFAULT, &key(index), &ReadOptions::default())
            .await
            .expect("read");
        assert_eq!(read, Some(value(index)));
    }
    db.close().await.expect("closed");
}
