//! Syncing the log on demand: batches written without a sync become durable when asked.

use std::path::Path;
use std::sync::Arc;

use super::Db;
use super::flush_tests::PATH;
use crate::log::fault::FaultStorage;
use crate::{DbOptions, Durability, KeyspaceId, ReadOptions, WriteBatch, WriteOptions};

async fn open(storage: &FaultStorage) -> Db {
    Db::open_with_storage(PATH, DbOptions::default(), Arc::new(storage.clone()))
        .await
        .expect("opened")
}

async fn put_buffered(db: &Db, key: &[u8]) {
    let mut batch = WriteBatch::new();
    batch.put(KeyspaceId::DEFAULT, key, b"v").expect("entry");
    let options = WriteOptions {
        durability: Durability::Buffered,
        ..WriteOptions::default()
    };
    db.write(&batch, &options).await.expect("written");
}

/// Syncs the log, bounded so that a sync nobody serves fails instead of hanging.
async fn sync(db: &Db) {
    tokio::time::timeout(std::time::Duration::from_secs(10), db.sync_wal())
        .await
        .expect("the sync is served")
        .expect("synced");
}

async fn present_after_power_loss(storage: &FaultStorage, key: &[u8]) -> bool {
    let reopened = open(&storage.crash(Path::new(PATH), true)).await;
    reopened
        .get(KeyspaceId::DEFAULT, key, &ReadOptions::default())
        .await
        .expect("read")
        .is_some()
}

/// A buffered batch is lost with the power unless the log is synced; after `sync_wal` it
/// survives, and so does every buffered batch written before the call.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn buffered_batches_survive_a_power_loss_once_the_log_is_synced() {
    let storage = FaultStorage::default();
    let db = open(&storage).await;
    put_buffered(&db, b"a").await;
    assert!(
        !present_after_power_loss(&storage, b"a").await,
        "a buffered batch is not durable on its own"
    );
    put_buffered(&db, b"b").await;
    sync(&db).await;
    assert!(present_after_power_loss(&storage, b"a").await);
    assert!(present_after_power_loss(&storage, b"b").await);
}

/// Nothing written yet, or a database in memory: syncing succeeds and changes nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn syncing_with_nothing_to_sync_succeeds() {
    let db = open(&FaultStorage::default()).await;
    sync(&db).await;
    sync(&db).await;
    let memory = Db::open("memory", DbOptions::in_memory())
        .await
        .expect("opened");
    sync(&memory).await;
}
