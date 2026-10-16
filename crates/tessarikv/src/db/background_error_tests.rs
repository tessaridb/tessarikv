//! The failure that stopped the database's background work, read without writing.

use super::Db;
use super::flush_tests::open;
use crate::log::fault::FaultStorage;
use crate::{DbOptions, Durability, KeyspaceId, WriteBatch, WriteOptions};

async fn put(db: &Db, key: &[u8], durability: Durability) -> crate::Result<()> {
    let mut batch = WriteBatch::new();
    batch.put(KeyspaceId::DEFAULT, key, b"v").expect("entry");
    let options = WriteOptions {
        durability,
        ..WriteOptions::default()
    };
    db.write(&batch, &options).await.map(|_| ())
}

fn code(db: &Db) -> Option<&'static str> {
    db.background_error().map(|error| error.code())
}

/// A healthy database, on storage or in memory, reports nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_healthy_database_reports_no_background_error() {
    let db = open(&FaultStorage::default()).await;
    put(&db, b"k", Durability::Sync).await.expect("written");
    db.flush().await.expect("flushed");
    assert_eq!(code(&db), None);
    let memory = Db::open("memory", DbOptions::in_memory())
        .await
        .expect("opened");
    assert_eq!(code(&memory), None);
}

/// A failed log sync is reported from then on, without another write.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_log_sync_is_reported() {
    let storage = FaultStorage::default();
    let db = open(&storage).await;
    storage.fail_syncs();
    let refused = put(&db, b"k", Durability::Sync).await;
    assert_eq!(refused.err().map(|error| error.code()), Some("log_failed"));
    assert_eq!(code(&db), Some("log_failed"));
}

/// A failed flush is reported from then on, without another write.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_flush_is_reported() {
    let storage = FaultStorage::default();
    let db = open(&storage).await;
    put(&db, b"k", Durability::Buffered).await.expect("written");
    storage.fail_syncs();
    let refused = db.flush().await;
    assert_eq!(
        refused.err().map(|error| error.code()),
        Some("flush_failed")
    );
    assert_eq!(code(&db), Some("flush_failed"));
}
