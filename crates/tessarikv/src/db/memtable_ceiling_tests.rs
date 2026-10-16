//! One ceiling over the newest in-memory tables of every keyspace together.

use std::sync::Arc;

use super::Db;
use super::flush_tests::{PATH, until, value};
use crate::log::fault::FaultStorage;
use crate::{DbOptions, Durability, KeyspaceId, KeyspaceOptions, WriteBatch, WriteOptions};

/// Tables far larger than anything the tests write, so no single one fills.
const TABLE_BYTES: u64 = 64 * 1024 * 1024;

fn options(total_memtable_bytes: u64) -> DbOptions {
    let keyspace = KeyspaceOptions::builder()
        .memtable_bytes(TABLE_BYTES)
        .build()
        .expect("valid");
    DbOptions::builder()
        .default_keyspace(keyspace.clone())
        .keyspace("users", keyspace)
        .create_missing_keyspaces(true)
        .total_memtable_bytes(total_memtable_bytes)
        .build()
        .expect("valid")
}

/// Writes about `bytes` into each of the default keyspace and `users`, alternating.
async fn fill_both(db: &Db, bytes: u64) {
    let users = db.keyspace("users").expect("users");
    let options = WriteOptions {
        durability: Durability::Buffered,
        ..WriteOptions::default()
    };
    let mut written = 0_u64;
    for index in 0_u32.. {
        if written >= bytes {
            break;
        }
        let value = value(index);
        for keyspace in [KeyspaceId::DEFAULT, users] {
            let mut batch = WriteBatch::new();
            batch
                .put(keyspace, &index.to_be_bytes(), &value)
                .expect("entry");
            db.write(&batch, &options).await.expect("written");
        }
        written = written.saturating_add(u64::try_from(value.len()).expect("small"));
    }
}

fn table_files(db: &Db) -> usize {
    let view = db.view.load();
    view.files(0).count().saturating_add(view.files(1).count())
}

/// Neither table is full, but together they reach the database's ceiling: they are frozen and
/// flushed. Without a ceiling the same writes stay in memory.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_newest_tables_together_are_flushed_at_the_ceiling() {
    let open =
        |total| Db::open_with_storage(PATH, options(total), Arc::new(FaultStorage::default()));
    let unbounded = open(0).await.expect("opened");
    fill_both(&unbounded, 2 * 1024 * 1024).await;
    assert_eq!(
        table_files(&unbounded),
        0,
        "no table is full, nothing flushes"
    );

    // Each table stays well under the ceiling on its own; only their sum reaches it.
    let bounded = open(1024 * 1024).await.expect("opened");
    fill_both(&bounded, 640 * 1024).await;
    until("the tables reaching the ceiling are flushed", || {
        table_files(&bounded) > 0
    })
    .await;
}

/// A ceiling below the smallest table a keyspace may have is refused; zero means none.
#[test]
fn a_ceiling_below_the_smallest_table_is_refused() {
    let refused = DbOptions::builder().total_memtable_bytes(1024).build();
    assert_eq!(
        refused.err().map(|error| error.code()),
        Some("invalid_options")
    );
    let options = DbOptions::builder()
        .total_memtable_bytes(0)
        .build()
        .expect("valid");
    assert_eq!(options.total_memtable_bytes(), 0);
}
