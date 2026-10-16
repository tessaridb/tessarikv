//! Writes are admitted by every keyspace's own limits: a keyspace's in-memory tables are frozen
//! at its own size, and its level 0 holds writes by its own triggers from the moment the
//! database opens.

use std::sync::Arc;

use super::Db;
use super::flush_tests::{PATH, key, until, value};
use crate::log::fault::FaultStorage;
use crate::{CompactionStyle, DbOptions, KeyspaceId, KeyspaceOptions, WriteBatch, WriteOptions};

/// A keyspace with small in-memory tables is flushed each time its own table fills, beside a
/// default keyspace whose tables are far larger; first-in-first-out, its tables are never
/// merged, so each switch leaves one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_keyspace_table_is_frozen_at_its_own_size() {
    let small = KeyspaceOptions::builder()
        .memtable_bytes(64 * 1024)
        .compaction(CompactionStyle::Fifo {
            max_table_bytes: 1 << 30,
        })
        .build()
        .expect("valid");
    let options = DbOptions::builder()
        .keyspace("users", small)
        .create_missing_keyspaces(true)
        .build()
        .expect("valid");
    let db = Db::open_with_storage(PATH, options, Arc::new(FaultStorage::default()))
        .await
        .expect("opened");
    let users = db.keyspace("users").expect("users");
    for index in 0..400 {
        let mut batch = WriteBatch::new();
        batch.put(users, &key(index), &value(index)).expect("entry");
        db.write(&batch, &WriteOptions::default())
            .await
            .expect("written");
    }
    until("three users tables on file", || {
        db.view.load().files(users.get()).count() >= 3
    })
    .await;
    assert_eq!(
        db.view.load().files(0).count(),
        0,
        "the default keyspace held nothing"
    );
}

/// Users options: compacting level 0 at `compact` tables, slowing writes at `slowdown` and
/// stopping them at `stop`.
fn users(compact: u32, slowdown: u32, stop: u32) -> DbOptions {
    let keyspace = KeyspaceOptions::builder()
        .level0_compaction_trigger(compact)
        .level0_slowdown_trigger(slowdown)
        .level0_stop_trigger(stop)
        .build()
        .expect("valid");
    DbOptions::builder()
        .keyspace("users", keyspace)
        .create_missing_keyspaces(true)
        .build()
        .expect("valid")
}

/// A database reopened with a keyspace's level 0 already past its stop trigger compacts that
/// keyspace without waiting for a write to wake it, so a write held by it goes through.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn level_zero_past_a_trigger_at_open_holds_writes_until_compacted() {
    let storage = FaultStorage::default();
    let open = |options| Db::open_with_storage(PATH, options, Arc::new(storage.clone()));
    let db = open(users(8, 20, 36)).await.expect("opened");
    let users_id = db.keyspace("users").expect("users");
    for index in 0..5 {
        let mut batch = WriteBatch::new();
        batch
            .put(users_id, &key(index), &value(index))
            .expect("entry");
        db.write(&batch, &WriteOptions::default())
            .await
            .expect("written");
        db.flush().await.expect("flushed");
    }
    assert_eq!(db.view.load().level0_files(users_id.get()), 5);
    db.close().await.expect("closed");

    let db = open(users(2, 3, 4)).await.expect("reopened");
    let mut batch = WriteBatch::new();
    batch.put(KeyspaceId::DEFAULT, b"k", b"v").expect("entry");
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        db.write(&batch, &WriteOptions::default()),
    )
    .await
    .expect("compacting the users keyspace released the write")
    .expect("written");
}
