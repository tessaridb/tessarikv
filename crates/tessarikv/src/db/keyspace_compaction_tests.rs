//! Every keyspace compacts on its own, by its own options, and a range request compacts the
//! keyspace it names.

use std::sync::Arc;

use tessarikv_constants::LEVELS;

use super::Db;
use super::flush_tests::{PATH, key, until, value};
use crate::log::fault::FaultStorage;
use crate::{DbOptions, KeyspaceId, KeyspaceOptions, WriteBatch, WriteOptions};

/// A database whose default keyspace compacts level 0 at `default_trigger` tables and whose
/// users keyspace at `users_trigger`.
async fn open(default_trigger: u32, users_trigger: u32) -> (Db, KeyspaceId) {
    let trigger = |tables| {
        KeyspaceOptions::builder()
            .level0_compaction_trigger(tables)
            .build()
            .expect("valid")
    };
    let options = DbOptions::builder()
        .default_keyspace(trigger(default_trigger))
        .keyspace("users", trigger(users_trigger))
        .create_missing_keyspaces(true)
        .build()
        .expect("valid");
    let db = Db::open_with_storage(PATH, options, Arc::new(FaultStorage::default()))
        .await
        .expect("opened");
    let users = db.keyspace("users").expect("users");
    (db, users)
}

/// Writes key `index` into both keyspaces and flushes: one level-0 table each.
async fn flush_both(db: &Db, users: KeyspaceId, index: u32) {
    let mut batch = WriteBatch::new();
    for keyspace in [KeyspaceId::DEFAULT, users] {
        batch
            .put(keyspace, &key(index), &value(index))
            .expect("entry");
    }
    db.write(&batch, &WriteOptions::default())
        .await
        .expect("written");
    db.flush().await.expect("flushed");
}

/// The levels of `keyspace`'s tables.
fn levels(db: &Db, keyspace: KeyspaceId) -> Vec<u8> {
    db.view
        .load()
        .files(keyspace.get())
        .map(|file| file.meta.level)
        .collect()
}

/// A keyspace whose level 0 reaches its trigger compacts, while one under its own trigger
/// keeps its tables at level 0.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn each_keyspace_compacts_by_its_own_trigger() {
    let (db, users) = open(8, 2).await;
    for index in 0..4 {
        flush_both(&db, users, index).await;
    }
    until("a users table below level 0", || {
        levels(&db, users).iter().any(|level| *level > 0)
    })
    .await;
    assert_eq!(levels(&db, KeyspaceId::DEFAULT), [0, 0, 0, 0]);
}

/// A range request compacts the keyspace it names down to the bottom level, and no other.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_range_request_compacts_only_the_keyspace_it_names() {
    let (db, users) = open(8, 8).await;
    for index in 0..2 {
        flush_both(&db, users, index).await;
    }
    db.compact_range(users, b"", None).await.expect("compacted");
    let bottom = LEVELS
        .checked_sub(1)
        .and_then(|bottom| u8::try_from(bottom).ok())
        .expect("a bottom level");
    let compacted = levels(&db, users);
    assert!(!compacted.is_empty(), "the users keyspace keeps its keys");
    assert!(
        compacted.iter().all(|level| *level == bottom),
        "{compacted:?}"
    );
    assert_eq!(levels(&db, KeyspaceId::DEFAULT), [0, 0]);
}
