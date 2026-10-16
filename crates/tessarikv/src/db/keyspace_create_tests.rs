//! Keyspaces created while the database runs: written, flushed and read back after reopening,
//! in memory as on storage; names already taken are refused.

use std::sync::Arc;

use super::Db;
use super::flush_tests::PATH;
use crate::log::fault::FaultStorage;
use crate::{DbOptions, KeyspaceId, KeyspaceOptions, ReadOptions, WriteBatch, WriteOptions};

async fn put(db: &Db, keyspace: KeyspaceId, key: &[u8], value: &[u8]) {
    let mut batch = WriteBatch::new();
    batch.put(keyspace, key, value).expect("entry");
    db.write(&batch, &WriteOptions::default())
        .await
        .expect("written");
}

async fn get(db: &Db, keyspace: KeyspaceId, key: &[u8]) -> Option<Vec<u8>> {
    db.get(keyspace, key, &ReadOptions::default())
        .await
        .expect("read")
}

fn names(db: &Db) -> Vec<String> {
    db.keyspaces().into_iter().map(|(_, name)| name).collect()
}

/// A keyspace created on storage takes writes at once, is flushed like any other, and is read
/// back after reopening with options naming it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_keyspace_created_on_storage_is_written_flushed_and_reopened() {
    let storage = FaultStorage::default();
    let open = |options| Db::open_with_storage(PATH, options, Arc::new(storage.clone()));
    let db = open(DbOptions::default()).await.expect("opened");
    let users = db
        .create_keyspace("users", KeyspaceOptions::default())
        .await
        .expect("created");
    assert_ne!(users, KeyspaceId::DEFAULT);
    assert_eq!(db.keyspace("users"), Some(users));
    assert_eq!(names(&db), ["default", "users"]);
    put(&db, users, b"k", b"user").await;
    assert_eq!(get(&db, users, b"k").await.as_deref(), Some(&b"user"[..]));
    assert_eq!(get(&db, KeyspaceId::DEFAULT, b"k").await, None);
    db.flush().await.expect("flushed");
    assert_eq!(db.view.load().files(users.get()).count(), 1);
    db.close().await.expect("closed");

    let refused = open(DbOptions::default()).await;
    assert_eq!(
        refused.err().map(|error| error.code()),
        Some("invalid_argument"),
        "the options must name every keyspace held"
    );
    let named = DbOptions::builder()
        .keyspace("users", KeyspaceOptions::default())
        .build()
        .expect("valid");
    let db = open(named).await.expect("reopened");
    assert_eq!(db.keyspace("users"), Some(users), "ids are kept");
    assert_eq!(get(&db, users, b"k").await.as_deref(), Some(&b"user"[..]));
    let orders = db
        .create_keyspace("orders", KeyspaceOptions::default())
        .await
        .expect("created");
    assert!(orders.get() > users.get(), "ids are never reused");
}

/// In memory a created keyspace holds its own keys.
#[tokio::test]
async fn a_keyspace_created_in_memory_holds_its_own_keys() {
    let db = Db::open("memory", DbOptions::in_memory())
        .await
        .expect("opened");
    let users = db
        .create_keyspace("users", KeyspaceOptions::default())
        .await
        .expect("created");
    put(&db, users, b"k", b"user").await;
    put(&db, KeyspaceId::DEFAULT, b"k", b"default").await;
    assert_eq!(get(&db, users, b"k").await.as_deref(), Some(&b"user"[..]));
    let orders = db
        .create_keyspace("orders", KeyspaceOptions::default())
        .await
        .expect("created");
    assert!(orders.get() > users.get(), "ids are never reused");
    assert_eq!(get(&db, orders, b"k").await, None);
    assert_eq!(names(&db), ["default", "users", "orders"]);
}

/// A name already held, the default keyspace's, or no name at all is refused, and nothing is
/// created.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn taken_or_empty_names_are_refused() {
    for options in [
        DbOptions::in_memory(),
        DbOptions::builder()
            .keyspace("users", KeyspaceOptions::default())
            .create_missing_keyspaces(true)
            .build()
            .expect("valid"),
    ] {
        let in_memory = options.storage() == crate::StorageMode::Memory;
        let db = if in_memory {
            Db::open("memory", options).await
        } else {
            Db::open_with_storage(PATH, options, Arc::new(FaultStorage::default())).await
        }
        .expect("opened");
        if in_memory {
            db.create_keyspace("users", KeyspaceOptions::default())
                .await
                .expect("created");
        }
        for name in ["users", "default", ""] {
            let refused = db.create_keyspace(name, KeyspaceOptions::default()).await;
            assert_eq!(
                refused.err().map(|error| error.code()),
                Some("invalid_argument"),
                "{name:?}"
            );
        }
        assert_eq!(names(&db), ["default", "users"]);
    }
}
