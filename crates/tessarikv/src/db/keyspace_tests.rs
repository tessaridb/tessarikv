//! Keyspaces beside the default one: each holds its own keys, every read and write names the
//! one it means, and a batch spanning several commits as one.

use std::path::Path;
use std::sync::Arc;

use super::Db;
use super::flush_tests::PATH;
use crate::log::fault::FaultStorage;
use crate::{
    DbOptions, Durability, KeyspaceId, KeyspaceOptions, ReadOptions, StorageMode, WriteBatch,
    WriteOptions,
};

async fn memory() -> Db {
    let options = DbOptions::builder()
        .storage(StorageMode::Memory)
        .keyspace("users", KeyspaceOptions::default())
        .keyspace("orders", KeyspaceOptions::default())
        .build()
        .expect("valid");
    Db::open("keyspaces", options).await.expect("opened")
}

async fn get(db: &Db, keyspace: KeyspaceId, key: &[u8]) -> Option<Vec<u8>> {
    db.get(keyspace, key, &ReadOptions::default())
        .await
        .expect("read")
}

#[tokio::test]
async fn each_keyspace_holds_its_own_keys() {
    let db = memory().await;
    let users = db.keyspace("users").expect("users");
    let orders = db.keyspace("orders").expect("orders");
    assert_eq!(db.keyspace("default"), Some(KeyspaceId::DEFAULT));
    assert_eq!(db.keyspace("missing"), None);
    let mut batch = WriteBatch::new();
    batch
        .put(KeyspaceId::DEFAULT, b"k", b"default")
        .expect("entry");
    batch.put(users, b"k", b"user").expect("entry");
    batch.put(users, b"m", b"user m").expect("entry");
    batch.put(orders, b"z", b"order").expect("entry");
    db.write(&batch, &WriteOptions::default())
        .await
        .expect("one batch across keyspaces");
    assert_eq!(
        get(&db, KeyspaceId::DEFAULT, b"k").await.as_deref(),
        Some(&b"default"[..])
    );
    assert_eq!(get(&db, users, b"k").await.as_deref(), Some(&b"user"[..]));
    assert_eq!(get(&db, orders, b"k").await, None);
    let page = db
        .scan(users, b"", None, 10, &ReadOptions::default())
        .await
        .expect("scanned");
    let keys: Vec<&[u8]> = page.entries.iter().map(|(key, _)| key.as_slice()).collect();
    assert_eq!(keys, [&b"k"[..], &b"m"[..]]);
    let mut iterator = db.iter(orders, &ReadOptions::default()).expect("iterator");
    iterator.seek_to_first().await.expect("seeked");
    assert!(iterator.valid());
    assert_eq!(iterator.key(), b"z");
    iterator.next().await.expect("moved");
    assert!(!iterator.valid(), "only the orders keyspace's key");
    let values = db
        .multi_get(users, [&b"k"[..], b"z"], &ReadOptions::default())
        .await
        .expect("read");
    assert_eq!(values, [Some(b"user".to_vec()), None]);
}

#[tokio::test]
async fn a_transaction_writes_several_keyspaces_at_once() {
    let db = memory().await;
    let users = db.keyspace("users").expect("users");
    let orders = db.keyspace("orders").expect("orders");
    let mut txn = db.begin().expect("began");
    txn.put(users, b"alice", b"1").expect("put");
    txn.put(orders, b"o1", b"alice").expect("put");
    assert_eq!(
        txn.get(users, b"alice", &ReadOptions::default())
            .await
            .expect("read")
            .as_deref(),
        Some(&b"1"[..])
    );
    txn.commit(&WriteOptions::default())
        .await
        .expect("committed");
    assert_eq!(
        get(&db, orders, b"o1").await.as_deref(),
        Some(&b"alice"[..])
    );
    assert_eq!(get(&db, KeyspaceId::DEFAULT, b"alice").await, None);
}

#[tokio::test]
async fn an_unknown_keyspace_is_refused_everywhere() {
    let db = memory().await;
    let unknown = KeyspaceId::new(9);
    let mut batch = WriteBatch::new();
    batch.put(KeyspaceId::DEFAULT, b"k", b"v").expect("entry");
    batch.put(unknown, b"k", b"v").expect("entry");
    let refused = db.write(&batch, &WriteOptions::default()).await;
    assert_eq!(refused.expect_err("refused").code(), "unknown_keyspace");
    assert_eq!(
        get(&db, KeyspaceId::DEFAULT, b"k").await,
        None,
        "nothing applied"
    );
    let read = db.get(unknown, b"k", &ReadOptions::default()).await;
    assert_eq!(read.expect_err("refused").code(), "unknown_keyspace");
}

fn on_storage() -> DbOptions {
    DbOptions::builder()
        .keyspace("users", KeyspaceOptions::default())
        .create_missing_keyspaces(true)
        .build()
        .expect("valid")
}

async fn open(storage: &FaultStorage) -> Db {
    Db::open_with_storage(PATH, on_storage(), Arc::new(storage.clone()))
        .await
        .expect("opened")
}

/// One batch writing the default keyspace and the users keyspace.
fn both(users: KeyspaceId, durability: Durability) -> (WriteBatch, WriteOptions) {
    let mut batch = WriteBatch::new();
    batch
        .put(KeyspaceId::DEFAULT, b"k", b"default")
        .expect("entry");
    batch.put(users, b"k", b"user").expect("entry");
    let options = WriteOptions {
        durability,
        ..WriteOptions::default()
    };
    (batch, options)
}

/// A flush writes a table for every keyspace holding entries, and reopening reads them back.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn on_storage_every_keyspace_is_flushed_and_read_back() {
    let storage = FaultStorage::default();
    let db = open(&storage).await;
    let users = db.keyspace("users").expect("users");
    let (batch, options) = both(users, Durability::Buffered);
    db.write(&batch, &options).await.expect("written");
    db.flush().await.expect("flushed");
    let view = db.view.load();
    assert_eq!(view.files(0).count(), 1, "the default keyspace's table");
    assert_eq!(
        view.files(users.get()).count(),
        1,
        "the users keyspace's table"
    );
    drop(view);
    drop(db);
    let reopened = open(&storage).await;
    assert_eq!(
        get(&reopened, users, b"k").await.as_deref(),
        Some(&b"user"[..])
    );
    assert_eq!(
        get(&reopened, KeyspaceId::DEFAULT, b"k").await.as_deref(),
        Some(&b"default"[..])
    );
}

/// A synced batch spanning two keyspaces survives a power loss whole; a buffered one is lost whole.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_batch_across_keyspaces_survives_a_crash_whole_or_not_at_all() {
    let storage = FaultStorage::default();
    let db = open(&storage).await;
    let users = db.keyspace("users").expect("users");
    let (batch, options) = both(users, Durability::Sync);
    db.write(&batch, &options).await.expect("written");
    let reopened = open(&storage.crash(Path::new(PATH), true)).await;
    assert_eq!(
        get(&reopened, users, b"k").await.as_deref(),
        Some(&b"user"[..])
    );
    assert_eq!(
        get(&reopened, KeyspaceId::DEFAULT, b"k").await.as_deref(),
        Some(&b"default"[..])
    );

    let storage = FaultStorage::default();
    let db = open(&storage).await;
    let (batch, options) = both(users, Durability::Buffered);
    db.write(&batch, &options).await.expect("written");
    let reopened = open(&storage.crash(Path::new(PATH), true)).await;
    assert_eq!(get(&reopened, users, b"k").await, None);
    assert_eq!(get(&reopened, KeyspaceId::DEFAULT, b"k").await, None);
}
