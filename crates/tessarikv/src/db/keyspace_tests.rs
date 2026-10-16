//! Keyspaces beside the default one: each holds its own keys, every read and write names the
//! one it means, and a batch spanning several commits as one.

use std::sync::Arc;

use super::Db;
use super::flush_tests::PATH;
use crate::log::fault::FaultStorage;
use crate::{
    DbOptions, KeyspaceId, KeyspaceOptions, ReadOptions, StorageMode, WriteBatch, WriteOptions,
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

/// On storage a keyspace beside the default one is not written yet: its tables are not flushed.
#[tokio::test]
async fn on_storage_writes_beside_the_default_keyspace_are_refused_for_now() {
    let options = DbOptions::builder()
        .keyspace("users", KeyspaceOptions::default())
        .create_missing_keyspaces(true)
        .build()
        .expect("valid");
    let db = Db::open_with_storage(PATH, options, Arc::new(FaultStorage::default()))
        .await
        .expect("opened");
    let users = db.keyspace("users").expect("users");
    let mut batch = WriteBatch::new();
    batch.put(users, b"k", b"v").expect("entry");
    let refused = db.write(&batch, &WriteOptions::default()).await;
    assert_eq!(refused.expect_err("refused").code(), "unsupported");
}
