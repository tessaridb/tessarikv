//! Many point reads at one snapshot.

use std::sync::Arc;

use crate::{Db, DbOptions, KeyspaceId, ReadOptions, WriteBatch, WriteOptions};

const KS: KeyspaceId = KeyspaceId::DEFAULT;

async fn memory() -> Db {
    Db::open("multi_get", DbOptions::in_memory())
        .await
        .expect("opened")
}

async fn put(db: &Db, entries: &[(&[u8], &[u8])]) {
    let mut batch = WriteBatch::new();
    for (key, value) in entries {
        batch.put(KS, key, value).expect("entry");
    }
    db.write(&batch, &WriteOptions::default())
        .await
        .expect("written");
}

#[tokio::test]
async fn values_come_back_in_the_order_asked_with_missing_keys_as_none() {
    let db = memory().await;
    put(&db, &[(b"a", b"1"), (b"c", b"3")]).await;
    let values = db
        .multi_get(KS, [&b"c"[..], b"b", b"a", b"c"], &ReadOptions::default())
        .await
        .expect("read");
    assert_eq!(
        values,
        [
            Some(b"3".to_vec()),
            None,
            Some(b"1".to_vec()),
            Some(b"3".to_vec())
        ]
    );
}

#[tokio::test]
async fn a_snapshot_and_a_transaction_read_many_keys_as_they_see_one() {
    let db = memory().await;
    put(&db, &[(b"a", b"1"), (b"b", b"2")]).await;
    let snapshot = db.snapshot().expect("snapshot");
    let mut txn = db.begin().expect("began");
    put(&db, &[(b"a", b"later")]).await;
    txn.put(KS, b"b", b"own").expect("put");
    let options = ReadOptions::default();
    assert_eq!(
        snapshot
            .multi_get(KS, [&b"a"[..], b"b"], &options)
            .await
            .expect("read"),
        [Some(b"1".to_vec()), Some(b"2".to_vec())]
    );
    assert_eq!(
        txn.multi_get(KS, [&b"a"[..], b"b"], &options)
            .await
            .expect("read"),
        [Some(b"1".to_vec()), Some(b"own".to_vec())]
    );
}

/// A writer keeps two keys equal in every batch; a multi-key read never sees them differ,
/// because all its keys are read at one snapshot.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_key_is_read_at_the_same_snapshot() {
    let db = Arc::new(memory().await);
    put(&db, &[(b"x", b"0"), (b"y", b"0")]).await;
    let writer = {
        let db = Arc::clone(&db);
        tokio::spawn(async move {
            for round in 1..=2_000_u32 {
                let value = round.to_string();
                put(&db, &[(b"x", value.as_bytes()), (b"y", value.as_bytes())]).await;
            }
        })
    };
    for _ in 0..2_000 {
        let values = db
            .multi_get(KS, [&b"x"[..], b"y"], &ReadOptions::default())
            .await
            .expect("read");
        assert_eq!(values[0], values[1]);
    }
    writer.await.expect("writer finished");
}
