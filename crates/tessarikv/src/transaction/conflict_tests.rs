//! First committer wins: a commit is refused when a write committed after the transaction began
//! touched a key the transaction writes.

use std::sync::Arc;

use crate::log::fault::FaultStorage;
use crate::{
    Db, DbOptions, Error, KeyspaceId, KeyspaceOptions, ReadOptions, Result, WriteBatch,
    WriteOptions,
};

const KS: KeyspaceId = KeyspaceId::DEFAULT;

async fn memory() -> Db {
    Db::open("conflicts", DbOptions::in_memory())
        .await
        .expect("opened")
}

async fn on_storage(storage: &FaultStorage) -> Db {
    Db::open_with_storage("/db", DbOptions::default(), Arc::new(storage.clone()))
        .await
        .expect("opened")
}

async fn put(db: &Db, key: &[u8], value: &[u8]) {
    let mut batch = WriteBatch::new();
    batch.put(KS, key, value).expect("entry");
    db.write(&batch, &WriteOptions::default())
        .await
        .expect("written");
}

async fn read(db: &Db, key: &[u8]) -> Option<Vec<u8>> {
    db.get(KS, key, &ReadOptions::default())
        .await
        .expect("read")
}

/// The refusal a conflicting commit must give: a retryable conflict, not any failure.
fn assert_conflict<T: std::fmt::Debug>(outcome: Result<T>) {
    let error = outcome.expect_err("the commit conflicts");
    assert_eq!(error.code(), "conflict", "{error}");
    assert!(error.is_retryable());
}

#[tokio::test]
async fn of_two_transactions_updating_one_key_only_the_first_to_commit_succeeds() {
    let db = memory().await;
    put(&db, b"k", b"0").await;
    let mut first = db.begin().expect("began");
    let mut second = db.begin().expect("began");
    first.put(KS, b"k", b"first").expect("put");
    second.put(KS, b"k", b"second").expect("put");
    first
        .commit(&WriteOptions::default())
        .await
        .expect("first commits");
    assert_conflict(second.commit(&WriteOptions::default()).await);
    assert_eq!(read(&db, b"k").await, Some(b"first".to_vec()));
}

#[tokio::test]
async fn a_plain_write_after_the_snapshot_refuses_the_transaction() {
    let db = memory().await;
    let mut txn = db.begin().expect("began");
    put(&db, b"k", b"plain").await;
    txn.delete(KS, b"k").expect("delete");
    assert_conflict(txn.commit(&WriteOptions::default()).await);
    assert_eq!(read(&db, b"k").await, Some(b"plain".to_vec()));
}

#[tokio::test]
async fn a_write_committed_before_the_transaction_began_does_not_conflict() {
    let db = memory().await;
    put(&db, b"k", b"before").await;
    let mut txn = db.begin().expect("began");
    txn.put(KS, b"k", b"txn").expect("put");
    txn.commit(&WriteOptions::default()).await.expect("commits");
    assert_eq!(read(&db, b"k").await, Some(b"txn".to_vec()));
}

#[tokio::test]
async fn transactions_writing_different_keys_both_commit() {
    let db = memory().await;
    let mut first = db.begin().expect("began");
    let mut second = db.begin().expect("began");
    first.put(KS, b"a", b"1").expect("put");
    second.put(KS, b"b", b"2").expect("put");
    first
        .commit(&WriteOptions::default())
        .await
        .expect("first commits");
    second
        .commit(&WriteOptions::default())
        .await
        .expect("second commits");
    assert_eq!(read(&db, b"a").await, Some(b"1".to_vec()));
    assert_eq!(read(&db, b"b").await, Some(b"2".to_vec()));
}

/// A range delete committed after the snapshot refuses a transaction that writes a key in its
/// range of its keyspace, and no other; a transaction's own range delete is refused after any
/// later commit (see the serializable tests for a database that records key prefixes).
#[tokio::test]
async fn range_deletes_after_the_snapshot_refuse_the_keys_they_cover() {
    let options = DbOptions::in_memory()
        .to_builder()
        .keyspace("other", KeyspaceOptions::default())
        .build()
        .expect("valid");
    let db = Db::open("conflicts", options).await.expect("opened");
    let other = db.keyspace("other").expect("held");
    let mut inside = db.begin().expect("began");
    let mut outside = db.begin().expect("began");
    let mut elsewhere = db.begin().expect("began");
    let mut batch = WriteBatch::new();
    batch.delete_range(KS, b"x", b"y").expect("range");
    db.write(&batch, &WriteOptions::default())
        .await
        .expect("written");
    inside.put(KS, b"xa", b"1").expect("put");
    assert_conflict(inside.commit(&WriteOptions::default()).await);
    outside.put(KS, b"a", b"1").expect("put");
    outside
        .commit(&WriteOptions::default())
        .await
        .expect("a key outside the range commits");
    elsewhere.put(other, b"xa", b"1").expect("put");
    elsewhere
        .commit(&WriteOptions::default())
        .await
        .expect("another keyspace commits");

    let mut ranged = db.begin().expect("began");
    put(&db, b"z", b"later").await;
    ranged.delete_range(KS, b"a", b"b").expect("range");
    assert_conflict(ranged.commit(&WriteOptions::default()).await);
}

/// A refused commit leaves nothing behind: not in memory, not in the log, and the writes after
/// it still commit and survive a reopen.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refused_commit_leaves_nothing_and_later_writes_survive_a_reopen() {
    let storage = FaultStorage::default();
    let db = on_storage(&storage).await;
    let mut txn = db.begin().expect("began");
    put(&db, b"k", b"winner").await;
    txn.put(KS, b"k", b"loser").expect("put");
    txn.put(KS, b"only-loser", b"x").expect("put");
    assert_conflict(txn.commit(&WriteOptions::default()).await);
    put(&db, b"after", b"kept").await;
    assert_eq!(read(&db, b"only-loser").await, None);
    db.close().await.expect("closed");

    let db = on_storage(&storage).await;
    assert_eq!(read(&db, b"k").await, Some(b"winner".to_vec()));
    assert_eq!(read(&db, b"only-loser").await, None);
    assert_eq!(read(&db, b"after").await, Some(b"kept".to_vec()));
}

/// Many tasks increment one counter through transactions, retrying refused commits: no
/// increment is lost, so the counter ends at the number of commits.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_increments_through_transactions_lose_nothing() {
    const TASKS: u64 = 8;
    const INCREMENTS: u64 = 40;
    let db = Arc::new(memory().await);
    put(&db, b"counter", &0_u64.to_le_bytes()).await;
    let tasks: Vec<_> = (0..TASKS)
        .map(|_| {
            let db = Arc::clone(&db);
            tokio::spawn(async move {
                for _ in 0..INCREMENTS {
                    while let Err(error) = increment(&db).await {
                        assert_eq!(error.code(), "conflict", "{error}");
                        tokio::task::yield_now().await;
                    }
                }
            })
        })
        .collect();
    for task in tasks {
        task.await.expect("task finished");
    }
    let total = read(&db, b"counter").await.expect("counter");
    let total = u64::from_le_bytes(total.try_into().expect("eight bytes"));
    assert_eq!(total, TASKS * INCREMENTS);
}

async fn increment(db: &Db) -> std::result::Result<(), Error> {
    let mut txn = db.begin()?;
    let current = txn
        .get(KS, b"counter", &ReadOptions::default())
        .await?
        .expect("counter");
    let current = u64::from_le_bytes(current.try_into().expect("eight bytes"));
    txn.put(KS, b"counter", &(current + 1).to_le_bytes())?;
    txn.commit(&WriteOptions::default()).await.map(|_| ())
}
