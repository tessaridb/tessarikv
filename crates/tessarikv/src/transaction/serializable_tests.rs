//! Serializable transactions: what they read must still be current when they commit.

use std::sync::Arc;

use crate::{
    Db, DbOptions, Isolation, KeyspaceId, ReadOptions, Result, Transaction, TransactionOptions,
    WriteBatch, WriteOptions,
};

const KS: KeyspaceId = KeyspaceId::DEFAULT;

async fn memory(serializable: bool) -> Db {
    let options = DbOptions::in_memory()
        .to_builder()
        .serializable_transactions(serializable)
        .build()
        .expect("valid");
    Db::open("serializable", options).await.expect("opened")
}

fn begin(db: &Db, isolation: Isolation) -> Transaction<'_> {
    db.begin_with(&TransactionOptions { isolation })
        .expect("began")
}

async fn put(db: &Db, key: &[u8], value: &[u8]) {
    let mut batch = WriteBatch::new();
    batch.put(KS, key, value).expect("entry");
    db.write(&batch, &WriteOptions::default())
        .await
        .expect("written");
}

async fn read(txn: &mut Transaction<'_>, key: &[u8]) -> Option<Vec<u8>> {
    txn.get(KS, key, &ReadOptions::default())
        .await
        .expect("read")
}

fn assert_conflict<T: std::fmt::Debug>(outcome: Result<T>) {
    let error = outcome.expect_err("the commit conflicts");
    assert_eq!(error.code(), "conflict", "{error}");
}

/// Two doctors on call; each transaction checks both are on call and takes one off. Under
/// snapshot isolation both commit and nobody is on call; serializable refuses the second.
async fn doctors(isolation: Isolation) -> (Result<()>, Result<()>) {
    let db = memory(true).await;
    put(&db, b"alice", b"on").await;
    put(&db, b"bob", b"on").await;
    let mut first = begin(&db, isolation);
    let mut second = begin(&db, isolation);
    for txn in [&mut first, &mut second] {
        assert_eq!(read(txn, b"alice").await.as_deref(), Some(&b"on"[..]));
        assert_eq!(read(txn, b"bob").await.as_deref(), Some(&b"on"[..]));
    }
    first.put(KS, b"alice", b"off").expect("put");
    second.put(KS, b"bob", b"off").expect("put");
    let options = WriteOptions::default();
    (
        first.commit(&options).await.map(|_| ()),
        second.commit(&options).await.map(|_| ()),
    )
}

#[tokio::test]
async fn write_skew_is_refused_when_serializable_and_admitted_under_snapshot_isolation() {
    let (first, second) = doctors(Isolation::Serializable).await;
    first.expect("the first commits");
    assert_conflict(second);
    let (first, second) = doctors(Isolation::Snapshot).await;
    first.expect("the first commits");
    second.expect("snapshot isolation admits write skew");
}

#[tokio::test]
async fn an_insert_into_a_scanned_range_refuses_the_scanning_transaction() {
    let db = memory(true).await;
    put(&db, b"order:1", b"10").await;
    let mut txn = begin(&db, Isolation::Serializable);
    let page = txn
        .scan(KS, b"order:", Some(b"order;"), 100, &ReadOptions::default())
        .await
        .expect("scanned");
    assert_eq!(page.entries.len(), 1);
    put(&db, b"order:2", b"20").await;
    txn.put(KS, b"total", b"10").expect("put");
    assert_conflict(txn.commit(&WriteOptions::default()).await);
}

#[tokio::test]
async fn writes_outside_what_was_read_do_not_refuse() {
    let db = memory(true).await;
    put(&db, b"order:1", b"10").await;
    let mut txn = begin(&db, Isolation::Serializable);
    txn.scan(KS, b"order:", Some(b"order;"), 100, &ReadOptions::default())
        .await
        .expect("scanned");
    read(&mut txn, b"limit").await;
    put(&db, b"invoice:7", b"x").await;
    put(&db, b"limits", b"x").await;
    txn.put(KS, b"total", b"10").expect("put");
    txn.commit(&WriteOptions::default())
        .await
        .expect("nothing it read changed");
}

#[tokio::test]
async fn a_key_read_and_then_updated_or_range_deleted_refuses_the_reader() {
    let db = memory(true).await;
    put(&db, b"price", b"5").await;
    let mut updated = begin(&db, Isolation::Serializable);
    read(&mut updated, b"price").await;
    let mut deleted = begin(&db, Isolation::Serializable);
    read(&mut deleted, b"price").await;
    put(&db, b"price", b"6").await;
    updated.put(KS, b"quote", b"5").expect("put");
    assert_conflict(updated.commit(&WriteOptions::default()).await);

    let mut later = begin(&db, Isolation::Serializable);
    read(&mut later, b"price").await;
    let mut batch = WriteBatch::new();
    batch.delete_range(KS, b"pa", b"pz").expect("range");
    db.write(&batch, &WriteOptions::default())
        .await
        .expect("written");
    later.put(KS, b"quote", b"6").expect("put");
    assert_conflict(later.commit(&WriteOptions::default()).await);
    drop(deleted);
}

#[tokio::test]
async fn a_serializable_transaction_that_writes_nothing_always_commits() {
    let db = memory(true).await;
    put(&db, b"k", b"1").await;
    let mut txn = begin(&db, Isolation::Serializable);
    read(&mut txn, b"k").await;
    put(&db, b"k", b"2").await;
    txn.commit(&WriteOptions::default())
        .await
        .expect("a reader is serialized at its snapshot");
}

#[tokio::test]
async fn serializable_needs_the_database_switch() {
    let db = memory(false).await;
    let options = TransactionOptions {
        isolation: Isolation::Serializable,
    };
    let error = db.begin_with(&options).map(|_| ()).expect_err("refused");
    assert_eq!(error.code(), "invalid_argument");
}

/// Many tasks run the doctors' rule with retries: whatever the interleaving, one doctor stays
/// on call.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_doctors_never_leave_nobody_on_call() {
    for _ in 0..20 {
        let db = Arc::new(memory(true).await);
        let names: [&[u8]; 4] = [b"a", b"b", b"c", b"d"];
        for name in names {
            put(&db, name, b"on").await;
        }
        let tasks: Vec<_> = names
            .into_iter()
            .map(|name| {
                let db = Arc::clone(&db);
                tokio::spawn(async move {
                    while let Err(error) = go_off_call(&db, name, names).await {
                        assert_eq!(error.code(), "conflict", "{error}");
                        tokio::task::yield_now().await;
                    }
                })
            })
            .collect();
        for task in tasks {
            task.await.expect("task finished");
        }
        let on_call = count_on_call(&db, names).await;
        assert_eq!(on_call, 1, "exactly the last doctor stays on call");
    }
}

/// Takes `name` off call if another doctor is still on call.
async fn go_off_call(db: &Db, name: &[u8], names: [&[u8]; 4]) -> Result<()> {
    let mut txn = begin(db, Isolation::Serializable);
    let mut others = 0;
    for other in names {
        let state = txn.get(KS, other, &ReadOptions::default()).await?;
        if other != name && state.as_deref() == Some(&b"on"[..]) {
            others += 1;
        }
    }
    if others > 0 {
        txn.put(KS, name, b"off")?;
    }
    txn.commit(&WriteOptions::default()).await.map(|_| ())
}

async fn count_on_call(db: &Db, names: [&[u8]; 4]) -> usize {
    let mut on_call = 0;
    for name in names {
        let state = db
            .get(KS, name, &ReadOptions::default())
            .await
            .expect("read");
        if state.as_deref() == Some(&b"on"[..]) {
            on_call += 1;
        }
    }
    on_call
}
