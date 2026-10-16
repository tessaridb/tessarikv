use std::collections::BTreeMap;

use proptest::prelude::*;

use crate::{Db, DbOptions, KeyspaceId, ReadOptions, WriteBatch, WriteOptions};

const KS: KeyspaceId = KeyspaceId::DEFAULT;

/// What every key holds, by the transaction's own account.
type Model = BTreeMap<Vec<u8>, Vec<u8>>;

async fn memory() -> Db {
    Db::open("transactions", DbOptions::in_memory())
        .await
        .unwrap()
}

async fn put(db: &Db, key: &[u8], value: &[u8]) {
    let mut batch = WriteBatch::new();
    batch.put(KS, key, value).unwrap();
    db.write(&batch, &WriteOptions::default()).await.unwrap();
}

async fn read(db: &Db, key: &[u8]) -> Option<Vec<u8>> {
    db.get(KS, key, &ReadOptions::default()).await.unwrap()
}

#[tokio::test]
async fn a_transaction_reads_its_own_writes_and_nobody_else_does_until_it_commits() {
    let db = memory().await;
    let mut txn = db.begin().unwrap();
    txn.put(KS, b"k", b"v").unwrap();
    let options = ReadOptions::default();
    assert_eq!(
        txn.get(KS, b"k", &options).await.unwrap(),
        Some(b"v".to_vec())
    );
    assert_eq!(read(&db, b"k").await, None);
    txn.commit(&WriteOptions::default()).await.unwrap();
    assert_eq!(read(&db, b"k").await, Some(b"v".to_vec()));
}

#[tokio::test]
async fn a_transaction_reads_the_database_as_it_was_when_it_began() {
    let db = memory().await;
    put(&db, b"k", b"old").await;
    let txn = db.begin().unwrap();
    put(&db, b"k", b"new").await;
    let options = ReadOptions::default();
    assert_eq!(
        txn.get(KS, b"k", &options).await.unwrap(),
        Some(b"old".to_vec())
    );
}

#[tokio::test]
async fn a_transaction_dropped_without_committing_writes_nothing() {
    let db = memory().await;
    {
        let mut txn = db.begin().unwrap();
        txn.put(KS, b"k", b"v").unwrap();
        txn.delete_range(KS, b"a", b"z").unwrap();
    }
    assert_eq!(read(&db, b"k").await, None);
}

#[tokio::test]
async fn an_empty_or_reversed_range_is_refused_when_it_is_deleted() {
    let db = memory().await;
    let mut txn = db.begin().unwrap();
    for (start, end) in [(&b"b"[..], &b"b"[..]), (b"c", b"a")] {
        let refused = txn.delete_range(KS, start, end);
        assert!(
            matches!(
                refused,
                Err(crate::Error::InvalidArgument {
                    what: "range delete",
                    ..
                })
            ),
            "{refused:?}"
        );
    }
    txn.commit(&WriteOptions::default()).await.unwrap();
}

/// One step of a transaction, applied to it and to the model.
#[derive(Debug, Clone)]
enum Step {
    Put(u8, u8),
    Delete(u8),
    DeleteRange(u8, u8),
    Savepoint,
    Rollback,
}

fn step() -> impl Strategy<Value = Step> {
    prop_oneof![
        4 => (0_u8..8, any::<u8>()).prop_map(|(k, v)| Step::Put(k, v)),
        2 => (0_u8..8).prop_map(Step::Delete),
        1 => (0_u8..8, 1_u8..9).prop_map(|(a, width)| Step::DeleteRange(a, a.saturating_add(width).min(9))),
        1 => Just(Step::Savepoint),
        1 => Just(Step::Rollback),
    ]
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
}

fn key(k: u8) -> Vec<u8> {
    vec![b'k', k]
}

/// Every entry of `[start, end)` the transaction sees, read in pages of at most `limit`, each
/// starting where the previous one said the next begins.
async fn scan_all(
    txn: &super::Transaction<'_>,
    start: &[u8],
    end: &[u8],
    limit: usize,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut entries = Vec::new();
    let mut from = start.to_vec();
    loop {
        let page = txn
            .scan(KS, &from, Some(end), limit, &ReadOptions::default())
            .await
            .unwrap();
        assert!(page.entries.len() <= limit);
        entries.extend(page.entries);
        match page.next {
            Some(next) => {
                assert!(next > from, "a page moves forward");
                from = next;
            }
            None => return entries,
        }
    }
}

proptest! {
    #[test]
    fn reads_inside_and_the_state_after_commit_match_a_model(
        before in proptest::collection::vec((0_u8..8, any::<u8>()), 0..8),
        steps in proptest::collection::vec(step(), 0..24),
        span in (0_u8..9, 0_u8..9),
        limit in 1_usize..4,
    ) {
        runtime().block_on(async {
            let db = memory().await;
            let mut model = Model::new();
            for (k, v) in &before {
                put(&db, &key(*k), &[*v]).await;
                model.insert(key(*k), vec![*v]);
            }
            let mut txn = db.begin().unwrap();
            let mut saved: Vec<(Model, super::Savepoint)> = Vec::new();
            for step in &steps {
                match step {
                    Step::Put(k, v) => {
                        txn.put(KS, &key(*k), &[*v]).unwrap();
                        model.insert(key(*k), vec![*v]);
                    }
                    Step::Delete(k) => {
                        txn.delete(KS, &key(*k)).unwrap();
                        model.remove(&key(*k));
                    }
                    Step::DeleteRange(a, b) => {
                        txn.delete_range(KS, &key(*a), &key(*b)).unwrap();
                        model.retain(|k, _| k.as_slice() < key(*a).as_slice() || k.as_slice() >= key(*b).as_slice());
                    }
                    Step::Savepoint => saved.push((model.clone(), txn.savepoint())),
                    Step::Rollback => {
                        if let Some((state, point)) = saved.pop() {
                            txn.rollback_to(point);
                            model = state;
                        }
                    }
                }
                for k in 0_u8..8 {
                    let inside = txn.get(KS, &key(k), &ReadOptions::default()).await.unwrap();
                    assert_eq!(inside, model.get(&key(k)).cloned(), "inside, key {k} after {step:?}");
                }
                let (low, high) = (span.0.min(span.1), span.0.max(span.1));
                let expected: Vec<_> = model
                    .range(key(low)..key(high))
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
                assert_eq!(scan_all(&txn, &key(low), &key(high), limit).await, expected, "scan after {step:?}");
            }
            txn.commit(&WriteOptions::default()).await.unwrap();
            for k in 0_u8..8 {
                assert_eq!(read(&db, &key(k)).await, model.get(&key(k)).cloned(), "after commit, key {k}");
            }
        });
    }
}

#[tokio::test]
async fn a_scan_shows_the_snapshot_and_the_transactions_own_writes_but_not_later_commits() {
    let db = memory().await;
    put(&db, b"a", b"1").await;
    put(&db, b"c", b"3").await;
    let mut txn = db.begin().unwrap();
    put(&db, b"b", b"later").await;
    txn.put(KS, b"d", b"4").unwrap();
    txn.delete(KS, b"a").unwrap();
    let page = txn
        .scan(KS, b"", None, 10, &ReadOptions::default())
        .await
        .unwrap();
    assert_eq!(
        page.entries,
        [
            (b"c".to_vec(), b"3".to_vec()),
            (b"d".to_vec(), b"4".to_vec())
        ]
    );
    assert_eq!(page.next, None);
}

/// A full page names the first key it left out, whether that key is the snapshot's or the
/// transaction's own.
#[tokio::test]
async fn a_full_page_names_the_key_the_next_page_starts_at() {
    let db = memory().await;
    put(&db, b"a", b"1").await;
    put(&db, b"c", b"3").await;
    let mut txn = db.begin().unwrap();
    txn.put(KS, b"b", b"2").unwrap();
    let options = ReadOptions::default();
    let page = txn.scan(KS, b"a", None, 1, &options).await.unwrap();
    assert_eq!(page.entries, [(b"a".to_vec(), b"1".to_vec())]);
    assert_eq!(page.next.as_deref(), Some(&b"b"[..]));
    let page = txn.scan(KS, b"b", None, 1, &options).await.unwrap();
    assert_eq!(page.entries, [(b"b".to_vec(), b"2".to_vec())]);
    assert_eq!(page.next.as_deref(), Some(&b"c"[..]));
}
