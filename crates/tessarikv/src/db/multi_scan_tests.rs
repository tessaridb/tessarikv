//! Many range scans at one snapshot.

use std::sync::Arc;

use crate::{
    Db, DbOptions, Isolation, KeyspaceId, ReadOptions, ScanPage, ScanRange, TransactionOptions,
    WriteBatch, WriteOptions,
};

const KS: KeyspaceId = KeyspaceId::DEFAULT;

async fn memory() -> Db {
    Db::open("multi_scan", DbOptions::in_memory())
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

fn range<'a>(start: &'a [u8], end: Option<&'a [u8]>, limit: usize) -> ScanRange<'a> {
    ScanRange { start, end, limit }
}

fn page(entries: &[(&[u8], &[u8])], next: Option<&[u8]>) -> ScanPage {
    ScanPage {
        entries: entries
            .iter()
            .map(|(key, value)| (key.to_vec(), value.to_vec()))
            .collect(),
        next: next.map(<[u8]>::to_vec),
    }
}

/// One page per range, in the order given, each limited and continued on its own; empty,
/// overlapping, repeated and out-of-order ranges are all answered.
#[tokio::test]
async fn every_range_gets_its_own_page_in_the_order_asked() {
    let db = memory().await;
    put(
        &db,
        &[(b"a", b"1"), (b"b", b"2"), (b"c", b"3"), (b"d", b"4")],
    )
    .await;
    let pages = db
        .multi_scan(
            KS,
            [
                range(b"c", None, 10),
                range(b"a", Some(b"d"), 2),
                range(b"b", Some(b"b"), 10),
                range(b"a", Some(b"d"), 2),
                range(b"x", None, 10),
            ],
            &ReadOptions::default(),
        )
        .await
        .expect("scanned");
    assert_eq!(
        pages,
        [
            page(&[(b"c", b"3"), (b"d", b"4")], None),
            page(&[(b"a", b"1"), (b"b", b"2")], Some(b"c")),
            page(&[], None),
            page(&[(b"a", b"1"), (b"b", b"2")], Some(b"c")),
            page(&[], None),
        ]
    );
}

#[tokio::test]
async fn a_snapshot_and_a_transaction_scan_many_ranges_as_they_see_one() {
    let db = memory().await;
    put(&db, &[(b"a", b"1"), (b"m", b"2")]).await;
    let snapshot = db.snapshot().expect("snapshot");
    let mut txn = db.begin().expect("began");
    put(&db, &[(b"a", b"later"), (b"b", b"new")]).await;
    txn.put(KS, b"n", b"own").expect("put");
    let options = ReadOptions::default();
    let ranges = || [range(b"a", Some(b"c"), 10), range(b"m", None, 10)];
    assert_eq!(
        snapshot
            .multi_scan(KS, ranges(), &options)
            .await
            .expect("scanned"),
        [page(&[(b"a", b"1")], None), page(&[(b"m", b"2")], None)]
    );
    assert_eq!(
        txn.multi_scan(KS, ranges(), &options)
            .await
            .expect("scanned"),
        [
            page(&[(b"a", b"1")], None),
            page(&[(b"m", b"2"), (b"n", b"own")], None)
        ]
    );
}

#[tokio::test]
async fn a_keyspace_not_held_is_refused() {
    let db = memory().await;
    let refused = db
        .multi_scan(
            KeyspaceId::new(7),
            [range(b"", None, 1)],
            &ReadOptions::default(),
        )
        .await;
    assert_eq!(
        refused.err().map(|error| error.code()),
        Some("unknown_keyspace")
    );
}

/// A writer keeps a key in each of two ranges equal in every batch; a multi-range scan never
/// sees them differ, because every range is read at one snapshot.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_range_is_read_at_the_same_snapshot() {
    let db = Arc::new(memory().await);
    put(&db, &[(b"a", b"0"), (b"z", b"0")]).await;
    let writer = {
        let db = Arc::clone(&db);
        tokio::spawn(async move {
            for round in 1..=2_000_u32 {
                let value = round.to_string();
                put(&db, &[(b"a", value.as_bytes()), (b"z", value.as_bytes())]).await;
            }
        })
    };
    for _ in 0..2_000 {
        let pages = db
            .multi_scan(
                KS,
                [range(b"a", Some(b"b"), 1), range(b"z", None, 1)],
                &ReadOptions::default(),
            )
            .await
            .expect("scanned");
        assert_eq!(pages[0].entries[0].1, pages[1].entries[0].1);
    }
    writer.await.expect("writer finished");
}

/// A serializable transaction records every range it scanned: a key committed by another
/// writer into any one of them refuses its commit.
#[tokio::test]
async fn a_serializable_transaction_conflicts_on_a_write_into_any_scanned_range() {
    for inserted in [&b"b"[..], b"q"] {
        let options = DbOptions::in_memory()
            .to_builder()
            .serializable_transactions(true)
            .build()
            .expect("valid");
        let db = Db::open("multi_scan", options).await.expect("opened");
        let mut txn = db
            .begin_with(&TransactionOptions {
                isolation: Isolation::Serializable,
            })
            .expect("began");
        txn.multi_scan(
            KS,
            [range(b"a", Some(b"c"), 10), range(b"m", None, 10)],
            &ReadOptions::default(),
        )
        .await
        .expect("scanned");
        put(&db, &[(inserted, b"new")]).await;
        txn.put(KS, b"x", b"own").expect("put");
        let refused = txn.commit(&WriteOptions::default()).await;
        assert_eq!(
            refused.err().map(|error| error.code()),
            Some("conflict"),
            "a write at {inserted:?}"
        );
    }
}
