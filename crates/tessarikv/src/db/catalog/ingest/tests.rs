use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use crate::log::fault::FaultStorage;
use crate::{Db, DbOptions, Error, KeyspaceId, ReadOptions, TableWriter, WriteBatch, WriteOptions};

const PATH: &str = "/db";

async fn open(storage: &FaultStorage) -> Db {
    Db::open_with_storage(PATH, DbOptions::default(), Arc::new(storage.clone()))
        .await
        .expect("opened")
}

async fn put(db: &Db, key: &[u8], value: &[u8]) -> u64 {
    let mut batch = WriteBatch::new();
    batch.put(KeyspaceId::DEFAULT, key, value).expect("entry");
    db.write(&batch, &WriteOptions::default())
        .await
        .expect("written")
        .get()
}

async fn read(db: &Db, key: &[u8]) -> Option<Vec<u8>> {
    db.get(KeyspaceId::DEFAULT, key, &ReadOptions::default())
        .await
        .expect("read")
}

/// k1 and k3 put, k2 deleted.
fn table() -> Vec<u8> {
    let mut table = TableWriter::new();
    table.put(b"k1", b"ingested").expect("entry");
    table.delete(b"k2").expect("entry");
    table.put(b"k3", b"ingested").expect("entry");
    table.finish().expect("finished")
}

async fn check_after(db: &Db) {
    assert_eq!(read(db, b"k1").await.as_deref(), Some(&b"ingested"[..]));
    assert_eq!(read(db, b"k2").await, None);
    assert_eq!(read(db, b"k3").await.as_deref(), Some(&b"later"[..]));
    assert_eq!(read(db, b"k4").await.as_deref(), Some(&b"ingested"[..]));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_ingest_lands_between_the_writes_before_and_after_it() {
    let storage = FaultStorage::default();
    let db = open(&storage).await;
    put(&db, b"k1", b"old").await;
    put(&db, b"k2", b"old").await;
    db.flush().await.expect("flushed");
    put(&db, b"k3", b"old").await;
    let before = db.snapshot().expect("snapshot");
    let mut second = TableWriter::new();
    second.put(b"k4", b"ingested").expect("entry");
    let sequence = db
        .ingest(
            KeyspaceId::DEFAULT,
            vec![table(), second.finish().expect("finished")],
        )
        .await
        .expect("ingested");
    assert!(put(&db, b"k3", b"later").await > sequence.get());
    check_after(&db).await;
    let old = |key: &'static [u8]| {
        let before = &before;
        async move {
            before
                .get(KeyspaceId::DEFAULT, key, &ReadOptions::default())
                .await
                .expect("read")
        }
    };
    assert_eq!(old(b"k1").await.as_deref(), Some(&b"old"[..]));
    assert_eq!(old(b"k2").await.as_deref(), Some(&b"old"[..]));
    assert_eq!(old(b"k4").await, None);
    drop(before);
    db.flush().await.expect("flushed");
    db.compact_range(KeyspaceId::DEFAULT, b"", None)
        .await
        .expect("compacted");
    check_after(&db).await;
    db.close().await.expect("closed");
    let reopened = open(&storage).await;
    check_after(&reopened).await;
    // The ingest's sequence is never handed out again.
    assert!(put(&reopened, b"k5", b"after").await > sequence.get());
}

/// A transaction spanning an ingest neither waits for it nor is refused by it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_transaction_spanning_an_ingest_commits() {
    let storage = FaultStorage::default();
    let db = open(&storage).await;
    let mut transaction = db.begin().expect("begun");
    transaction
        .put(KeyspaceId::DEFAULT, b"t", b"value")
        .expect("put");
    db.ingest(KeyspaceId::DEFAULT, vec![table()])
        .await
        .expect("ingested");
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        transaction.commit(&WriteOptions::default()),
    )
    .await
    .expect("the commit did not wait for the ingest")
    .expect("committed");
    assert_eq!(read(&db, b"t").await.as_deref(), Some(&b"value"[..]));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_ingest_the_database_cannot_take_is_refused_by_name() {
    let storage = FaultStorage::default();
    let db = open(&storage).await;
    let refused = |result: crate::Result<_>| match result {
        Err(Error::InvalidArgument { reason, .. }) => reason,
        other => panic!("expected a refusal, got {other:?}"),
    };
    let ingest = |tables: Vec<Vec<u8>>| db.ingest(KeyspaceId::DEFAULT, tables);
    assert_eq!(refused(ingest(vec![]).await), "an ingest names no table");
    assert_eq!(
        refused(ingest(vec![table(), table()]).await),
        "ingested tables overlap"
    );
    assert_eq!(
        refused(ingest(vec![b"not a table".to_vec()]).await),
        "an ingested table does not decode"
    );
    let mut flipped = table();
    flipped[3] ^= 1;
    assert_eq!(
        refused(ingest(vec![flipped]).await),
        "an ingested table does not decode"
    );
    assert!(matches!(
        db.ingest(KeyspaceId::new(9), vec![table()]).await,
        Err(Error::UnknownKeyspace { keyspace: 9 })
    ));
    assert_eq!(
        read(&db, b"k1").await,
        None,
        "a refused ingest wrote nothing"
    );
    db.close().await.expect("closed");

    let reader = Db::open_read_only_with_storage(PATH, DbOptions::default(), Arc::new(storage))
        .await
        .expect("opened read-only");
    assert!(matches!(
        reader.ingest(KeyspaceId::DEFAULT, vec![table()]).await,
        Err(Error::ReadOnly { what: "an ingest" })
    ));
    let memory = Db::open("memory", DbOptions::in_memory())
        .await
        .expect("opened");
    assert!(matches!(
        memory.ingest(KeyspaceId::DEFAULT, vec![table()]).await,
        Err(Error::Unsupported {
            what: "an ingest into a database in memory"
        })
    ));
}

/// Writers race an ingest of the keys they write: each key reads the newer of its write and
/// the ingest, by the sequence each returned.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn writes_racing_an_ingest_are_older_or_newer_never_lost() {
    let storage = FaultStorage::default();
    let db = Arc::new(open(&storage).await);
    let keys: Vec<Vec<u8>> = (0..200_u32)
        .map(|n| format!("key-{n:04}").into_bytes())
        .collect();
    let mut table = TableWriter::new();
    for key in &keys {
        table.put(key, b"ingested").expect("entry");
    }
    let table = table.finish().expect("finished");
    let writers: Vec<_> = (0..4_usize)
        .map(|writer| {
            let (db, keys) = (Arc::clone(&db), keys.clone());
            tokio::spawn(async move {
                let mut written = Vec::new();
                for key in keys.iter().skip(writer).step_by(4) {
                    written.push((key.clone(), put(&db, key, b"written").await));
                }
                written
            })
        })
        .collect();
    let sequence = db
        .ingest(KeyspaceId::DEFAULT, vec![table])
        .await
        .expect("ingested")
        .get();
    for writer in writers {
        for (key, written) in writer.await.expect("writer") {
            let expected: &[u8] = if written > sequence {
                b"written"
            } else {
                b"ingested"
            };
            assert_eq!(read(&db, &key).await.as_deref(), Some(expected));
        }
    }
}

/// A crash at any step of an ingest leaves all of it or none of it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_crash_during_an_ingest_leaves_all_of_it_or_none() {
    let storage = FaultStorage::default();
    let db = open(&storage).await;
    put(&db, b"k1", b"old").await;
    storage.capture_steps(Path::new(PATH), Arc::new(AtomicU64::new(0)));
    let mut second = TableWriter::new();
    second.put(b"k4", b"ingested").expect("entry");
    db.ingest(
        KeyspaceId::DEFAULT,
        vec![table(), second.finish().expect("finished")],
    )
    .await
    .expect("ingested");
    let images = storage.take_images();
    let (mut whole, mut none) = (0, 0);
    for image in images {
        let reopened = open(&image.storage).await;
        let state = (read(&reopened, b"k1").await, read(&reopened, b"k4").await);
        match state {
            (Some(k1), Some(k4)) if k1 == b"ingested" && k4 == b"ingested" => whole += 1,
            (Some(k1), None) if k1 == b"old" => none += 1,
            other => panic!("{}: a partial ingest {other:?}", image.step),
        }
        reopened.close().await.expect("closed");
    }
    assert!(whole > 0 && none > 0, "{whole} whole, {none} none");
}
