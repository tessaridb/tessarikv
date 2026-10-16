//! Writes wait while frozen in-memory tables wait for their flushes; snapshots read across
//! flushes.

use std::sync::Arc;

use super::Db;
use super::flush_tests::{PATH, key, table_files, until, value};
use crate::log::fault::FaultStorage;
use crate::{
    DbOptions, Durability, Error, KeyspaceId, KeyspaceOptions, ReadOptions, WriteBatch,
    WriteOptions,
};

/// Two frozen tables may wait for flushes; a log segment larger than every test writes, so the
/// log never syncs and only flushes are held by paused syncs.
fn options() -> DbOptions {
    let keyspace = KeyspaceOptions::builder()
        .memtable_bytes(64 * 1024)
        .max_immutable_memtables(2)
        .build()
        .expect("valid");
    DbOptions::builder()
        .default_keyspace(keyspace)
        .wal_segment_bytes(4 * 1024 * 1024)
        .build()
        .expect("valid")
}

/// Lets paused syncs through when dropped, so a failing test still lets its flush finish and the
/// runtime shut down.
struct Resume(FaultStorage);

impl Drop for Resume {
    fn drop(&mut self) {
        self.0.resume_syncs();
    }
}

async fn write(db: &Db, index: u32, no_slowdown: bool) -> crate::Result<()> {
    let mut batch = WriteBatch::new();
    batch
        .put(KeyspaceId::DEFAULT, &key(index), &value(index))
        .expect("small entry");
    let options = WriteOptions {
        durability: Durability::Buffered,
        no_slowdown,
    };
    db.write(&batch, &options).await.map(|_| ())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn writes_wait_while_frozen_tables_wait_for_their_flushes() {
    let storage = FaultStorage::default();
    let db = Arc::new(
        Db::open_with_storage(PATH, options(), Arc::new(storage.clone()))
            .await
            .expect("opened"),
    );
    storage.pause_syncs();
    let resume = Resume(storage.clone());
    let mut written = 0;
    loop {
        match write(&db, written, true).await {
            Ok(()) => written += 1,
            Err(Error::WriteStalled { .. }) => break,
            Err(error) => panic!("unexpected {error:?}"),
        }
        assert!(written < 200, "writes never stalled");
    }
    // The active table is full and two frozen ones wait: memory stays bounded, however long the
    // installer runs on the switch requests already made.
    for _ in 0..1000 {
        tokio::task::yield_now().await;
    }
    assert_eq!(db.view.load().memtables(0).count(), 3);
    let waiting = {
        let db = Arc::clone(&db);
        tokio::spawn(async move { write(&db, written, false).await })
    };
    for _ in 0..1000 {
        tokio::task::yield_now().await;
    }
    assert!(!waiting.is_finished(), "a stalled write went through");
    drop(resume);
    tokio::time::timeout(std::time::Duration::from_secs(10), waiting)
        .await
        .expect("the stalled write resumes")
        .expect("joined")
        .expect("written after the flush");
    until("the frozen tables are flushed", || table_files(&db) >= 2).await;
    for index in 0..=written {
        let read = db
            .get(KeyspaceId::DEFAULT, &key(index), &ReadOptions::default())
            .await
            .expect("readable");
        assert_eq!(read, Some(value(index)), "key {index}");
    }
    Arc::into_inner(db)
        .expect("only handle")
        .close()
        .await
        .expect("closed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_snapshot_reads_the_same_before_and_after_its_versions_are_flushed() {
    let storage = FaultStorage::default();
    let db = Db::open_with_storage(PATH, options(), Arc::new(storage.clone()))
        .await
        .expect("opened");
    for index in 0..8 {
        write(&db, index, false).await.expect("written");
    }
    let snapshot = db.snapshot().expect("registered");
    let mut batch = WriteBatch::new();
    for index in 0..8 {
        batch
            .delete(KeyspaceId::DEFAULT, &key(index))
            .expect("small entry");
    }
    db.write(&batch, &WriteOptions::default())
        .await
        .expect("deleted");
    for index in 8..120 {
        write(&db, index, false).await.expect("written");
    }
    until("the snapshot's versions are flushed", || {
        table_files(&db) >= 1
    })
    .await;
    let view = db.view.load();
    assert!(
        view.files(0)
            .any(|file| file.meta.min_sequence <= snapshot.sequence().get()),
        "the snapshot's versions are in a table file"
    );
    drop(view);
    for index in 0..8 {
        let then = snapshot
            .get(KeyspaceId::DEFAULT, &key(index), &ReadOptions::default())
            .await
            .expect("readable");
        assert_eq!(then, Some(value(index)), "key {index} at the snapshot");
        let now = db
            .get(KeyspaceId::DEFAULT, &key(index), &ReadOptions::default())
            .await
            .expect("readable");
        assert_eq!(now, None, "key {index} now");
    }
    drop(snapshot);
    db.close().await.expect("closed");
}
