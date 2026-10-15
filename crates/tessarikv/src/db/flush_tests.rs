//! Flushing full in-memory tables to table files, and reopening from them.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use super::Db;
use crate::log::fault::FaultStorage;
use crate::log::segment_path;
use crate::storage::Storage as _;
use crate::{
    DbOptions, Durability, Error, KeyspaceId, KeyspaceOptions, ReadOptions, WriteBatch,
    WriteOptions,
};

pub(super) const PATH: &str = "/db";
const WRITES: u32 = 400;

fn options() -> DbOptions {
    let keyspace = KeyspaceOptions::builder()
        .memtable_bytes(64 * 1024)
        .build()
        .expect("valid");
    DbOptions::builder()
        .default_keyspace(keyspace)
        .wal_segment_bytes(1024 * 1024)
        .build()
        .expect("valid")
}

async fn open(storage: &FaultStorage) -> Db {
    Db::open_with_storage(PATH, options(), Arc::new(storage.clone()))
        .await
        .expect("opened")
}

pub(super) fn key(index: u32) -> Vec<u8> {
    format!("key{index:05}").into_bytes()
}

/// About four kilobytes, different for every index.
pub(super) fn value(index: u32) -> Vec<u8> {
    format!("value of {index};").repeat(256).into_bytes()
}

async fn write(db: &Db, index: u32, durability: Durability) -> crate::Result<()> {
    let mut batch = WriteBatch::new();
    batch
        .put(KeyspaceId::DEFAULT, &key(index), &value(index))
        .expect("small entry");
    let options = WriteOptions {
        durability,
        ..WriteOptions::default()
    };
    db.write(&batch, &options).await.map(|_| ())
}

async fn write_all(db: &Db, durability: Durability) {
    for index in 0..WRITES {
        write(db, index, durability).await.expect("written");
    }
}

async fn assert_all_readable(db: &Db) {
    for index in 0..WRITES {
        let read = db
            .get(KeyspaceId::DEFAULT, &key(index), &ReadOptions::default())
            .await
            .expect("readable");
        assert_eq!(read, Some(value(index)), "key {index}");
    }
}

/// Returns once `done` holds, failing the test instead of hanging.
pub(super) async fn until(what: &str, done: impl Fn() -> bool) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while !done() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{what}"));
}

pub(super) fn table_files(db: &Db) -> usize {
    db.view.load().files(0).count()
}

fn files_on_storage(storage: &FaultStorage, suffix: &str) -> usize {
    storage
        .list(Path::new(PATH))
        .expect("listable")
        .iter()
        .filter(|name| name.ends_with(suffix))
        .count()
}

/// The highest sequence the table files of `db` hold.
fn flushed(db: &Db) -> u64 {
    let view = db.view.load();
    view.files(0)
        .map(|file| file.meta.max_sequence)
        .max()
        .unwrap_or(0)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn full_tables_are_flushed_and_the_log_they_cover_is_removed() {
    let storage = FaultStorage::default();
    let db = open(&storage).await;
    write_all(&db, Durability::Buffered).await;
    until("two tables are flushed", || table_files(&db) >= 2).await;
    let first_segment = segment_path(Path::new(PATH), 1);
    until("the first log segment is removed", || {
        !storage.exists(&first_segment).expect("exists")
    })
    .await;
    assert_all_readable(&db).await;
    assert_eq!(files_on_storage(&storage, ".sst"), table_files(&db));
    db.close().await.expect("closed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reopening_reads_the_tables_and_replays_only_the_log_above_them() {
    let storage = FaultStorage::default();
    let db = open(&storage).await;
    write_all(&db, Durability::Buffered).await;
    until("two tables are flushed", || table_files(&db) >= 2).await;
    db.close().await.expect("closed");
    // A table file a crash left between writing it and naming it in the manifest.
    let orphan = Path::new(PATH).join(format!("{:020}.sst", 900));
    let mut file = storage.create(&orphan).expect("created");
    file.append(b"never named").expect("written");
    drop(file);

    let db = open(&storage).await;
    assert!(
        !storage.exists(&orphan).expect("exists"),
        "the orphan is removed"
    );
    assert_all_readable(&db).await;
    let flushed = flushed(&db);
    assert!(flushed > 0);
    let view = db.view.load();
    let newest = view.memtables().next().expect("a table");
    let mut versions = newest.iter();
    versions.seek_to_first();
    while let Some(version) = versions.entry() {
        assert!(
            version.sequence.get() > flushed,
            "a flushed batch was replayed again"
        );
        versions.next();
    }
    drop(view);
    db.close().await.expect("closed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn synced_writes_survive_a_power_loss_after_their_log_was_released() {
    let storage = FaultStorage::default();
    let db = open(&storage).await;
    write_all(&db, Durability::Sync).await;
    let first_segment = segment_path(Path::new(PATH), 1);
    until("the first log segment is removed", || {
        !storage.exists(&first_segment).expect("exists")
    })
    .await;
    let power_loss = storage.crash(Path::new(PATH), true);
    db.close().await.expect("closed");
    let db = open(&power_loss).await;
    assert_all_readable(&db).await;
    db.close().await.expect("closed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_flush_stops_writes() {
    let storage = FaultStorage::default();
    let db = open(&storage).await;
    storage.fail_syncs();
    let refused = tokio::time::timeout(Duration::from_secs(10), async {
        for index in 0.. {
            match write(&db, index, Durability::Buffered).await {
                Ok(()) => tokio::task::yield_now().await,
                Err(error) => return error,
            }
        }
        unreachable!("the loop only ends by returning")
    })
    .await
    .expect("a write is refused");
    assert!(matches!(refused, Error::FlushFailed), "{refused:?}");
    assert!(db.close().await.is_err(), "close reports the failed flush");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sequence_numbers_continue_after_the_tables_when_no_log_is_left() {
    let storage = FaultStorage::default();
    let db = open(&storage).await;
    write_all(&db, Durability::Buffered).await;
    until("two tables are flushed", || table_files(&db) >= 2).await;
    let flushed = flushed(&db);
    db.close().await.expect("closed");
    for name in storage.list(Path::new(PATH)).expect("listable") {
        if name.ends_with(".log") {
            storage
                .remove(&Path::new(PATH).join(name))
                .expect("removed");
        }
    }

    let db = open(&storage).await;
    let mut batch = WriteBatch::new();
    batch
        .put(KeyspaceId::DEFAULT, b"after", b"reopen")
        .expect("small entry");
    let sequence = db
        .write(&batch, &WriteOptions::default())
        .await
        .expect("written");
    assert!(sequence.get() > flushed, "a sequence number was reused");
    let flushed_value = db
        .get(KeyspaceId::DEFAULT, &key(0), &ReadOptions::default())
        .await
        .expect("readable");
    assert_eq!(flushed_value, Some(value(0)));
    db.close().await.expect("closed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_writers_lose_nothing_across_flushes() {
    let storage = FaultStorage::default();
    let db = Arc::new(open(&storage).await);
    let writers: Vec<_> = (0..8)
        .map(|writer| {
            let db = Arc::clone(&db);
            tokio::spawn(async move {
                for index in (writer..WRITES).step_by(8) {
                    write(&db, index, Durability::NoWal).await.expect("written");
                }
            })
        })
        .collect();
    for writer in writers {
        writer.await.expect("joined");
    }
    until("two tables are flushed", || table_files(&db) >= 2).await;
    assert_all_readable(&db).await;
    Arc::into_inner(db)
        .expect("only handle")
        .close()
        .await
        .expect("closed");
}
