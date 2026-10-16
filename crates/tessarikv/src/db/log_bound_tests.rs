//! The log stays bounded while one keyspace sits idle: every flush freezes the newest tables of
//! all keyspaces together, so a keyspace written once and never again does not hold the log
//! segment its write landed in.

use std::path::Path;
use std::sync::Arc;

use super::Db;
use super::flush_tests::{PATH, key, until, value};
use crate::log::fault::FaultStorage;
use crate::log::segment_path;
use crate::storage::Storage as _;
use crate::{DbOptions, KeyspaceOptions, ReadOptions, WriteBatch, WriteOptions};

/// Writes of about four kilobytes to the busy keyspace: about 8 MiB of log in all.
const BUSY_WRITES: u32 = 2000;

/// Log bytes left on storage that show segments were removed: a written log of 8 MiB minus
/// several segments of 1 MiB.
const BOUND: u64 = 4 * 1024 * 1024;

fn options() -> DbOptions {
    let keyspace = KeyspaceOptions::builder()
        .memtable_bytes(256 * 1024)
        .build()
        .expect("valid");
    DbOptions::builder()
        .keyspace("idle", keyspace.clone())
        .keyspace("busy", keyspace)
        .create_missing_keyspaces(true)
        .wal_segment_bytes(1024 * 1024)
        .build()
        .expect("valid")
}

async fn open(storage: &FaultStorage) -> Db {
    Db::open_with_storage(PATH, options(), Arc::new(storage.clone()))
        .await
        .expect("opened")
}

/// Bytes of every log segment in the database's directory.
fn log_bytes(storage: &FaultStorage) -> u64 {
    storage
        .list(Path::new(PATH))
        .expect("listable")
        .iter()
        .filter(|name| name.ends_with(".log"))
        .map(|name| {
            let file = storage.open(&Path::new(PATH).join(name)).expect("open");
            file.len().expect("length")
        })
        .sum()
}

#[tokio::test]
async fn an_idle_keyspace_does_not_hold_the_log() {
    let storage = FaultStorage::default();
    let db = open(&storage).await;
    let idle = db.keyspace("idle").expect("idle");
    let busy = db.keyspace("busy").expect("busy");
    let mut batch = WriteBatch::new();
    batch.put(idle, b"once", b"written once").expect("entry");
    db.write(&batch, &WriteOptions::default())
        .await
        .expect("written");
    for index in 0..BUSY_WRITES {
        let mut batch = WriteBatch::new();
        batch.put(busy, &key(index), &value(index)).expect("entry");
        db.write(&batch, &WriteOptions::default())
            .await
            .expect("written");
    }
    let first = segment_path(Path::new(PATH), 1);
    until(
        "the first segment, holding the idle write, is removed",
        || !storage.exists(&first).expect("checked"),
    )
    .await;
    let left = log_bytes(&storage);
    assert!(left < BOUND, "{left} bytes of log left");
    db.close().await.expect("closed");
    let db = open(&storage).await;
    let read = db
        .get(idle, b"once", &ReadOptions::default())
        .await
        .expect("read");
    assert_eq!(read.as_deref(), Some(&b"written once"[..]));
}
