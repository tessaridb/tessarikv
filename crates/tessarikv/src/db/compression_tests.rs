//! Table compression end to end: each codec's tables read back after a reopen, flushes use the
//! keyspace codec, and compactions into the bottom level use the bottommost one.

use std::sync::Arc;

use tessarikv_constants::LEVELS;
use tessarikv_format::Compression;

use super::Db;
use super::flush_tests::{PATH, key, until, value};
use crate::log::fault::FaultStorage;
use crate::{DbOptions, KeyspaceId, KeyspaceOptions, ReadOptions, WriteBatch, WriteOptions};

const WRITES: u32 = 600;

/// The bottom level's number, as tables record it.
const BOTTOM: u8 = 6;

/// Tiny memtables and levels, so a few hundred writes of four-kilobyte values flush and compact;
/// `trigger` level-0 tables start a compaction.
fn options(compression: Compression, bottom: Option<Compression>, trigger: u32) -> DbOptions {
    let keyspace = KeyspaceOptions::builder()
        .memtable_bytes(64 * 1024)
        .level0_compaction_trigger(trigger)
        .level_base_bytes(64 * 1024)
        .level_size_multiplier(2)
        .target_file_bytes(16 * 1024)
        .compression(compression)
        .bottommost_compression(bottom)
        .build()
        .expect("valid");
    DbOptions::builder()
        .default_keyspace(keyspace)
        .wal_segment_bytes(1024 * 1024)
        .build()
        .expect("valid")
}

async fn write_all(db: &Db) {
    for index in 0..WRITES {
        let mut batch = WriteBatch::new();
        batch
            .put(KeyspaceId::DEFAULT, &key(index), &value(index))
            .expect("entry");
        db.write(&batch, &WriteOptions::default())
            .await
            .expect("written");
    }
}

/// Table bytes per entry over the live tables whose level `at` accepts.
fn bytes_per_entry(db: &Db, at: impl Fn(u8) -> bool) -> u64 {
    let view = db.view.load();
    let (bytes, entries) = view
        .files(0)
        .filter(|file| at(file.meta.level))
        .fold((0, 0), |(bytes, entries), file| {
            (bytes + file.meta.size, entries + file.table.entries())
        });
    assert!(entries > 0, "no table at the levels asked for");
    bytes / entries
}

/// One value's length: what an entry takes stored as it is, at least.
fn plain() -> u64 {
    u64::try_from(value(0).len()).expect("fits")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tables_of_every_codec_read_back_after_flush_compaction_and_reopen() {
    let codecs = [
        Compression::None,
        Compression::Lz4,
        Compression::Snappy,
        Compression::Zstd,
        Compression::Deflate { level: 6 },
    ];
    for codec in codecs {
        let storage = FaultStorage::default();
        let options = options(codec, Some(codec), 2);
        let db = Db::open_with_storage(PATH, options.clone(), Arc::new(storage.clone()))
            .await
            .expect("opened");
        write_all(&db).await;
        until("a table below level 0", || {
            db.view.load().files(0).any(|file| file.meta.level > 0)
        })
        .await;
        db.close().await.expect("closed");
        let db = Db::open_with_storage(PATH, options, Arc::new(storage))
            .await
            .expect("reopened");
        for index in 0..WRITES {
            let read = db
                .get(KeyspaceId::DEFAULT, &key(index), &ReadOptions::default())
                .await
                .expect("readable");
            assert_eq!(read, Some(value(index)), "{codec:?} key {index}");
        }
        db.close().await.expect("closed");
    }
}

/// Bytes per entry of the bottom level once it holds a table, writing with no codec above it.
async fn bottom_bytes_per_entry(bottom: Option<Compression>) -> u64 {
    let storage = FaultStorage::default();
    let db = Db::open_with_storage(
        PATH,
        options(Compression::None, bottom, 2),
        Arc::new(storage),
    )
    .await
    .expect("opened");
    write_all(&db).await;
    until("a table in the bottom level", || {
        db.view
            .load()
            .files(0)
            .any(|file| file.meta.level == BOTTOM)
    })
    .await;
    let bytes = bytes_per_entry(&db, |level| level == BOTTOM);
    db.close().await.expect("closed");
    bytes
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compactions_into_the_bottom_level_write_with_the_bottommost_codec() {
    assert_eq!(usize::from(BOTTOM) + 1, LEVELS);
    let control = bottom_bytes_per_entry(None).await;
    assert!(control >= plain(), "{control} bytes per entry stored plain");
    let compressed = bottom_bytes_per_entry(Some(Compression::Deflate { level: 6 })).await;
    assert!(
        compressed < plain() / 4,
        "{compressed} bytes per entry, {} plain",
        plain()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flushes_write_with_the_keyspace_codec_not_the_bottommost_one() {
    let flushed = |compression, bottom| async move {
        let storage = FaultStorage::default();
        // A trigger past the tables these writes flush: nothing leaves level 0.
        let db = Db::open_with_storage(PATH, options(compression, bottom, 16), Arc::new(storage))
            .await
            .expect("opened");
        write_all(&db).await;
        until("a flushed table", || {
            db.view.load().files(0).next().is_some()
        })
        .await;
        let bytes = bytes_per_entry(&db, |level| level == 0);
        db.close().await.expect("closed");
        bytes
    };
    let deflate = Compression::Deflate { level: 6 };
    let plain_flush = flushed(Compression::None, Some(deflate)).await;
    assert!(plain_flush >= plain(), "{plain_flush} bytes per entry");
    let compressed_flush = flushed(deflate, None).await;
    assert!(
        compressed_flush < plain() / 4,
        "{compressed_flush} bytes per entry"
    );
}
