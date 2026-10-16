//! A crash at any durable step of a flush or a compaction keeps every acknowledged write: a
//! table written, a table synced, the manifest edit synced, the log segment a flush covers
//! removed, the input tables of a compaction removed.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use super::Db;
use super::flush_tests::{PATH, key, until, value};
use crate::log::fault::{CrashImage, FaultStorage};
use crate::log::segment_path;
use crate::storage::Storage as _;
use crate::{
    DbOptions, Durability, KeyspaceId, KeyspaceOptions, ReadOptions, WriteBatch, WriteOptions,
};

/// About 1.6 MiB of log: past the first segment, and some twenty-five flushes.
const WRITES: u32 = 400;

/// Tiny memtables; with `compacting`, a level-0 trigger of two and small levels as well.
fn options(compacting: bool) -> DbOptions {
    let keyspace = KeyspaceOptions::builder().memtable_bytes(64 * 1024);
    let keyspace = if compacting {
        keyspace
            .level0_compaction_trigger(2)
            .level_base_bytes(64 * 1024)
            .level_size_multiplier(2)
            .target_file_bytes(16 * 1024)
    } else {
        keyspace
    };
    let keyspace = keyspace.build().expect("valid");
    DbOptions::builder()
        .default_keyspace(keyspace)
        .wal_segment_bytes(1024 * 1024)
        .build()
        .expect("valid")
}

async fn open(storage: &FaultStorage, compacting: bool) -> Db {
    Db::open_with_storage(PATH, options(compacting), Arc::new(storage.clone()))
        .await
        .expect("opened")
}

/// Writes `0..WRITES` one synced batch at a time, counting each acknowledgement.
async fn write_counting(db: &Db, acknowledged: &AtomicU64) {
    let options = WriteOptions {
        durability: Durability::Sync,
        ..WriteOptions::default()
    };
    for index in 0..WRITES {
        let mut batch = WriteBatch::new();
        batch
            .put(KeyspaceId::DEFAULT, &key(index), &value(index))
            .expect("small entry");
        db.write(&batch, &options).await.expect("written");
        // SeqCst: the capture reads the count before copying the files.
        acknowledged.store(u64::from(index) + 1, Ordering::SeqCst);
    }
}

/// Reopens `image` and checks that it holds a prefix of the writes, covering every write
/// acknowledged before the image was taken.
async fn check(image: CrashImage, compacting: bool) {
    let step = &image.step;
    let db = Db::open_with_storage(PATH, options(compacting), Arc::new(image.storage))
        .await
        .unwrap_or_else(|error| panic!("{step}: reopen failed: {error}"));
    let mut present = 0_u32;
    for index in 0..WRITES {
        let read = db
            .get(KeyspaceId::DEFAULT, &key(index), &ReadOptions::default())
            .await
            .unwrap_or_else(|error| panic!("{step}: key {index}: {error}"));
        if let Some(read) = read {
            assert_eq!(present, index, "{step}: key {index} follows a missing key");
            assert!(
                read == value(index),
                "{step}: key {index} holds another value"
            );
            present += 1;
        }
    }
    assert!(
        u64::from(present) >= image.acknowledged,
        "{step}: {present} writes read back, {} were acknowledged",
        image.acknowledged
    );
    db.close().await.expect("closed");
}

/// Writes with crash images captured at every durable step, then checks every image; returns
/// the steps crashed at.
async fn crash_everywhere(compacting: bool) -> Vec<String> {
    let storage = FaultStorage::default();
    let db = open(&storage, compacting).await;
    let acknowledged = Arc::new(AtomicU64::new(0));
    storage.capture_steps(Path::new(PATH), Arc::clone(&acknowledged));
    write_counting(&db, &acknowledged).await;
    let first_segment = segment_path(Path::new(PATH), 1);
    until("the first log segment is removed", || {
        !storage.exists(&first_segment).expect("exists")
    })
    .await;
    if compacting {
        until("a table below level 0", || {
            db.view.load().files(0).any(|file| file.meta.level > 0)
        })
        .await;
    }
    db.close().await.expect("closed");
    let images = storage.take_images();
    let steps: Vec<String> = images.iter().map(|image| image.step.clone()).collect();
    for image in images {
        check(image, compacting).await;
    }
    steps
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_crash_at_any_install_step_keeps_every_acknowledged_write() {
    let steps = crash_everywhere(false).await;
    // Every install step was crashed at.
    for (step, file) in [
        ("before syncing", ".sst"),
        ("after syncing", ".sst"),
        ("before syncing", "MANIFEST"),
        ("after syncing", "MANIFEST"),
        ("removed", ".log"),
    ] {
        assert!(
            steps
                .iter()
                .any(|taken| taken.starts_with(step) && taken.contains(file)),
            "no image {step} a {file} file"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_crash_at_any_compaction_step_keeps_every_acknowledged_write() {
    let steps = crash_everywhere(true).await;
    assert!(
        steps
            .iter()
            .any(|taken| taken.starts_with("removed") && taken.contains(".sst")),
        "no image after a compaction removed an input table"
    );
}
