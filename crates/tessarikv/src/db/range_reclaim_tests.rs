//! Range deletes leave the disk: a compaction drops the versions a range delete hides and, once
//! nothing older can lie under it, the range delete itself — unless a snapshot still reads them.

use std::sync::Arc;

use super::Db;
use super::compaction_model_tests::{value, versions_below_level_zero};
use super::compaction_tests::options;
use super::flush_tests::{PATH, key};
use crate::log::fault::FaultStorage;
use crate::{KeyspaceId, ReadOptions, WriteBatch, WriteOptions};

const KEYS: u32 = 200;

async fn open() -> Db {
    Db::open_with_storage(PATH, options(), Arc::new(FaultStorage::default()))
        .await
        .unwrap()
}

async fn fill(db: &Db) {
    for index in 0..KEYS {
        let mut batch = WriteBatch::new();
        batch
            .put(KeyspaceId::DEFAULT, &key(index), &value(index))
            .unwrap();
        db.write(&batch, &WriteOptions::default()).await.unwrap();
    }
}

/// Deletes the keys `[from, to)` in ranges of ten.
async fn delete_ranges(db: &Db, from: u32, to: u32) {
    for start in (from..to).step_by(10) {
        let mut batch = WriteBatch::new();
        batch
            .delete_range(KeyspaceId::DEFAULT, &key(start), &key((start + 10).min(to)))
            .unwrap();
        db.write(&batch, &WriteOptions::default()).await.unwrap();
    }
}

/// The user keys with a version in the table files, and the range deletes they hold.
async fn on_disk(db: &Db) -> (usize, usize) {
    let levels = versions_below_level_zero(db, 0).await;
    let keys = levels.iter().map(std::collections::BTreeMap::len).sum();
    let tombstones = db
        .view
        .load()
        .files(0)
        .map(|file| file.tombstones.len())
        .sum();
    (keys, tombstones)
}

async fn compact_all(db: &Db) {
    db.compact_range(KeyspaceId::DEFAULT, b"", None)
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_compaction_drops_what_range_deletes_hide_and_then_the_range_deletes() {
    let db = open().await;
    fill(&db).await;
    compact_all(&db).await;
    delete_ranges(&db, 0, KEYS / 2).await;
    compact_all(&db).await;
    let (keys, _) = on_disk(&db).await;
    assert_eq!(
        keys,
        usize::try_from(KEYS / 2).unwrap(),
        "keys left in the tables"
    );
    // A range delete whose range another table still held when its own compaction ran goes
    // the next time its table is compacted.
    compact_all(&db).await;
    let (_, tombstones) = on_disk(&db).await;
    assert_eq!(tombstones, 0, "range deletes left in the tables");
    for index in 0..KEYS {
        let read = db
            .get(KeyspaceId::DEFAULT, &key(index), &ReadOptions::default())
            .await
            .unwrap();
        assert_eq!(
            read,
            (index >= KEYS / 2).then(|| value(index)),
            "key {index}"
        );
    }
    db.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_snapshot_keeps_what_range_deletes_hid_after_it() {
    let db = open().await;
    fill(&db).await;
    compact_all(&db).await;
    let before = db.snapshot().unwrap();
    delete_ranges(&db, 0, KEYS / 2).await;
    compact_all(&db).await;
    for index in 0..KEYS {
        let then = before
            .get(KeyspaceId::DEFAULT, &key(index), &ReadOptions::default())
            .await
            .unwrap();
        assert_eq!(then, Some(value(index)), "the snapshot lost key {index}");
        let now = db
            .get(KeyspaceId::DEFAULT, &key(index), &ReadOptions::default())
            .await
            .unwrap();
        assert_eq!(
            now,
            (index >= KEYS / 2).then(|| value(index)),
            "key {index}"
        );
    }
    drop(before);
    compact_all(&db).await;
    compact_all(&db).await;
    let (keys, tombstones) = on_disk(&db).await;
    assert_eq!(
        keys,
        usize::try_from(KEYS / 2).unwrap(),
        "keys left once the snapshot went"
    );
    assert_eq!(tombstones, 0, "range deletes left once the snapshot went");
    db.close().await.unwrap();
}
