//! Compactions run by the installer: table files move below level 0, the levels keep their
//! order, and every value reads as written.

use std::sync::Arc;

use super::Db;
use super::flush_tests::{PATH, files_on_storage, key, until, value};
use super::tables::user_key;
use super::view::View;
use crate::log::fault::FaultStorage;
use crate::{DbOptions, KeyspaceId, KeyspaceOptions, ReadOptions, WriteBatch, WriteOptions};

const WRITES: u32 = 600;

/// Tiny memtables, a level-0 trigger of two and small levels, so a few hundred writes compact.
pub(super) fn options() -> DbOptions {
    let keyspace = KeyspaceOptions::builder()
        .memtable_bytes(64 * 1024)
        .level0_compaction_trigger(2)
        .level_base_bytes(64 * 1024)
        .level_size_multiplier(2)
        .target_file_bytes(16 * 1024)
        .build()
        .expect("valid");
    DbOptions::builder()
        .default_keyspace(keyspace)
        .wal_segment_bytes(1024 * 1024)
        .build()
        .expect("valid")
}

/// Level 0 newest first by sequence; every other level in key order with disjoint ranges.
pub(super) fn assert_levels_ordered(view: &View) {
    let files: Vec<_> = view.files(0).collect();
    let level_zero: Vec<_> = files.iter().filter(|file| file.meta.level == 0).collect();
    for pair in level_zero.windows(2) {
        assert!(
            pair[0].meta.max_sequence > pair[1].meta.max_sequence,
            "level 0 is newest first"
        );
    }
    for level in 1..7_u8 {
        let sorted: Vec<_> = files
            .iter()
            .filter(|file| file.meta.level == level)
            .collect();
        for pair in sorted.windows(2) {
            assert!(
                user_key(&pair[0].meta.largest) < user_key(&pair[1].meta.smallest),
                "level {level}: table {} reaches table {}",
                pair[0].meta.id,
                pair[1].meta.id
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn writes_compact_below_level_zero_and_every_value_reads_back() {
    let storage = FaultStorage::default();
    let db = Db::open_with_storage(PATH, options(), Arc::new(storage.clone()))
        .await
        .expect("opened");
    for index in 0..WRITES {
        let mut batch = WriteBatch::new();
        batch
            .put(KeyspaceId::DEFAULT, &key(index % 300), &value(index))
            .expect("small entry");
        db.write(&batch, &WriteOptions::default())
            .await
            .expect("written");
    }
    until("tables below level 0 and level 0 under its trigger", || {
        let view = db.view.load();
        let files: Vec<_> = view.files(0).collect();
        files.iter().any(|file| file.meta.level > 0)
            && files.iter().filter(|file| file.meta.level == 0).count() < 2
    })
    .await;
    assert_levels_ordered(&db.view.load());
    until("every compacted table file removed from storage", || {
        files_on_storage(&storage, ".sst") == db.view.load().files(0).count()
    })
    .await;
    for index in 0..300 {
        let read = db
            .get(KeyspaceId::DEFAULT, &key(index), &ReadOptions::default())
            .await
            .expect("readable");
        assert_eq!(read, Some(value(index + 300)), "key {index}");
    }
    db.close().await.expect("closed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn paced_compactions_charge_the_limiter_and_unpaced_ones_have_none() {
    let paced = options()
        .to_builder()
        .compaction_bytes_per_second(1 << 30)
        .build()
        .expect("valid");
    let db = Db::open_with_storage(PATH, paced, Arc::new(FaultStorage::default()))
        .await
        .expect("opened");
    let limiter = db.limiter.clone().expect("paced");
    for index in 0..WRITES {
        let mut batch = WriteBatch::new();
        batch
            .put(KeyspaceId::DEFAULT, &key(index % 300), &value(index))
            .expect("small entry");
        db.write(&batch, &WriteOptions::default())
            .await
            .expect("written");
    }
    until("tables below level 0", || {
        db.view.load().files(0).any(|file| file.meta.level > 0)
    })
    .await;
    let below: u64 = db
        .view
        .load()
        .files(0)
        .filter(|file| file.meta.level > 0)
        .map(|file| file.meta.size)
        .sum();
    assert!(
        limiter.charged() >= below,
        "every table below level 0 was written by a compaction"
    );
    db.close().await.expect("closed");
    let unpaced = Db::open_with_storage(PATH, options(), Arc::new(FaultStorage::default()))
        .await
        .expect("opened");
    assert!(unpaced.limiter.is_none(), "zero paces nothing");
    unpaced.close().await.expect("closed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_installer_records_what_compaction_owes() {
    let db = Db::open_with_storage(PATH, options(), Arc::new(FaultStorage::default()))
        .await
        .expect("opened");
    for index in 0..WRITES {
        let mut batch = WriteBatch::new();
        batch
            .put(KeyspaceId::DEFAULT, &key(index % 300), &value(index))
            .expect("small entry");
        db.write(&batch, &WriteOptions::default())
            .await
            .expect("written");
    }
    until("tables below level 0", || {
        db.view.load().files(0).any(|file| file.meta.level > 0)
    })
    .await;
    let signals = &db.installer.as_ref().expect("on storage").signals;
    assert!(
        signals.peak_owed() > 0,
        "level 0 reached its trigger, so compaction owed bytes at some install"
    );
    db.close().await.expect("closed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tables_larger_than_a_write_chunk_are_flushed_compacted_and_reopened_whole() {
    let keyspace = KeyspaceOptions::builder()
        .memtable_bytes(3 << 20)
        .level0_compaction_trigger(2)
        .level_base_bytes(4 << 20)
        .target_file_bytes(4 << 20)
        .build()
        .expect("valid");
    let options = DbOptions::builder()
        .default_keyspace(keyspace)
        .build()
        .expect("valid");
    let storage = FaultStorage::default();
    let db = Db::open_with_storage(PATH, options.clone(), Arc::new(storage.clone()))
        .await
        .expect("opened");
    // Pseudo-random bytes, so compression cannot shrink the tables below a chunk.
    let large = |index: u32| -> Vec<u8> {
        let mut state = u64::from(index).wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
        (0..2048)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                state.to_le_bytes()[0]
            })
            .collect()
    };
    for round in 0..3_u32 {
        for index in 0..1500_u32 {
            let mut batch = WriteBatch::new();
            batch
                .put(KeyspaceId::DEFAULT, &key(index), &large(index + round))
                .expect("entry");
            db.write(&batch, &WriteOptions::default())
                .await
                .expect("written");
        }
    }
    let chunk = u64::try_from(tessarikv_constants::TABLE_WRITE_CHUNK_BYTES).unwrap();
    until("a compacted table larger than a write chunk", || {
        db.view
            .load()
            .files(0)
            .any(|file| file.meta.level > 0 && file.meta.size > chunk)
    })
    .await;
    db.close().await.expect("closed");
    let db = Db::open_with_storage(PATH, options, Arc::new(storage))
        .await
        .expect("reopened");
    for index in 0..1500_u32 {
        let read = db
            .get(KeyspaceId::DEFAULT, &key(index), &ReadOptions::default())
            .await
            .expect("readable");
        assert_eq!(read, Some(large(index + 2)), "key {index}");
    }
    db.close().await.expect("closed");
}
