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
