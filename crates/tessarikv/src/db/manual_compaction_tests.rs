//! Compacting a key range on request: every table touching it goes down to the bottom level, the
//! bottom tables it touches are rewritten, versions no snapshot needs leave, and tables outside the
//! range stay as they are.

use std::collections::BTreeMap;
use std::sync::Arc;

use tessarikv_constants::LEVELS;
use tessarikv_types::InternalKeyRef;

use super::Db;
use super::flush_tests::{PATH, key, value};
use crate::log::fault::FaultStorage;
use crate::{
    CompactionStyle, DbOptions, KeyspaceId, KeyspaceOptions, ReadOptions, WriteBatch, WriteOptions,
};

const KEYS: u32 = 200;

/// Small tables and a level-0 trigger high enough that no automatic compaction runs.
fn options(style: CompactionStyle) -> DbOptions {
    let keyspace = KeyspaceOptions::builder()
        .memtable_bytes(64 * 1024)
        .level0_compaction_trigger(50)
        .level0_slowdown_trigger(50)
        .level0_stop_trigger(60)
        .target_file_bytes(16 * 1024)
        .compaction(style)
        .build()
        .expect("valid");
    DbOptions::builder()
        .default_keyspace(keyspace)
        .wal_segment_bytes(1024 * 1024)
        .build()
        .expect("valid")
}

async fn open(style: CompactionStyle) -> Db {
    Db::open_with_storage(PATH, options(style), Arc::new(FaultStorage::default()))
        .await
        .expect("opened")
}

async fn put(db: &Db, index: u32, round: u32) {
    let mut batch = WriteBatch::new();
    batch
        .put(
            KeyspaceId::DEFAULT,
            &key(index),
            &value(index * 1000 + round),
        )
        .expect("entry");
    db.write(&batch, &WriteOptions::default())
        .await
        .expect("written");
}

async fn delete(db: &Db, index: u32) {
    let mut batch = WriteBatch::new();
    batch
        .delete(KeyspaceId::DEFAULT, &key(index))
        .expect("entry");
    db.write(&batch, &WriteOptions::default())
        .await
        .expect("written");
}

async fn read(db: &Db, index: u32) -> Option<Vec<u8>> {
    db.get(KeyspaceId::DEFAULT, &key(index), &ReadOptions::default())
        .await
        .expect("read")
}

/// The versions every table file stores, by user key, and each table's level and number.
pub(super) async fn stored(db: &Db) -> (BTreeMap<Vec<u8>, usize>, Vec<(u8, u64)>) {
    let files: Vec<_> = db.view.load().files(0).cloned().collect();
    tokio::task::spawn_blocking(move || {
        let mut versions_of = BTreeMap::new();
        let mut tables = Vec::new();
        for file in files {
            tables.push((file.meta.level, file.meta.id));
            let mut versions = file.table.iter();
            versions.seek_to_first();
            file.blocks
                .complete_blocking(&mut versions)
                .expect("block read");
            while versions.valid() {
                let user = InternalKeyRef::parse(versions.key())
                    .expect("stored key")
                    .user_key()
                    .to_vec();
                *versions_of.entry(user).or_insert(0) += 1;
                versions.next();
                file.blocks
                    .complete_blocking(&mut versions)
                    .expect("block read");
            }
        }
        (versions_of, tables)
    })
    .await
    .expect("table walk")
}

fn bottom() -> u8 {
    u8::try_from(LEVELS - 1).expect("few levels")
}

/// Three flushed generations (written, overwritten, half deleted) in level 0; a full manual
/// compaction leaves one version of each live key, all in the bottom level, and reads unchanged.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_full_manual_compaction_leaves_one_version_per_live_key_in_the_bottom_level() {
    let db = open(CompactionStyle::Leveled).await;
    for round in 0..2 {
        for index in 0..KEYS {
            put(&db, index, round).await;
        }
        db.flush().await.expect("flushed");
    }
    for index in 0..KEYS / 2 {
        delete(&db, index).await;
    }
    db.flush().await.expect("flushed");
    let (before, _) = stored(&db).await;
    assert!(before.values().any(|versions| *versions > 1));
    db.compact_range(KeyspaceId::DEFAULT, b"", None)
        .await
        .expect("compacted");
    let (after, tables) = stored(&db).await;
    assert!(
        tables.iter().all(|(level, _)| *level == bottom()),
        "every table is in the bottom level: {tables:?}"
    );
    let live: BTreeMap<Vec<u8>, usize> = (KEYS / 2..KEYS).map(|index| (key(index), 1)).collect();
    assert_eq!(after, live, "one version per live key, no tombstone");
    for index in 0..KEYS {
        let expected = (index >= KEYS / 2).then(|| value(index * 1000 + 1));
        assert_eq!(read(&db, index).await, expected, "key {index}");
    }
}

/// Two key ranges in separate bottom tables: compacting one rewrites its tables and leaves the
/// other's untouched.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_bounded_manual_compaction_rewrites_only_the_tables_touching_its_range() {
    let db = open(CompactionStyle::Leveled).await;
    for index in 0..KEYS / 4 {
        put(&db, index, 0).await;
    }
    db.compact_range(KeyspaceId::DEFAULT, b"", None)
        .await
        .expect("compacted");
    for index in KEYS * 3 / 4..KEYS {
        put(&db, index, 0).await;
    }
    db.compact_range(KeyspaceId::DEFAULT, &key(KEYS * 3 / 4), None)
        .await
        .expect("compacted");
    let (_, tables) = stored(&db).await;
    let first: Vec<u64> = tables.iter().map(|(_, id)| *id).collect();
    db.compact_range(KeyspaceId::DEFAULT, &key(0), Some(&key(KEYS / 4)))
        .await
        .expect("compacted");
    let (versions, tables) = stored(&db).await;
    assert!(tables.iter().all(|(level, _)| *level == bottom()));
    let kept: Vec<u64> = tables
        .iter()
        .map(|(_, id)| *id)
        .filter(|id| first.contains(id))
        .collect();
    assert!(!kept.is_empty(), "the tables beyond the range stay");
    assert!(
        kept.len() < first.len(),
        "the tables in the range are rewritten: {first:?} → {tables:?}"
    );
    assert_eq!(
        versions.len(),
        usize::try_from(KEYS / 2).expect("small count")
    );
}

/// A snapshot taken before an overwrite still reads the old value after a full manual
/// compaction, which keeps that version for it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_held_snapshot_keeps_the_version_it_reads_through_a_manual_compaction() {
    let db = open(CompactionStyle::Leveled).await;
    put(&db, 7, 0).await;
    db.flush().await.expect("flushed");
    let snapshot = db.snapshot().expect("snapshot");
    put(&db, 7, 1).await;
    db.compact_range(KeyspaceId::DEFAULT, b"", None)
        .await
        .expect("compacted");
    let old = snapshot
        .get(KeyspaceId::DEFAULT, &key(7), &ReadOptions::default())
        .await
        .expect("read");
    assert_eq!(old, Some(value(7000)));
    assert_eq!(read(&db, 7).await, Some(value(7001)));
    let (versions, _) = stored(&db).await;
    assert_eq!(versions.get(&key(7)), Some(&2));
}

/// Writers keep going while a manual compaction runs; afterwards every key reads its last value.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn writes_during_a_manual_compaction_are_all_kept() {
    let db = Arc::new(open(CompactionStyle::Leveled).await);
    for index in 0..KEYS {
        put(&db, index, 0).await;
    }
    db.flush().await.expect("flushed");
    let writer = {
        let db = Arc::clone(&db);
        tokio::spawn(async move {
            for round in 1..4 {
                for index in 0..KEYS {
                    put(&db, index, round).await;
                }
            }
        })
    };
    db.compact_range(KeyspaceId::DEFAULT, b"", None)
        .await
        .expect("compacted");
    writer.await.expect("writer finished");
    db.compact_range(KeyspaceId::DEFAULT, b"", None)
        .await
        .expect("compacted");
    for index in 0..KEYS {
        assert_eq!(read(&db, index).await, Some(value(index * 1000 + 3)));
    }
}

/// A first-in-first-out keyspace never merges: a manual compaction flushes and changes nothing
/// else. In memory there is nothing to compact.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_manual_compaction_in_memory_or_first_in_first_out_only_flushes() {
    let memory = Db::open("memory", DbOptions::in_memory())
        .await
        .expect("opened");
    memory
        .compact_range(KeyspaceId::DEFAULT, b"", None)
        .await
        .expect("nothing to do");
    let db = open(CompactionStyle::Fifo {
        max_table_bytes: 1 << 30,
    })
    .await;
    put(&db, 1, 0).await;
    db.flush().await.expect("flushed");
    put(&db, 1, 1).await;
    db.compact_range(KeyspaceId::DEFAULT, b"", None)
        .await
        .expect("compacted");
    let (versions, tables) = stored(&db).await;
    assert_eq!(tables.len(), 2);
    assert!(tables.iter().all(|(level, _)| *level == 0));
    assert_eq!(versions.get(&key(1)), Some(&2));
}
