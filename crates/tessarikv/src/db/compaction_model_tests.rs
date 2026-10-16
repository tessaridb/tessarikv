//! Compactions against a model: every snapshot reads as the writes said, across flushes,
//! compactions and a reopen, and versions no reader can see leave the table files.

use std::collections::BTreeMap;
use std::sync::Arc;

use tessarikv_types::InternalKeyRef;

use super::Db;
use super::compaction_tests::{assert_levels_ordered, options};
use super::flush_tests::{PATH, key, until};
use crate::log::fault::FaultStorage;
use crate::{KeyspaceId, ReadOptions, WriteBatch, WriteOptions};

const KEYS: u32 = 150;

type Model = BTreeMap<u32, Vec<u8>>;

/// A kilobyte that does not compress, different for every index: the levels grow with the
/// writes, so tables come to lie in more than one level.
fn value(index: u32) -> Vec<u8> {
    let mut state = u64::from(index).wrapping_mul(0x2545_F491_4F6C_DD1D) | 1;
    (0..1024)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state.to_le_bytes()[0]
        })
        .collect()
}

/// A deterministic stream of numbers below 100.
fn draws(seed: u64) -> impl FnMut() -> u32 {
    let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    move || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        u32::try_from(state % 100).unwrap()
    }
}

async fn apply(db: &Db, model: &mut Model, op: u32, draw: u32, target: u32) {
    let mut batch = WriteBatch::new();
    let index = target % KEYS;
    if draw < 70 {
        batch
            .put(KeyspaceId::DEFAULT, &key(index), &value(op))
            .unwrap();
        model.insert(index, value(op));
    } else if draw < 92 {
        batch.delete(KeyspaceId::DEFAULT, &key(index)).unwrap();
        model.remove(&index);
    } else {
        let end = (index + 5).min(KEYS);
        batch
            .delete_range(KeyspaceId::DEFAULT, &key(index), &key(end))
            .unwrap();
        model.retain(|held, _| !(index..end).contains(held));
    }
    db.write(&batch, &WriteOptions::default()).await.unwrap();
}

async fn assert_reads(db: &Db, model: &Model, what: &str) {
    for index in 0..KEYS {
        let read = db
            .get(KeyspaceId::DEFAULT, &key(index), &ReadOptions::default())
            .await
            .unwrap();
        assert_eq!(read.as_ref(), model.get(&index), "{what}: key {index}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_snapshot_reads_as_the_model_across_compactions_and_a_reopen() {
    for seed in 1..=3 {
        let storage = FaultStorage::default();
        let db = Db::open_with_storage(PATH, options(), Arc::new(storage.clone()))
            .await
            .unwrap();
        let mut model = Model::new();
        let mut snapshots = Vec::new();
        let mut draw = draws(seed);
        for op in 0..1200 {
            let (kind, target) = (draw(), draw() + draw());
            if kind == 99 && snapshots.len() < 3 {
                snapshots.push((db.snapshot().unwrap(), model.clone()));
                continue;
            }
            if kind == 98 && !snapshots.is_empty() {
                snapshots.remove(0);
                continue;
            }
            apply(&db, &mut model, op, kind, target).await;
        }
        until("level 0 under its trigger", || {
            db.view
                .load()
                .files(0)
                .filter(|file| file.meta.level == 0)
                .count()
                < 2
        })
        .await;
        assert_levels_ordered(&db.view.load());
        assert!(
            db.view.load().files(0).any(|file| file.meta.level > 0),
            "seed {seed} compacted"
        );
        for (number, (snapshot, then)) in snapshots.iter().enumerate() {
            for index in 0..KEYS {
                let read = snapshot
                    .get(KeyspaceId::DEFAULT, &key(index), &ReadOptions::default())
                    .await
                    .unwrap();
                assert_eq!(
                    read.as_ref(),
                    then.get(&index),
                    "seed {seed} snapshot {number} key {index}"
                );
            }
        }
        assert_reads(&db, &model, "latest").await;
        drop(snapshots);
        db.close().await.unwrap();
        let reopened = Db::open_with_storage(PATH, options(), Arc::new(storage.clone()))
            .await
            .unwrap();
        assert_reads(&reopened, &model, "reopened").await;
        reopened.close().await.unwrap();
    }
}

/// The number of versions of each user key in the table files below level 0 numbered above
/// `after`, by level.
async fn versions_below_level_zero(db: &Db, after: u64) -> Vec<BTreeMap<Vec<u8>, usize>> {
    let files: Vec<_> = db
        .view
        .load()
        .files(0)
        .filter(|file| file.meta.level > 0 && file.meta.id > after)
        .cloned()
        .collect();
    tokio::task::spawn_blocking(move || {
        let mut levels = vec![BTreeMap::new(); 7];
        for file in files {
            let mut versions = file.table.iter();
            versions.seek_to_first();
            file.blocks.complete_blocking(&mut versions).unwrap();
            while versions.valid() {
                let user = InternalKeyRef::parse(versions.key())
                    .unwrap()
                    .user_key()
                    .to_vec();
                *levels[usize::from(file.meta.level)]
                    .entry(user)
                    .or_insert(0) += 1;
                versions.next();
                file.blocks.complete_blocking(&mut versions).unwrap();
            }
        }
        levels
    })
    .await
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn overwritten_versions_leave_the_tables_unless_a_snapshot_needs_them() {
    let storage = FaultStorage::default();
    let db = Db::open_with_storage(PATH, options(), Arc::new(storage.clone()))
        .await
        .unwrap();
    let write = |round: u32| {
        let db = &db;
        async move {
            for index in 0..KEYS {
                let mut batch = WriteBatch::new();
                batch
                    .put(
                        KeyspaceId::DEFAULT,
                        &key(index),
                        &value(round * KEYS + index),
                    )
                    .unwrap();
                db.write(&batch, &WriteOptions::default()).await.unwrap();
            }
        }
    };
    write(0).await;
    let first = db.snapshot().unwrap();
    for round in 1..6 {
        write(round).await;
    }
    until("level 0 under its trigger", || {
        db.view
            .load()
            .files(0)
            .filter(|file| file.meta.level == 0)
            .count()
            < 2
    })
    .await;
    for index in 0..KEYS {
        let then = first
            .get(KeyspaceId::DEFAULT, &key(index), &ReadOptions::default())
            .await
            .unwrap();
        assert_eq!(
            then,
            Some(value(index)),
            "the snapshot's version of key {index} was dropped"
        );
    }
    let held = versions_below_level_zero(&db, 0).await;
    let most = held
        .iter()
        .flat_map(BTreeMap::values)
        .max()
        .copied()
        .unwrap_or(0);
    assert!(
        most <= 2,
        "a level holds {most} versions of one key with one snapshot held"
    );
    // Versions kept for the snapshot stay in the tables written while it was held until those
    // are compacted again; every table written after it is gone keeps one version per key.
    let dropped_at = db
        .view
        .load()
        .files(0)
        .map(|file| file.meta.id)
        .max()
        .unwrap_or(0);
    drop(first);
    for round in 6..9 {
        write(round).await;
    }
    until("level 0 under its trigger", || {
        db.view
            .load()
            .files(0)
            .filter(|file| file.meta.level == 0)
            .count()
            < 2
    })
    .await;
    let freed = versions_below_level_zero(&db, dropped_at).await;
    let total: usize = freed.iter().flat_map(BTreeMap::values).sum();
    assert!(
        total < 3 * usize::try_from(KEYS).unwrap(),
        "{total} versions of {KEYS} keys after 9 rounds"
    );
    for level in &freed {
        assert!(
            level.values().all(|count| *count == 1),
            "a level kept an overwritten version"
        );
    }
    db.close().await.unwrap();
}
