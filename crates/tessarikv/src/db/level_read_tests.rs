//! Reads over sorted levels of many table files: a positioned read opens one file per level,
//! and scans and iterators still list exactly what the writes left.

use std::collections::BTreeMap;
use std::sync::Arc;

use super::Db;
use super::compaction_tests::options;
use super::flush_tests::{PATH, key};
use crate::log::fault::FaultStorage;
use crate::{DbOptions, KeyspaceId, ReadOptions, Ticker, WriteBatch, WriteOptions};

/// What a keyspace should hold: user key to value.
pub(super) type Entries = BTreeMap<Vec<u8>, Vec<u8>>;

/// A model keyed by key index, as user keys and values.
pub(super) fn by_key(model: &BTreeMap<u32, Vec<u8>>) -> Entries {
    model
        .iter()
        .map(|(index, value)| (key(*index), value.clone()))
        .collect()
}

/// Every key of `db`'s default keyspace by a forward walk, a backward walk and one scan page,
/// each as the model says; and a seek to every model key, and to just past it, lands on the
/// right key in both directions.
pub(super) async fn assert_walks(db: &Db, model: &Entries, what: &str) {
    let keyspace = KeyspaceId::DEFAULT;
    let page = db
        .scan(keyspace, b"", None, usize::MAX, &ReadOptions::default())
        .await
        .unwrap();
    let listed: Entries = page.entries.into_iter().collect();
    assert_eq!(&listed, model, "{what}: scan");
    let mut iterator = db.iter(keyspace, &ReadOptions::default()).unwrap();
    iterator.seek_to_first().await.unwrap();
    let mut forward = Entries::new();
    while iterator.valid() {
        forward.insert(iterator.key().to_vec(), iterator.value().to_vec());
        iterator.next().await.unwrap();
    }
    assert_eq!(&forward, model, "{what}: forward walk");
    iterator.seek_to_last().await.unwrap();
    let mut backward = Vec::new();
    while iterator.valid() && backward.len() <= model.len() {
        backward.push(iterator.key().to_vec());
        iterator.prev().await.unwrap();
    }
    let mut expected: Vec<Vec<u8>> = model.keys().cloned().collect();
    expected.reverse();
    assert_eq!(backward, expected, "{what}: backward walk");
    assert_bounded(db, model, what).await;
    for held in model.keys() {
        let mut past = held.clone();
        past.push(0);
        iterator.seek(&past).await.unwrap();
        let next = model
            .range(past.clone()..)
            .next()
            .map(|(key, _)| key.clone());
        assert_eq!(
            iterator.valid().then(|| iterator.key().to_vec()),
            next,
            "{what}: seek past {held:?}"
        );
        iterator.seek_for_prev(held).await.unwrap();
        assert_eq!(
            iterator.key(),
            held.as_slice(),
            "{what}: seek_for_prev {held:?}"
        );
        if iterator.valid() {
            iterator.prev().await.unwrap();
            let before = model
                .range(..held.clone())
                .next_back()
                .map(|(key, _)| key.clone());
            assert_eq!(
                iterator.valid().then(|| iterator.key().to_vec()),
                before,
                "{what}: prev from {held:?}"
            );
        }
    }
}

/// Scans and iterators bounded at model keys — where a file can start or end — list exactly the
/// model's keys between the bounds, both ways.
async fn assert_bounded(db: &Db, model: &Entries, what: &str) {
    let keys: Vec<&Vec<u8>> = model.keys().collect();
    let step = (keys.len() / 7).max(1);
    for (low, high) in keys
        .iter()
        .step_by(step)
        .zip(keys.iter().skip(step / 2 + 1).step_by(step))
    {
        let expected: Entries = model
            .range((*low).clone()..(*high).clone())
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        let page = db
            .scan(
                KeyspaceId::DEFAULT,
                low,
                Some(high),
                usize::MAX,
                &ReadOptions::default(),
            )
            .await
            .unwrap();
        let listed: Entries = page.entries.into_iter().collect();
        assert_eq!(listed, expected, "{what}: scan [{low:?}, {high:?})");
        let options = ReadOptions {
            lower_bound: Some((*low).clone()),
            upper_bound: Some((*high).clone()),
            ..ReadOptions::default()
        };
        let mut iterator = db.iter(KeyspaceId::DEFAULT, &options).unwrap();
        iterator.seek_to_first().await.unwrap();
        let mut forward = Entries::new();
        while iterator.valid() {
            forward.insert(iterator.key().to_vec(), iterator.value().to_vec());
            iterator.next().await.unwrap();
        }
        assert_eq!(forward, expected, "{what}: iterator [{low:?}, {high:?})");
        iterator.seek_to_last().await.unwrap();
        let mut backward = Vec::new();
        while iterator.valid() && backward.len() <= expected.len() {
            backward.push(iterator.key().to_vec());
            iterator.prev().await.unwrap();
        }
        let mut keys_back: Vec<Vec<u8>> = expected.keys().cloned().collect();
        keys_back.reverse();
        assert_eq!(
            backward, keys_back,
            "{what}: iterator back [{low:?}, {high:?})"
        );
    }
}

fn counted() -> DbOptions {
    options().to_builder().statistics(true).build().unwrap()
}

/// Writes `count` keys of four kilobytes each and compacts them into the sorted levels.
async fn loaded(count: u32) -> (Db, Entries) {
    let storage = FaultStorage::default();
    let db = Db::open_with_storage(PATH, counted(), Arc::new(storage))
        .await
        .unwrap();
    let mut model = Entries::new();
    for index in 0..count {
        let value = super::compaction_model_tests::value(index);
        let mut batch = WriteBatch::new();
        batch.put(KeyspaceId::DEFAULT, &key(index), &value).unwrap();
        db.write(&batch, &WriteOptions::default()).await.unwrap();
        model.insert(key(index), value);
    }
    db.compact_range(KeyspaceId::DEFAULT, b"", None)
        .await
        .unwrap();
    (db, model)
}

fn blocks_read(db: &Db) -> u64 {
    let statistics = db.statistics().unwrap();
    statistics.ticker(Ticker::BlockCacheHit) + statistics.ticker(Ticker::BlockCacheMiss)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_short_scan_reads_blocks_of_one_file_per_level() {
    let (db, _) = loaded(1000).await;
    let view = db.view.load();
    let files = view.files(0).count();
    let levels = view
        .files(0)
        .map(|file| file.meta.level)
        .collect::<std::collections::BTreeSet<_>>()
        .len();
    assert!(files > 20, "{files} table files in {levels} levels");
    let before = blocks_read(&db);
    let page = db
        .scan(
            KeyspaceId::DEFAULT,
            &key(500),
            None,
            1,
            &ReadOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(page.entries.len(), 1);
    let read = blocks_read(&db) - before;
    // One block per level, and at most one more where a level's first file ends at the key.
    let most = u64::try_from(2 * levels).unwrap();
    assert!(
        read <= most,
        "{read} blocks for {files} files in {levels} levels"
    );
    db.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scans_and_iterators_over_many_files_list_what_was_written() {
    let (db, mut model) = loaded(300).await;
    assert_walks(&db, &model, "loaded").await;
    // Deletes of every third key, flushed into level 0 above the sorted levels.
    let mut batch = WriteBatch::new();
    for index in (0..300).step_by(3) {
        batch.delete(KeyspaceId::DEFAULT, &key(index)).unwrap();
        model.remove(&key(index));
    }
    db.write(&batch, &WriteOptions::default()).await.unwrap();
    db.flush().await.unwrap();
    assert_walks(&db, &model, "with deletes above").await;
    db.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_point_read_beside_range_deletes_still_asks_the_filters() {
    let (db, _) = loaded(300).await;
    // Range deletes over keys nobody reads below, flushed into a table file.
    let mut batch = WriteBatch::new();
    for index in (0..50).step_by(5) {
        batch
            .delete_range(KeyspaceId::DEFAULT, &key(index), &key(index + 2))
            .unwrap();
    }
    db.write(&batch, &WriteOptions::default()).await.unwrap();
    db.flush().await.unwrap();
    let asked = |db: &Db| {
        let statistics = db.statistics().unwrap();
        statistics.ticker(Ticker::BloomUseful) + statistics.ticker(Ticker::BloomMaybe)
    };
    let before = asked(&db);
    let absent = db
        .get(
            KeyspaceId::DEFAULT,
            b"key00150-absent",
            &ReadOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(absent, None);
    assert!(asked(&db) > before, "the filters were not asked");
    // A key a range delete hides reads as deleted, one beside it as written.
    let hidden = db
        .get(KeyspaceId::DEFAULT, &key(10), &ReadOptions::default())
        .await
        .unwrap();
    assert_eq!(hidden, None);
    let kept = db
        .get(KeyspaceId::DEFAULT, &key(12), &ReadOptions::default())
        .await
        .unwrap();
    assert_eq!(kept, Some(super::compaction_model_tests::value(12)));
    db.close().await.unwrap();
}
