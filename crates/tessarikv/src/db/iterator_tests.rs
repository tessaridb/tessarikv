//! Iterators against a model: every snapshot, every bound, every seek target, across in-memory
//! tables and table files.

use std::collections::BTreeMap;
use std::sync::Arc;

use proptest::prelude::*;
use tessarikv_types::{KeyspaceId, SequenceNumber};

use super::block_cache::BlockCache;
use super::iterator::DbIterator;
use super::read_tests::{Op, block_on, cache_bytes, file, model, unflushed, user_key};
use super::view::View;
use crate::ReadOptions;

/// Every key and value from the iterator's position on.
async fn walk(iterator: &mut DbIterator<'_>) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut entries = Vec::new();
    while iterator.valid() {
        entries.push((iterator.key().to_vec(), iterator.value().to_vec()));
        iterator.next().await.unwrap();
    }
    entries
}

/// The model's entries in `[max(start, lower), upper)`.
fn expected(
    state: &BTreeMap<Vec<u8>, Vec<u8>>,
    start: &[u8],
    lower: Option<&[u8]>,
    upper: Option<&[u8]>,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    let from = lower.filter(|lower| *lower > start).unwrap_or(start);
    state
        .range(from.to_vec()..)
        .take_while(|(key, _)| upper.is_none_or(|upper| key.as_slice() < upper))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

/// The view of `ops` split in three tables, with the oldest `flushed` of them in table files.
fn view(ops: &[Op], first: usize, second: usize, flushed: usize, capacity: u64) -> View {
    let cache = Arc::new(BlockCache::new(
        capacity,
        tessarikv_constants::DEFAULT_BLOCK_BYTES,
    ));
    let mut view = unflushed(ops, first, second);
    for epoch in 0..flushed {
        let id = u64::try_from(epoch).unwrap() + 1;
        let table = file(
            view.table(0, u8::try_from(epoch).unwrap()).unwrap(),
            id,
            &cache,
        );
        view = view
            .after_flush(u8::try_from(epoch).unwrap(), vec![table])
            .unwrap();
    }
    view
}

proptest! {
    #[test]
    fn iterators_show_what_the_writes_say(
        ops in proptest::collection::vec((0_u8..12, proptest::option::of(any::<u16>())), 3..50),
        cuts in (any::<prop::sample::Index>(), any::<prop::sample::Index>()),
        flushed in 0_usize..=2,
        capacity in cache_bytes(),
        bounds in (proptest::option::of(0_u8..13), proptest::option::of(0_u8..13)),
        target in 0_u8..13,
    ) {
        let first = 1 + cuts.0.index(ops.len() - 2);
        let second = first + 1 + cuts.1.index(ops.len() - first - 1);
        let view = Arc::new(view(&ops, first, second, flushed, capacity));
        let lower = bounds.0.map(|key| user_key(key).to_vec());
        let upper = bounds.1.map(|key| user_key(key).to_vec());
        let options = ReadOptions {
            lower_bound: lower.clone(),
            upper_bound: upper.clone(),
            ..ReadOptions::default()
        };
        let target = user_key(target);
        for snapshot in 0..=ops.len() {
            let at = SequenceNumber::new(u64::try_from(snapshot).unwrap()).unwrap();
            let state = model(&ops, snapshot);
            let mut iterator = DbIterator::new(&view, KeyspaceId::DEFAULT, at, &options, None).unwrap();
            let (all, from_target) = block_on(async {
                iterator.seek_to_first().await.unwrap();
                let all = walk(&mut iterator).await;
                iterator.seek(&target).await.unwrap();
                (all, walk(&mut iterator).await)
            });
            prop_assert_eq!(all, expected(&state, b"", lower.as_deref(), upper.as_deref()));
            prop_assert_eq!(
                from_target,
                expected(&state, &target, lower.as_deref(), upper.as_deref())
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_iterator_shows_the_same_entries_after_flushes_and_later_writes() {
    use std::sync::Arc;

    use super::Db;
    use super::flush_tests::{PATH, key, table_files, until, value};
    use crate::log::fault::FaultStorage;
    use crate::{DbOptions, KeyspaceOptions, WriteBatch, WriteOptions};

    let keyspace = KeyspaceOptions::builder()
        .memtable_bytes(64 * 1024)
        .build()
        .unwrap();
    let options = DbOptions::builder()
        .default_keyspace(keyspace)
        .build()
        .unwrap();
    let storage = FaultStorage::default();
    let db = Db::open_with_storage(PATH, options, Arc::new(storage))
        .await
        .unwrap();
    let write = |from: u32, to: u32, version: u32| {
        let mut batch = WriteBatch::new();
        for index in from..to {
            let mut written = value(index);
            written.extend_from_slice(&version.to_le_bytes());
            batch
                .put(KeyspaceId::DEFAULT, &key(index), &written)
                .unwrap();
        }
        batch
    };
    for start in (0..40).step_by(10) {
        db.write(&write(start, start + 10, 0), &WriteOptions::default())
            .await
            .unwrap();
    }
    let mut iterator = db
        .iter(KeyspaceId::DEFAULT, &ReadOptions::default())
        .unwrap();
    // Overwrite everything, twice over the table size, so the tables the iterator read flush.
    for start in (0..40).step_by(10) {
        db.write(&write(start, start + 10, 1), &WriteOptions::default())
            .await
            .unwrap();
    }
    until("a table is flushed", || table_files(&db) >= 1).await;
    iterator.seek_to_first().await.unwrap();
    let entries = walk(&mut iterator).await;
    let expected: Vec<_> = (0..40)
        .map(|index| {
            let mut written = value(index);
            written.extend_from_slice(&0_u32.to_le_bytes());
            (key(index), written)
        })
        .collect();
    assert_eq!(entries.len(), expected.len());
    assert!(entries == expected, "the iterator shows later writes");
    drop(iterator);
    db.close().await.unwrap();
}

/// Every key and value from the iterator's position backward, at most `limit` of them so a
/// step that does not move ends the walk instead of looping.
async fn walk_back(iterator: &mut DbIterator<'_>, limit: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut entries = Vec::new();
    while iterator.valid() && entries.len() <= limit {
        entries.push((iterator.key().to_vec(), iterator.value().to_vec()));
        iterator.prev().await.unwrap();
    }
    entries
}

proptest! {
    #[test]
    fn iterators_walk_backward_and_turn_around_as_the_writes_say(
        ops in proptest::collection::vec((0_u8..12, proptest::option::of(any::<u16>())), 3..50),
        cuts in (any::<prop::sample::Index>(), any::<prop::sample::Index>()),
        flushed in 0_usize..=2,
        capacity in cache_bytes(),
        bounds in (proptest::option::of(0_u8..13), proptest::option::of(0_u8..13)),
        target in 0_u8..13,
        moves in proptest::collection::vec(any::<bool>(), 0..30),
        start in any::<prop::sample::Index>(),
    ) {
        let first = 1 + cuts.0.index(ops.len() - 2);
        let second = first + 1 + cuts.1.index(ops.len() - first - 1);
        let view = Arc::new(view(&ops, first, second, flushed, capacity));
        let lower = bounds.0.map(|key| user_key(key).to_vec());
        let upper = bounds.1.map(|key| user_key(key).to_vec());
        let options = ReadOptions {
            lower_bound: lower.clone(),
            upper_bound: upper.clone(),
            ..ReadOptions::default()
        };
        let target = user_key(target);
        for snapshot in 0..=ops.len() {
            let at = SequenceNumber::new(u64::try_from(snapshot).unwrap()).unwrap();
            let entries = expected(&model(&ops, snapshot), b"", lower.as_deref(), upper.as_deref());
            let mut iterator = DbIterator::new(&view, KeyspaceId::DEFAULT, at, &options, None).unwrap();
            let mut position = start.index(entries.len().max(1));
            let (backward, previous, walked) = block_on(async {
                iterator.seek_to_last().await.unwrap();
                let backward = walk_back(&mut iterator, entries.len()).await;
                iterator.seek_for_prev(&target).await.unwrap();
                let previous = iterator.valid().then(|| iterator.key().to_vec());
                let mut walked = Vec::new();
                if let Some((key, _)) = entries.get(position) {
                    iterator.seek(key).await.unwrap();
                    for &forward in &moves {
                        if forward {
                            iterator.next().await.unwrap();
                        } else {
                            iterator.prev().await.unwrap();
                        }
                        walked.push(
                            iterator
                                .valid()
                                .then(|| (iterator.key().to_vec(), iterator.value().to_vec())),
                        );
                        if !iterator.valid() {
                            break;
                        }
                    }
                }
                (backward, previous, walked)
            });
            prop_assert_eq!(backward, entries.iter().rev().cloned().collect::<Vec<_>>());
            let expected_previous = entries
                .iter()
                .rev()
                .find(|(key, _)| key.as_slice() <= target.as_slice())
                .map(|(key, _)| key.clone());
            prop_assert_eq!(previous, expected_previous);
            let mut expected_walk = Vec::new();
            if !entries.is_empty() {
                for &forward in moves.iter().take(walked.len()) {
                    let moved = if forward {
                        position.checked_add(1).filter(|&next| next < entries.len())
                    } else {
                        position.checked_sub(1)
                    };
                    expected_walk.push(moved.and_then(|next| entries.get(next)).cloned());
                    match moved {
                        Some(next) => position = next,
                        None => break,
                    }
                }
            }
            prop_assert_eq!(walked, expected_walk);
        }
    }
}
