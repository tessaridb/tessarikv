//! Reads over table files in levels: which files a point read consults, and that a read through
//! level 0 and sorted deeper levels answers the newest version at every snapshot.

use std::collections::BTreeMap;
use std::sync::Arc;

use proptest::prelude::*;
use tessarikv_types::{KeyspaceId, SequenceNumber, ValueKind};

use super::block_cache::BlockCache;
use super::read::get_in;
use super::read_tests::{block_on, file, user_key};
use super::view::{TableFile, View};
use crate::memtable::Memtable;

fn cache() -> Arc<BlockCache> {
    Arc::new(BlockCache::new(
        1 << 20,
        tessarikv_constants::DEFAULT_BLOCK_BYTES,
    ))
}

/// `file` recorded at `level` instead of level 0.
fn at_level(file: TableFile, level: u8) -> TableFile {
    let mut meta = (*file.meta).clone();
    meta.level = level;
    TableFile {
        meta: Arc::new(meta),
        ..file
    }
}

/// A table file at `level` holding one version of each of `keys`, from sequence `first` on.
fn holding(keys: &[u8], level: u8, id: u64, first: u64) -> TableFile {
    let table = Memtable::new();
    for (sequence, key) in (first..).zip(keys) {
        let sequence = SequenceNumber::new(sequence).unwrap();
        table
            .insert(sequence, ValueKind::Value, &user_key(*key), &[*key])
            .unwrap();
    }
    at_level(file(&table, id, &cache()), level)
}

#[test]
fn a_point_read_consults_the_level_zero_files_holding_the_key_and_one_file_per_deeper_level() {
    let files = vec![
        holding(&[1, 2, 3, 4, 5], 0, 1, 100),
        holding(&[3, 5, 7, 9], 0, 2, 50),
        holding(&[0, 1, 2], 1, 3, 30),
        holding(&[4, 5, 6], 1, 4, 30),
        holding(&[8, 9], 1, 5, 30),
        holding(&[0, 2, 4, 6, 8, 9], 2, 6, 1),
    ];
    let view = View::recovered(Memtable::new(), files);
    let consulted = |key: u8| -> Vec<u64> {
        view.candidates(0, &user_key(key))
            .map(|file| file.meta.id)
            .collect()
    };
    assert_eq!(consulted(4), [1, 2, 4, 6]);
    assert_eq!(consulted(3), [1, 2, 6]);
    assert_eq!(consulted(7), [2, 6]);
    assert_eq!(consulted(0), [3, 6]);
    assert_eq!(consulted(10), Vec::<u64>::new());
    assert!(
        view.candidates(1, &user_key(4)).next().is_none(),
        "another keyspace's files"
    );
}

proptest! {
    /// Three generations of writes: the oldest in one level-2 file, the middle split by key into
    /// two level-1 files, the newest in level 0. Every key at every snapshot reads as the model.
    #[test]
    fn a_read_through_levels_answers_the_newest_version_at_every_snapshot(
        ops in proptest::collection::vec((0_u8..12, any::<u8>()), 3..60),
    ) {
        let third = ops.len() / 3;
        let (oldest, rest) = ops.split_at(third);
        let (middle, newest) = rest.split_at(third);
        let first_of = |part: usize| u64::try_from(part * third).unwrap() + 1;
        let table_of = |part: &[(u8, u8)], first: u64, keep: &dyn Fn(u8) -> bool| {
            let table = Memtable::new();
            let mut held = false;
            for (sequence, (key, value)) in (first..).zip(part) {
                if keep(*key) {
                    let sequence = SequenceNumber::new(sequence).unwrap();
                    table.insert(sequence, ValueKind::Value, &user_key(*key), &[*value]).unwrap();
                    held = true;
                }
            }
            held.then_some(table)
        };
        let mut files = Vec::new();
        let mut id = 1;
        let mut push = |table: Option<Memtable>, level: u8, files: &mut Vec<TableFile>| {
            if let Some(table) = table {
                files.push(at_level(file(&table, id, &cache()), level));
                id += 1;
            }
        };
        push(table_of(newest, first_of(2), &|_| true), 0, &mut files);
        push(table_of(middle, first_of(1), &|key| key < 6), 1, &mut files);
        push(table_of(middle, first_of(1), &|key| key >= 6), 1, &mut files);
        push(table_of(oldest, first_of(0), &|_| true), 2, &mut files);
        let view = View::recovered(Memtable::new(), files);
        for snapshot in 0..=u64::try_from(ops.len()).unwrap() {
            let mut expected = BTreeMap::new();
            for (key, value) in ops.iter().take(usize::try_from(snapshot).unwrap()) {
                expected.insert(*key, *value);
            }
            let at = SequenceNumber::new(snapshot).unwrap();
            for key in 0_u8..12 {
                let read = block_on(get_in(&view, KeyspaceId::DEFAULT, &user_key(key), at, true)).unwrap();
                prop_assert_eq!(read, expected.get(&key).map(|value| vec![*value]), "key {} at {}", key, snapshot);
            }
        }
    }
}
