use std::path::PathBuf;
use std::sync::Arc;

use tessarikv_format::{Compression, TableMeta, TableOptions};
use tessarikv_types::{SequenceNumber, ValueKind};

use super::job::Job;
use super::output::Destination;
use super::retain::Retention;
use crate::db::block_cache::BlockCache;
use crate::db::flush_job::FileNumbers;
use crate::db::read_tests::file;
use crate::db::tables::{self, user_key};
use crate::db::view::TableFile;
use crate::memtable::Memtable;
use crate::storage::MemoryStorage;

/// A key, its sequence, and its value; no value means a delete.
type Entry<'a> = (&'a [u8], u64, Option<&'a [u8]>);

fn destination() -> Destination {
    Destination {
        storage: Arc::new(MemoryStorage::new()),
        dir: PathBuf::from("/db"),
        options: TableOptions {
            block_bytes: 256,
            restart_interval: 2,
            bloom_bits_per_key: 10,
            compression: Compression::None,
        },
        target_bytes: 4096,
        numbers: Arc::new(FileNumbers::new(100)),
        cache: Arc::new(BlockCache::new(0, 256)),
        keyspace: 0,
        level: 6,
    }
}

/// A level-0 table of `(key, sequence, value)` entries; no value means a delete.
fn input(entries: &[Entry<'_>], id: u64) -> TableFile {
    let table = Memtable::new();
    for (key, sequence, value) in entries {
        let sequence = SequenceNumber::new(*sequence).unwrap();
        match value {
            Some(value) => table
                .insert(sequence, ValueKind::Value, key, value)
                .unwrap(),
            None => table.insert(sequence, ValueKind::Delete, key, b"").unwrap(),
        }
    }
    file(&table, id, &Arc::new(BlockCache::new(0, 256)))
}

#[test]
fn every_version_of_a_key_goes_to_one_output_table() {
    let big = vec![7_u8; 600];
    let mut entries: Vec<Entry<'_>> = Vec::new();
    for sequence in 1..=40 {
        entries.push((b"a", sequence, Some(&big)));
        entries.push((b"m", sequence + 40, Some(&big)));
        entries.push((b"z", sequence + 80, Some(&big)));
    }
    // A snapshot at every sequence keeps every version.
    let snapshots: Vec<u64> = (1..=120).collect();
    let outputs = Job {
        to: destination(),
        inputs: vec![input(&entries, 1)],
        deeper: Vec::new(),
        retention: Retention::new(121, &snapshots),
        parts: 1,
    }
    .run()
    .unwrap();
    assert!(
        outputs.len() >= 3,
        "the target splits {} versions",
        entries.len()
    );
    for pair in outputs.windows(2) {
        assert!(
            user_key(&pair[0].meta.largest) < user_key(&pair[1].meta.smallest),
            "tables {} and {} share a key",
            pair[0].meta.id,
            pair[1].meta.id
        );
    }
}

#[test]
fn a_delete_stays_while_a_deeper_level_may_hold_its_key() {
    let entries: [Entry<'_>; 2] = [(b"k", 10, None), (b"q", 11, Some(b"v"))];
    let deeper = Arc::new(TableMeta {
        level: 6,
        ..(*input(&[(b"j", 1, Some(b"old")), (b"l", 2, Some(b"old"))], 50).meta).clone()
    });
    let run = |deeper: Vec<Vec<Arc<TableMeta>>>| {
        let outputs = Job {
            to: Destination {
                level: 5,
                ..destination()
            },
            inputs: vec![input(&entries, 1)],
            deeper,
            retention: Retention::new(20, &[]),
            parts: 1,
        }
        .run()
        .unwrap();
        outputs
            .iter()
            .map(|output| output.meta.smallest.clone())
            .collect::<Vec<_>>()
    };
    let kept = run(vec![vec![Arc::clone(&deeper)]]);
    assert_eq!(
        kept.iter()
            .map(|key| user_key(key).to_vec())
            .collect::<Vec<_>>(),
        [b"k".to_vec()],
        "the delete of k hides the older k a deeper table may hold"
    );
    let dropped = run(Vec::new());
    assert_eq!(
        dropped
            .iter()
            .map(|key| user_key(key).to_vec())
            .collect::<Vec<_>>(),
        [b"q".to_vec()],
        "with nothing below, the delete hides nothing and goes"
    );
}

/// Byte pairs: a key and its value, or a range delete's start and end.
type Pairs = Vec<(Vec<u8>, Vec<u8>)>;

/// Every entry of `outputs` in order and every range delete.
fn written(outputs: &[TableFile]) -> (Pairs, Pairs) {
    let mut entries = Vec::new();
    let mut deletes = Vec::new();
    for output in outputs {
        let mut versions = output.table.iter();
        versions.seek_to_first();
        output.blocks.complete_blocking(&mut versions).unwrap();
        while versions.valid() {
            entries.push((versions.key().to_vec(), versions.value().to_vec()));
            versions.next();
            output.blocks.complete_blocking(&mut versions).unwrap();
        }
        let mut found = Vec::new();
        tables::tombstones(&output.table, &mut found).unwrap();
        deletes.extend(found.into_iter().map(|range| (range.start, range.end)));
    }
    (entries, deletes)
}

#[test]
fn a_compaction_split_into_parts_writes_what_one_part_writes() {
    let key = |index: u32| format!("k{index:03}").into_bytes();
    let tables: Vec<TableFile> = (0..4_u32)
        .map(|table| {
            let memtable = Memtable::new();
            for index in (table * 10)..(table * 10 + 40) {
                let sequence = SequenceNumber::new(u64::from(table * 100 + index + 1)).unwrap();
                memtable
                    .insert(
                        sequence,
                        ValueKind::Value,
                        &key(index),
                        &[u8::try_from(table).unwrap(); 64],
                    )
                    .unwrap();
            }
            let sequence = SequenceNumber::new(u64::from(table * 100 + 99)).unwrap();
            memtable
                .insert(
                    sequence,
                    ValueKind::RangeDelete,
                    &key(table * 10 + 5),
                    &key(table * 10 + 25),
                )
                .unwrap();
            file(
                &memtable,
                u64::from(table) + 1,
                &Arc::new(BlockCache::new(0, 256)),
            )
        })
        .collect();
    let run = |parts| {
        Job {
            to: Destination {
                target_bytes: u64::MAX,
                ..destination()
            },
            inputs: tables.clone(),
            deeper: Vec::new(),
            retention: Retention::new(1000, &[150, 250]),
            parts,
        }
        .run()
        .unwrap()
    };
    let whole = run(1);
    let split = run(4);
    assert_eq!(
        whole.len(),
        1,
        "one part writes one table at an unreachable target"
    );
    assert_eq!(split.len(), 4, "four parts write four tables");
    for pair in split.windows(2) {
        assert!(user_key(&pair[0].meta.largest) < user_key(&pair[1].meta.smallest));
    }
    let (entries, deletes) = written(&whole);
    // Snapshots 150 and 250 keep one version per stripe: keys 0-19 one, 20-29 two (tables 0-1
    // and 2), 30-49 three (tables 0-1, 2 and 3), 50-69 one.
    assert_eq!(entries.len(), 20 + 20 + 60 + 20, "every kept version");
    assert_eq!(written(&split), (entries, deletes));
}
