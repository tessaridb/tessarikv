use std::collections::BTreeMap;
use std::sync::Arc;

use tessarikv_types::{SequenceNumber, ValueKind};

use super::{Lookup, Memtable};

fn seq(raw: u64) -> SequenceNumber {
    SequenceNumber::new(raw).unwrap()
}

fn value(lookup: Lookup<'_>) -> Option<Option<Vec<u8>>> {
    match lookup {
        Lookup::Value(bytes) => Some(Some(bytes.to_vec())),
        Lookup::Deleted => Some(None),
        Lookup::Absent => None,
    }
}

#[test]
fn a_read_sees_the_newest_version_at_or_below_its_snapshot() {
    let table = Memtable::new();
    table.insert(seq(1), ValueKind::Value, b"k", b"v1").unwrap();
    table.insert(seq(3), ValueKind::Value, b"k", b"v3").unwrap();
    table.insert(seq(5), ValueKind::Delete, b"k", b"").unwrap();
    assert_eq!(
        value(table.get(b"k", seq(0))),
        None,
        "before the first write"
    );
    assert_eq!(value(table.get(b"k", seq(1))), Some(Some(b"v1".to_vec())));
    assert_eq!(value(table.get(b"k", seq(2))), Some(Some(b"v1".to_vec())));
    assert_eq!(value(table.get(b"k", seq(4))), Some(Some(b"v3".to_vec())));
    assert_eq!(
        value(table.get(b"k", seq(5))),
        Some(None),
        "the delete hides v3"
    );
    assert_eq!(value(table.get(b"k", SequenceNumber::MAX)), Some(None));
}

#[test]
fn a_read_never_returns_a_neighbouring_key() {
    let table = Memtable::new();
    table.insert(seq(1), ValueKind::Value, b"a", b"x").unwrap();
    table.insert(seq(2), ValueKind::Value, b"ab", b"y").unwrap();
    table
        .insert(seq(3), ValueKind::Value, b"", b"empty")
        .unwrap();
    assert_eq!(value(table.get(b"aa", SequenceNumber::MAX)), None);
    assert_eq!(
        value(table.get(b"a", SequenceNumber::MAX)),
        Some(Some(b"x".to_vec()))
    );
    assert_eq!(
        value(table.get(b"ab", SequenceNumber::MAX)),
        Some(Some(b"y".to_vec()))
    );
    assert_eq!(
        value(table.get(b"", SequenceNumber::MAX)),
        Some(Some(b"empty".to_vec()))
    );
    assert_eq!(value(table.get(b"b", SequenceNumber::MAX)), None);
}

#[test]
fn inserting_the_same_version_twice_keeps_the_first() {
    let table = Memtable::new();
    table
        .insert(seq(7), ValueKind::Value, b"k", b"first")
        .unwrap();
    table
        .insert(seq(7), ValueKind::Value, b"k", b"second")
        .unwrap();
    assert_eq!(
        value(table.get(b"k", seq(7))),
        Some(Some(b"first".to_vec()))
    );
}

#[test]
fn a_range_delete_is_kept_apart_from_the_point_entries() {
    let table = Memtable::new();
    assert!(!table.has_tombstones());
    table.insert(seq(1), ValueKind::Value, b"b", b"1").unwrap();
    table
        .insert(seq(2), ValueKind::RangeDelete, b"a", b"z")
        .unwrap();
    assert!(table.has_tombstones());
    // Point reads do not see it: applying it is the reader's work.
    assert_eq!(value(table.get(b"b", seq(5))), Some(Some(b"1".to_vec())));
    let mut tombstones = Vec::new();
    table.tombstones(&mut tombstones);
    assert_eq!(
        tombstones,
        [crate::tombstones::RangeTombstone {
            start: b"a".to_vec(),
            end: b"z".to_vec(),
            sequence: seq(2),
        }]
    );
}

#[test]
fn iteration_visits_user_keys_ascending_and_versions_newest_first() {
    let table = Memtable::new();
    table.insert(seq(2), ValueKind::Value, b"b", b"b2").unwrap();
    table.insert(seq(1), ValueKind::Value, b"a", b"a1").unwrap();
    table.insert(seq(4), ValueKind::Delete, b"a", b"").unwrap();
    table.insert(seq(3), ValueKind::Value, b"b", b"b3").unwrap();
    let mut seen = Vec::new();
    let mut entries = table.iter();
    entries.seek_to_first();
    while let Some(entry) = entries.entry() {
        seen.push((entry.user_key.to_vec(), entry.sequence.get(), entry.kind));
        entries.next();
    }
    assert_eq!(
        seen,
        vec![
            (b"a".to_vec(), 4, ValueKind::Delete),
            (b"a".to_vec(), 1, ValueKind::Value),
            (b"b".to_vec(), 3, ValueKind::Value),
            (b"b".to_vec(), 2, ValueKind::Value),
        ]
    );
}

#[test]
fn seek_lands_on_the_first_version_visible_at_the_snapshot() {
    let table = Memtable::new();
    for (sequence, key) in [(1, b"a"), (2, b"c"), (5, b"c"), (3, b"e")] {
        table
            .insert(seq(sequence), ValueKind::Value, key, key)
            .unwrap();
    }
    let mut entries = table.iter();
    entries.seek(b"c", seq(4));
    let entry = entries.entry().unwrap();
    assert_eq!((entry.user_key, entry.sequence.get()), (&b"c"[..], 2));
    entries.seek(b"d", SequenceNumber::MAX);
    assert_eq!(entries.entry().unwrap().user_key, b"e");
    entries.seek(b"f", SequenceNumber::MAX);
    assert!(entries.entry().is_none());
}

#[test]
fn the_size_estimate_grows_with_inserts() {
    let table = Memtable::new();
    let empty = table.approximate_bytes();
    table
        .insert(seq(1), ValueKind::Value, b"key", &[0; 1000])
        .unwrap();
    assert!(table.approximate_bytes() >= empty + 1000);
}

#[test]
fn concurrent_inserts_match_a_sequential_model() {
    let table = Arc::new(Memtable::new());
    let threads = 8_u64;
    let per_thread = if cfg!(miri) { 20 } else { 2000 };
    let workers: Vec<_> = (0..threads)
        .map(|thread| {
            let table = Arc::clone(&table);
            std::thread::spawn(move || {
                for index in 0..per_thread {
                    let sequence = 1 + index * threads + thread;
                    let key = (sequence % 97).to_be_bytes();
                    let kind = if sequence.is_multiple_of(5) {
                        ValueKind::Delete
                    } else {
                        ValueKind::Value
                    };
                    table
                        .insert(seq(sequence), kind, &key, &sequence.to_be_bytes())
                        .unwrap();
                }
            })
        })
        .collect();
    for worker in workers {
        worker.join().unwrap();
    }
    let mut model: BTreeMap<Vec<u8>, Option<Vec<u8>>> = BTreeMap::new();
    for sequence in 1..=threads * per_thread {
        let key = (sequence % 97).to_be_bytes().to_vec();
        let entry = (sequence % 5 != 0).then(|| sequence.to_be_bytes().to_vec());
        model.insert(key, entry);
    }
    for (key, expected) in &model {
        assert_eq!(
            value(table.get(key, SequenceNumber::MAX)),
            Some(expected.clone())
        );
    }
}

#[test]
fn a_key_held_only_as_a_delete_reads_as_deleted_not_absent() {
    // An absent answer would let an older table's value show through the delete.
    let table = Memtable::sized(1024 * 1024);
    for key in 0_u32..1000 {
        table
            .insert(seq(1), ValueKind::Delete, &key.to_be_bytes(), b"")
            .unwrap();
    }
    assert!((0_u32..1000).all(|key| table.get(&key.to_be_bytes(), seq(1)) == Lookup::Deleted));
}

#[test]
fn a_table_that_outgrows_its_filter_still_answers_every_key() {
    // One 512-bit line for 2000 keys: every bit set, so the filter passes everything.
    let table = Memtable::sized(0);
    for key in 0_u32..2000 {
        table
            .insert(
                seq(1),
                ValueKind::Value,
                &key.to_be_bytes(),
                &key.to_le_bytes(),
            )
            .unwrap();
    }
    for key in 0_u32..4000 {
        let expected = (key < 2000).then(|| Some(key.to_le_bytes().to_vec()));
        assert_eq!(
            value(table.get(&key.to_be_bytes(), seq(1))),
            expected,
            "key {key}"
        );
    }
}

#[test]
fn a_successor_table_keeps_its_predecessors_filter_size() {
    let table = Memtable::sized(1024 * 1024);
    assert_eq!(table.keys.bits(), 1024 * 1024 / 8);
    assert_eq!(table.successor().keys.bits(), table.keys.bits());
}
