use tessarikv_format::{ManifestEdit, TableMeta, TableRef};
use tessarikv_types::{SequenceNumber, Trailer, ValueKind, encode_internal};

use super::Version;
use crate::Error;

fn key(user: &[u8], sequence: u64) -> Vec<u8> {
    let mut out = Vec::new();
    let trailer = Trailer::new(SequenceNumber::new(sequence).unwrap(), ValueKind::Value);
    encode_internal(user, trailer, &mut out);
    out
}

fn table(level: u8, id: u64, range: (&[u8], &[u8]), sequences: (u64, u64)) -> TableMeta {
    TableMeta {
        keyspace: 0,
        level,
        id,
        size: 100,
        smallest: key(range.0, sequences.1),
        largest: key(range.1, sequences.0),
        min_sequence: sequences.0,
        max_sequence: sequences.1,
    }
}

fn add(tables: Vec<TableMeta>) -> ManifestEdit {
    ManifestEdit {
        added: tables,
        ..ManifestEdit::default()
    }
}

#[test]
fn applying_edits_builds_the_table_set_newest_level_zero_first() {
    let version = Version::empty()
        .apply(&ManifestEdit {
            flushed_sequence: Some(10),
            next_file: Some(3),
            added: vec![table(0, 1, (b"a", b"m"), (1, 10))],
            removed: vec![],
            ..ManifestEdit::default()
        })
        .unwrap()
        .apply(&add(vec![table(0, 2, (b"c", b"z"), (11, 20))]))
        .unwrap();
    let ids: Vec<u64> = version.tables(0, 0).iter().map(|table| table.id).collect();
    assert_eq!(ids, vec![2, 1], "level 0 is newest first");
    assert_eq!((version.flushed_sequence(), version.next_file()), (10, 3));
    let moved = version
        .apply(&ManifestEdit {
            next_file: Some(4),
            added: vec![table(1, 3, (b"a", b"z"), (1, 20))],
            removed: vec![
                TableRef {
                    keyspace: 0,
                    level: 0,
                    id: 1,
                },
                TableRef {
                    keyspace: 0,
                    level: 0,
                    id: 2,
                },
            ],
            ..ManifestEdit::default()
        })
        .unwrap();
    assert!(moved.tables(0, 0).is_empty());
    assert_eq!(moved.tables(0, 1).len(), 1);
    assert_eq!(
        version.tables(0, 0).len(),
        2,
        "applying leaves the old version unchanged"
    );
}

#[test]
fn edits_contradicting_the_version_are_refused() {
    let version = Version::empty()
        .apply(&ManifestEdit {
            flushed_sequence: Some(10),
            next_file: Some(5),
            added: vec![table(1, 1, (b"a", b"f"), (1, 5))],
            removed: vec![],
            ..ManifestEdit::default()
        })
        .unwrap();
    let refused = [
        (
            "unknown table removed",
            ManifestEdit {
                removed: vec![TableRef {
                    keyspace: 0,
                    level: 1,
                    id: 9,
                }],
                ..ManifestEdit::default()
            },
        ),
        (
            "table added twice",
            add(vec![table(2, 1, (b"x", b"y"), (1, 5))]),
        ),
        (
            "level out of range",
            add(vec![table(7, 2, (b"x", b"y"), (1, 5))]),
        ),
        (
            "overlap in a sorted level",
            add(vec![table(1, 2, (b"e", b"k"), (1, 5))]),
        ),
        (
            "flushed sequence going back",
            ManifestEdit {
                flushed_sequence: Some(9),
                ..ManifestEdit::default()
            },
        ),
        (
            "next file going back",
            ManifestEdit {
                next_file: Some(4),
                ..ManifestEdit::default()
            },
        ),
        (
            "table numbered at or past the next file",
            add(vec![table(0, 5, (b"x", b"y"), (11, 12))]),
        ),
    ];
    for (case, edit) in refused {
        assert!(
            matches!(
                version.apply(&edit),
                Err(Error::Corruption {
                    what: "manifest",
                    ..
                })
            ),
            "{case} was accepted"
        );
    }
    assert!(
        version
            .apply(&add(vec![table(1, 3, (b"g", b"k"), (1, 5))]))
            .is_ok()
    );
}
