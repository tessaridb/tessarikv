use proptest::prelude::*;

use super::{ManifestEdit, TableMeta, TableRef};
use crate::Error;

fn table() -> impl Strategy<Value = TableMeta> {
    (
        any::<u32>(),
        0_u8..7,
        any::<u64>(),
        any::<u64>(),
        proptest::collection::vec(any::<u8>(), 0..40),
        proptest::collection::vec(any::<u8>(), 0..40),
        any::<u64>(),
        any::<u64>(),
    )
        .prop_map(
            |(keyspace, level, id, size, smallest, largest, min_sequence, max_sequence)| {
                TableMeta {
                    keyspace,
                    level,
                    id,
                    size,
                    smallest,
                    largest,
                    min_sequence,
                    max_sequence,
                }
            },
        )
}

fn edit() -> impl Strategy<Value = ManifestEdit> {
    (
        proptest::option::of(any::<u64>()),
        proptest::option::of(any::<u64>()),
        proptest::collection::vec(table(), 0..4),
        proptest::collection::vec((any::<u32>(), 0_u8..7, any::<u64>()), 0..4),
    )
        .prop_map(
            |(flushed_sequence, next_file, added, removed)| ManifestEdit {
                flushed_sequence,
                next_file,
                added,
                removed: removed
                    .into_iter()
                    .map(|(keyspace, level, id)| TableRef {
                        keyspace,
                        level,
                        id,
                    })
                    .collect(),
            },
        )
}

proptest! {
    #[test]
    fn every_edit_round_trips(edit in edit()) {
        let mut bytes = Vec::new();
        edit.encode(&mut bytes);
        prop_assert_eq!(ManifestEdit::decode(&bytes).unwrap(), edit);
    }

    #[test]
    fn every_truncation_is_refused(edit in edit()) {
        let mut bytes = Vec::new();
        edit.encode(&mut bytes);
        for len in 0..bytes.len() {
            let Some(prefix) = bytes.get(..len) else { continue };
            // A prefix ending exactly between two complete fields is itself a valid, shorter
            // edit; any other prefix must be refused.
            if let Ok(shorter) = ManifestEdit::decode(prefix) {
                prop_assert!(shorter != edit, "a truncated edit decoded as the whole one");
            }
        }
    }

    #[test]
    fn arbitrary_bytes_never_panic(bytes in proptest::collection::vec(any::<u8>(), 0..200)) {
        let _ = ManifestEdit::decode(&bytes);
    }
}

#[test]
fn an_unknown_version_is_refused() {
    assert!(matches!(
        ManifestEdit::decode(&[9]),
        Err(Error::UnsupportedVersion { version: 9, .. })
    ));
}

#[test]
fn an_unknown_field_is_refused() {
    assert!(matches!(
        ManifestEdit::decode(&[1, 0x77]),
        Err(Error::Malformed {
            reason: "unknown field tag",
            ..
        })
    ));
}

#[test]
fn a_repeated_scalar_field_is_refused() {
    let mut bytes = Vec::new();
    ManifestEdit {
        flushed_sequence: Some(5),
        ..ManifestEdit::default()
    }
    .encode(&mut bytes);
    let repeated = bytes.get(1..).unwrap().to_vec();
    bytes.extend_from_slice(&repeated);
    assert!(matches!(
        ManifestEdit::decode(&bytes),
        Err(Error::Malformed {
            reason: "a field given twice",
            ..
        })
    ));
}

#[test]
fn a_key_longer_than_the_record_is_refused_before_allocating() {
    // version, add-table tag, keyspace 0, level 0, id 0, size 0, sequences 0 0, then a smallest
    // key claiming 2^32 - 1 bytes.
    let bytes = [1, 3, 0, 0, 0, 0, 0, 0, 0xFF, 0xFF, 0xFF, 0xFF, 0x0F];
    assert!(matches!(
        ManifestEdit::decode(&bytes),
        Err(Error::Truncated {
            what: "manifest edit key",
            ..
        })
    ));
}

#[test]
fn an_overlong_integer_is_malformed_not_truncated() {
    let mut bytes = vec![1, 1];
    bytes.extend_from_slice(&[0x80; 10]);
    bytes.push(0);
    assert!(matches!(
        ManifestEdit::decode(&bytes),
        Err(Error::Malformed {
            reason: "longer than its type",
            ..
        })
    ));
}
