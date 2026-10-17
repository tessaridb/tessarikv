use proptest::prelude::*;

use super::{KeyspaceRecord, ManifestEdit, TableMeta, TableRef, peek_store_format};
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

fn keyspaces() -> impl Strategy<Value = (Vec<KeyspaceRecord>, Vec<u32>, Option<u32>)> {
    (
        proptest::collection::vec((any::<u32>(), ".{0,12}"), 0..3),
        proptest::collection::vec(any::<u32>(), 0..3),
        proptest::option::of(any::<u32>()),
    )
        .prop_map(|(added, dropped, next)| {
            let added = added
                .into_iter()
                .map(|(id, name)| KeyspaceRecord { id, name })
                .collect();
            (added, dropped, next)
        })
}

fn edit() -> impl Strategy<Value = ManifestEdit> {
    (
        proptest::option::of(any::<u64>()),
        proptest::option::of(any::<u64>()),
        proptest::collection::vec(table(), 0..4),
        proptest::collection::vec((any::<u32>(), 0_u8..7, any::<u64>()), 0..4),
        keyspaces(),
        proptest::option::of(any::<u32>()),
    )
        .prop_map(
            |(flushed_sequence, next_file, added, removed, keyspaces, store_format)| ManifestEdit {
                store_format,
                flushed_sequence,
                next_file,
                added_keyspaces: keyspaces.0,
                dropped_keyspaces: keyspaces.1,
                next_keyspace: keyspaces.2,
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

#[test]
fn a_keyspace_name_that_is_not_text_is_refused() {
    // version, added-keyspace tag, id 3, a two-byte name that is not UTF-8.
    let bytes = [1, 5, 3, 2, 0xFF, 0xFE];
    assert!(matches!(
        ManifestEdit::decode(&bytes),
        Err(Error::Malformed {
            reason: "a keyspace name is not UTF-8",
            ..
        })
    ));
}

#[test]
fn an_edit_is_written_as_version_two_with_the_store_format_first() {
    let mut bytes = Vec::new();
    ManifestEdit {
        flushed_sequence: Some(5),
        store_format: Some(300),
        ..ManifestEdit::default()
    }
    .encode(&mut bytes);
    // Golden: version 2, store format tag 8 = 300 as a varint, then flushed sequence tag 1 = 5.
    assert_eq!(bytes, [2, 8, 0xAC, 0x02, 1, 5]);
}

#[test]
fn a_version_one_edit_still_decodes() {
    assert_eq!(
        ManifestEdit::decode(&[1, 1, 5, 2, 9]).unwrap(),
        ManifestEdit {
            flushed_sequence: Some(5),
            next_file: Some(9),
            ..ManifestEdit::default()
        }
    );
}

#[test]
fn a_version_one_edit_knows_no_store_format_field() {
    assert!(matches!(
        ManifestEdit::decode(&[1, 8, 1]),
        Err(Error::Malformed {
            reason: "unknown field tag",
            ..
        })
    ));
}

#[test]
fn the_store_format_is_read_from_the_prefix_alone() {
    assert_eq!(peek_store_format(&[1, 1, 5]).unwrap(), None);
    assert_eq!(
        peek_store_format(&[2, 8, 0xAC, 0x02, 1, 5]).unwrap(),
        Some(300)
    );
    // A later edit version keeps the same prefix, so its store format still reads; what follows
    // is not looked at.
    assert_eq!(peek_store_format(&[7, 8, 4, 0xEE, 0xEE]).unwrap(), Some(4));
}

#[test]
fn a_prefix_without_the_store_format_is_refused() {
    assert!(matches!(
        peek_store_format(&[2, 1, 5]),
        Err(Error::Malformed {
            reason: "the first field is not the store format",
            ..
        })
    ));
    assert!(matches!(
        peek_store_format(&[]),
        Err(Error::Truncated { .. })
    ));
    assert!(matches!(
        peek_store_format(&[2, 8]),
        Err(Error::Truncated { .. })
    ));
}

#[test]
fn an_unknown_skippable_field_is_skipped() {
    // version 2, a skippable tag 0x81 with three bytes, then flushed sequence 5.
    assert_eq!(
        ManifestEdit::decode(&[2, 0x81, 3, 0xAA, 0xBB, 0xCC, 1, 5]).unwrap(),
        ManifestEdit {
            flushed_sequence: Some(5),
            ..ManifestEdit::default()
        }
    );
}

#[test]
fn an_unknown_required_field_is_refused_in_version_two() {
    assert!(matches!(
        ManifestEdit::decode(&[2, 0x09, 0]),
        Err(Error::Malformed {
            reason: "unknown field tag",
            ..
        })
    ));
}

#[test]
fn a_skippable_field_longer_than_the_edit_is_refused() {
    assert!(matches!(
        ManifestEdit::decode(&[2, 0x81, 9, 0xAA]),
        Err(Error::Truncated {
            what: "manifest edit skippable field",
            ..
        })
    ));
}

#[test]
fn a_version_one_edit_knows_no_skippable_fields() {
    assert!(matches!(
        ManifestEdit::decode(&[1, 0x81, 0]),
        Err(Error::Malformed {
            reason: "unknown field tag",
            ..
        })
    ));
}
