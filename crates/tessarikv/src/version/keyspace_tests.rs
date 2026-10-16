//! Keyspaces in a version: added and dropped by edits, rebuilt by the version's own edit, and
//! every contradiction refused.

use tessarikv_format::{KeyspaceRecord, ManifestEdit, TableMeta, TableRef};
use tessarikv_types::{SequenceNumber, Trailer, ValueKind, encode_internal};

use super::Version;

fn key(user: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    let trailer = Trailer::new(SequenceNumber::new(1).expect("valid"), ValueKind::Value);
    encode_internal(user, trailer, &mut out);
    out
}

fn table(keyspace: u32, id: u64) -> TableMeta {
    TableMeta {
        keyspace,
        level: 1,
        id,
        size: 100,
        smallest: key(b"a"),
        largest: key(b"b"),
        min_sequence: 1,
        max_sequence: 1,
    }
}

fn added(id: u32, name: &str) -> ManifestEdit {
    ManifestEdit {
        added_keyspaces: vec![KeyspaceRecord {
            id,
            name: name.to_owned(),
        }],
        next_keyspace: Some(id + 1),
        next_file: Some(100),
        ..ManifestEdit::default()
    }
}

fn names(version: &Version) -> Vec<(u32, String)> {
    version
        .keyspaces()
        .map(|(id, name)| (id, name.to_owned()))
        .collect()
}

#[test]
fn keyspaces_are_added_and_dropped_and_survive_the_versions_own_edit() {
    let version = Version::empty()
        .apply(&added(1, "users"))
        .and_then(|version| version.apply(&added(2, "orders")))
        .expect("added");
    let with_table = version
        .apply(&ManifestEdit {
            added: vec![table(2, 7)],
            ..ManifestEdit::default()
        })
        .expect("table added");
    assert_eq!(
        names(&with_table),
        [(1, "users".to_owned()), (2, "orders".to_owned())]
    );
    let rebuilt = Version::empty()
        .apply(&with_table.as_edit())
        .expect("rebuilt");
    assert_eq!(rebuilt, with_table);
    let dropped = with_table
        .apply(&ManifestEdit {
            dropped_keyspaces: vec![2],
            removed: vec![TableRef {
                keyspace: 2,
                level: 1,
                id: 7,
            }],
            ..ManifestEdit::default()
        })
        .expect("dropped");
    assert_eq!(names(&dropped), [(1, "users".to_owned())]);
    assert_eq!(dropped.next_keyspace(), 3, "ids are never reused");
}

#[test]
fn edits_contradicting_the_keyspaces_are_refused() {
    let version = Version::empty().apply(&added(1, "users")).expect("added");
    let refused = |edit: ManifestEdit| {
        let error = version.apply(&edit).expect_err("refused");
        assert_eq!(error.code(), "corruption", "{error}");
    };
    refused(added(1, "again"));
    refused(added(2, "users"));
    refused(added(0, "default"));
    refused(ManifestEdit {
        added_keyspaces: vec![KeyspaceRecord {
            id: 5,
            name: "ahead".to_owned(),
        }],
        ..ManifestEdit::default()
    });
    refused(ManifestEdit {
        next_keyspace: Some(1),
        ..ManifestEdit::default()
    });
    refused(ManifestEdit {
        dropped_keyspaces: vec![0],
        ..ManifestEdit::default()
    });
    refused(ManifestEdit {
        dropped_keyspaces: vec![9],
        ..ManifestEdit::default()
    });
    refused(ManifestEdit {
        added: vec![table(9, 7)],
        next_file: Some(100),
        ..ManifestEdit::default()
    });
    let with_table = version
        .apply(&ManifestEdit {
            added: vec![table(1, 7)],
            next_file: Some(100),
            ..ManifestEdit::default()
        })
        .expect("table added");
    let error = with_table
        .apply(&ManifestEdit {
            dropped_keyspaces: vec![1],
            ..ManifestEdit::default()
        })
        .expect_err("a dropped keyspace keeps no tables");
    assert_eq!(error.code(), "corruption");
}
