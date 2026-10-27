//! The manifest's tests, and a written database the other commands' tests read.

use std::path::Path;

use tessarikv::{Db, DbOptions, KeyspaceId, KeyspaceOptions, WriteBatch, WriteOptions};

use super::read_manifest;
use crate::error::InspectError;

/// A closed database in `dir` with a named keyspace and one table file holding `apple`.
pub async fn written(dir: &Path) {
    let options = DbOptions::builder()
        .keyspace("fruit", KeyspaceOptions::default())
        .create_missing_keyspaces(true)
        .build()
        .expect("valid");
    let db = Db::open(dir, options).await.expect("opened");
    let mut batch = WriteBatch::new();
    batch
        .put(KeyspaceId::DEFAULT, b"apple", b"red")
        .expect("entry");
    db.write(&batch, &WriteOptions::default())
        .await
        .expect("written");
    db.flush().await.expect("flushed");
    let mut batch = WriteBatch::new();
    batch
        .put(KeyspaceId::DEFAULT, b"pear", b"green")
        .expect("entry");
    db.write(&batch, &WriteOptions::default())
        .await
        .expect("written");
    db.close().await.expect("closed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_manifest_names_the_keyspaces_and_table_files() {
    let dir = tempfile::tempdir().expect("a directory");
    written(dir.path()).await;
    let manifest = read_manifest(dir.path()).expect("read");
    let mut out = Vec::new();
    manifest.print(&mut out).expect("printed");
    let text = String::from_utf8(out).expect("utf-8");
    assert!(text.contains("store format 4\n"), "{text}");
    assert!(text.contains("keyspace 1 fruit\n"), "{text}");
    assert!(text.contains("keyspace 0 level 0"), "{text}");
    assert!(text.contains("keys [apple .. apple]"), "{text}");
}

#[test]
fn a_directory_without_a_database_is_refused_by_name() {
    let dir = tempfile::tempdir().expect("a directory");
    assert!(matches!(
        read_manifest(dir.path()),
        Err(InspectError::Read { .. })
    ));
    std::fs::write(dir.path().join("CURRENT"), b"elsewhere\n").expect("written");
    assert!(matches!(
        read_manifest(dir.path()),
        Err(InspectError::Unreadable {
            what: "manifest",
            ..
        })
    ));
}

#[test]
fn a_removed_table_or_dropped_keyspace_is_no_longer_named() {
    use tessarikv_format::{KeyspaceRecord, ManifestEdit, TableMeta, TableRef};
    let table = TableMeta {
        keyspace: 1,
        level: 0,
        id: 9,
        ..TableMeta::default()
    };
    let mut manifest = super::Manifest::default();
    manifest.apply(ManifestEdit {
        added: vec![table],
        added_keyspaces: vec![KeyspaceRecord {
            id: 1,
            name: "fruit".to_owned(),
        }],
        ..ManifestEdit::default()
    });
    assert_eq!((manifest.tables.len(), manifest.keyspaces.len()), (1, 1));
    manifest.apply(ManifestEdit {
        removed: vec![TableRef {
            keyspace: 1,
            level: 0,
            id: 9,
        }],
        dropped_keyspaces: vec![1],
        ..ManifestEdit::default()
    });
    assert_eq!((manifest.tables.len(), manifest.keyspaces.len()), (0, 0));
}
