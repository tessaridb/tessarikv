//! Verifying every table file's checksums on demand.

use std::path::{Path, PathBuf};

use super::Db;
use super::block_cache::BlockKey;
use super::flush_tests::{PATH, key, open, value};
use crate::log::fault::FaultStorage;
use crate::storage::Storage as _;
use crate::{DbOptions, Error, KeyspaceId, ReadOptions, StorageMode, WriteBatch, WriteOptions};
use tessarikv_types::{SequenceNumber, encode_internal, seek_trailer};

/// Writes about forty kilobytes to the default keyspace and flushes them to a table file.
async fn flushed(db: &Db) {
    for index in 0..10 {
        let mut batch = WriteBatch::new();
        batch
            .put(KeyspaceId::DEFAULT, &key(index), &value(index))
            .expect("entry");
        db.write(&batch, &WriteOptions::default())
            .await
            .expect("written");
    }
    db.flush().await.expect("flushed");
}

/// The table files in the database's directory.
fn tables(storage: &FaultStorage) -> Vec<PathBuf> {
    storage
        .list(Path::new(PATH))
        .expect("listable")
        .into_iter()
        .filter(|name| name.ends_with(".sst"))
        .map(|name| Path::new(PATH).join(name))
        .collect()
}

/// Replaces `path` with its bytes, the one at `at` inverted.
fn damage(storage: &FaultStorage, path: &Path, at: usize) {
    let mut bytes = storage.bytes(path);
    let byte = bytes.get_mut(at).expect("inside the file");
    *byte ^= 0xff;
    let mut file = storage.create(path).expect("replaced");
    file.append(&bytes).expect("written");
}

#[tokio::test]
async fn a_whole_database_verifies() {
    let storage = FaultStorage::default();
    let db = open(&storage).await;
    flushed(&db).await;
    assert!(!tables(&storage).is_empty(), "a table file was written");
    db.verify_checksums().await.expect("every block verifies");
}

#[tokio::test]
async fn a_damaged_block_on_storage_is_found_even_while_its_copy_is_cached() {
    let storage = FaultStorage::default();
    let db = open(&storage).await;
    flushed(&db).await;
    // Read with the cache filled: the block's good copy now answers reads from memory.
    let read = db
        .get(KeyspaceId::DEFAULT, &key(5), &ReadOptions::default())
        .await
        .expect("read");
    assert_eq!(read, Some(value(5)));
    let [table] = tables(&storage).try_into().expect("one table file");
    // The data block holding the key read, found from the index alone: opening a table does
    // not read it, so only a walk reading from storage sees it damaged.
    let mut seek = Vec::new();
    encode_internal(&key(5), seek_trailer(SequenceNumber::MAX), &mut seek);
    let view = db.view.load();
    let file = view.all_files().first().expect("a table");
    let offset = file.table.offset_of(&seek).expect("indexed");
    let installer = db.installer.as_ref().expect("on storage");
    let block = BlockKey {
        file: file.meta.id,
        offset,
    };
    assert!(
        installer.cache.get(block).is_some(),
        "the damaged block's good copy is cached"
    );
    damage(
        &storage,
        &table,
        usize::try_from(offset).expect("small") + 1,
    );
    let verified = db.verify_checksums().await;
    match verified {
        Err(Error::Corruption {
            what: "table",
            path: Some(path),
            ..
        }) => assert_eq!(path, table),
        other => panic!("expected the damaged table, got {other:?}"),
    }
}

#[tokio::test]
async fn a_database_in_memory_has_nothing_to_verify() {
    let options = DbOptions::builder()
        .storage(StorageMode::Memory)
        .build()
        .expect("valid");
    let db = Db::open("verify", options).await.expect("opened");
    db.verify_checksums().await.expect("nothing to verify");
}
