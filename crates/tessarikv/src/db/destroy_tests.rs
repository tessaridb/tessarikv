//! Destroying a database: every file the engine wrote is removed, other files stay, and an
//! open database is refused.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use super::Db;
use super::flush_tests::{PATH, key, open, value};
use crate::log::fault::FaultStorage;
use crate::storage::Storage as _;
use crate::{Error, KeyspaceId, ReadOptions, WriteBatch, WriteOptions};

/// A file the engine did not write.
const FOREIGN: &str = "notes.txt";

async fn written_and_closed(storage: &FaultStorage) {
    let db = open(storage).await;
    for index in 0..20 {
        let mut batch = WriteBatch::new();
        batch
            .put(KeyspaceId::DEFAULT, &key(index), &value(index))
            .expect("entry");
        db.write(&batch, &WriteOptions::default())
            .await
            .expect("written");
    }
    db.flush().await.expect("flushed");
    db.close().await.expect("closed");
}

#[tokio::test]
async fn a_destroyed_database_leaves_only_files_it_did_not_write() {
    let storage = FaultStorage::default();
    written_and_closed(&storage).await;
    storage
        .create(&Path::new(PATH).join(FOREIGN))
        .expect("created");
    Db::destroy_with_storage(PATH, Arc::new(storage.clone()))
        .await
        .expect("destroyed");
    let left = storage.list(Path::new(PATH)).expect("listable");
    assert_eq!(left, [FOREIGN], "only the foreign file is left");
    // A database opened there again starts empty.
    let db = open(&storage).await;
    let read = db
        .get(KeyspaceId::DEFAULT, &key(0), &ReadOptions::default())
        .await
        .expect("read");
    assert_eq!(read, None);
}

#[tokio::test]
async fn an_open_database_is_not_destroyed() {
    let storage = FaultStorage::default();
    let db = open(&storage).await;
    let destroyed = Db::destroy_with_storage(PATH, Arc::new(storage.clone())).await;
    assert!(
        matches!(destroyed, Err(Error::Locked { .. })),
        "{destroyed:?}"
    );
    db.close().await.expect("closed");
}

#[tokio::test]
async fn destroying_nothing_succeeds() {
    let storage = FaultStorage::default();
    Db::destroy_with_storage("/nothing", Arc::new(storage))
        .await
        .expect("nothing to destroy");
}

#[tokio::test]
async fn a_crash_while_destroying_leaves_all_of_the_data_or_none() {
    let storage = FaultStorage::default();
    written_and_closed(&storage).await;
    storage.capture_steps(Path::new(PATH), Arc::new(AtomicU64::new(0)));
    Db::destroy_with_storage(PATH, Arc::new(storage.clone()))
        .await
        .expect("destroyed");
    let images = storage.take_images();
    assert!(images.len() > 4, "crashed at every removal");
    for image in images {
        let step = image.step.clone();
        let db =
            Db::open_with_storage(PATH, super::flush_tests::options(), Arc::new(image.storage))
                .await
                .unwrap_or_else(|error| panic!("{step}: reopen failed: {error}"));
        let mut present = 0;
        for index in 0..20 {
            let read = db
                .get(KeyspaceId::DEFAULT, &key(index), &ReadOptions::default())
                .await
                .unwrap_or_else(|error| panic!("{step}: {error}"));
            present += usize::from(read == Some(value(index)));
        }
        assert!(
            present == 0 || present == 20,
            "{step}: {present} of 20 keys"
        );
        db.close().await.expect("closed");
    }
}
