//! A checkpoint: a copy of a live database holding every batch committed before it was asked
//! for, which opens as a database of its own.

use std::path::Path;
use std::sync::Arc;

use super::Db;
use super::encryption_tests::encrypted;
use super::flush_tests::{PATH, key, open, value};
use crate::log::fault::FaultStorage;
use crate::storage::Storage as _;
use crate::{DbOptions, Error, KeyspaceId, ReadOptions, StorageMode, WriteBatch, WriteOptions};

const COPY: &str = "/copy";

async fn write(db: &Db, indexes: std::ops::Range<u32>) {
    for index in indexes {
        let mut batch = WriteBatch::new();
        batch
            .put(KeyspaceId::DEFAULT, &key(index), &value(index))
            .expect("entry");
        db.write(&batch, &WriteOptions::default())
            .await
            .expect("written");
    }
}

async fn read(db: &Db, index: u32) -> Option<Vec<u8>> {
    db.get(KeyspaceId::DEFAULT, &key(index), &ReadOptions::default())
        .await
        .expect("read")
}

#[tokio::test]
async fn a_checkpoint_holds_what_was_written_before_it_and_nothing_after() {
    let storage = FaultStorage::default();
    let db = open(&storage).await;
    // Enough for table files and an in-memory table still holding some.
    write(&db, 0..40).await;
    db.flush().await.expect("flushed");
    write(&db, 40..50).await;
    db.checkpoint(COPY).await.expect("checkpoint written");
    write(&db, 50..60).await;
    db.close().await.expect("closed");
    let copy = Db::open_with_storage(COPY, super::flush_tests::options(), Arc::new(storage))
        .await
        .expect("the copy opens");
    for index in 0..50 {
        assert_eq!(read(&copy, index).await, Some(value(index)), "key {index}");
    }
    for index in 50..60 {
        assert_eq!(read(&copy, index).await, None, "key {index} came after");
    }
}

#[tokio::test]
async fn a_checkpoint_is_not_written_over_anything() {
    let storage = FaultStorage::default();
    let db = open(&storage).await;
    write(&db, 0..2).await;
    storage
        .create(&Path::new(COPY).join("somebody's file"))
        .expect("created");
    let written = db.checkpoint(COPY).await;
    assert!(
        matches!(written, Err(Error::AlreadyExists { .. })),
        "{written:?}"
    );
}

#[tokio::test]
async fn a_database_in_memory_has_no_checkpoint() {
    let options = DbOptions::builder()
        .storage(StorageMode::Memory)
        .build()
        .expect("valid");
    let db = Db::open("checkpoint", options).await.expect("opened");
    let written = db.checkpoint(COPY).await;
    assert!(
        matches!(written, Err(Error::Unsupported { .. })),
        "{written:?}"
    );
}

#[tokio::test]
async fn an_encrypted_checkpoint_opens_with_the_same_key() {
    let storage = FaultStorage::default();
    let db = Db::open_with_storage(PATH, encrypted(vec![(1, 7)], 1), Arc::new(storage.clone()))
        .await
        .expect("opened");
    write(&db, 0..10).await;
    db.checkpoint(COPY).await.expect("checkpoint written");
    db.close().await.expect("closed");
    let copy = Db::open_with_storage(COPY, encrypted(vec![(1, 7)], 1), Arc::new(storage))
        .await
        .expect("the copy opens with the key");
    for index in 0..10 {
        assert_eq!(read(&copy, index).await, Some(value(index)), "key {index}");
    }
}
