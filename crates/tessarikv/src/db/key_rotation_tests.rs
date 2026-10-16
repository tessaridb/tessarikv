//! Rotating the key of an encrypted database: new files take the current key id, files keep the
//! id they were written with, and an old key is needed only until no file names it.

use std::sync::Arc;

use super::Db;
use super::flush_tests::PATH;
use crate::log::fault::FaultStorage;
use crate::{
    DbOptions, Error, KeyProvider, KeyspaceId, ReadOptions, RootKey, WriteBatch, WriteOptions,
};

/// Keys 1 and 2 when `old` is set, key 2 alone otherwise.
#[derive(Debug)]
struct Keys {
    old: bool,
}

impl KeyProvider for Keys {
    fn root_key(&self, key_id: u32) -> Option<RootKey> {
        match key_id {
            1 if self.old => Some(RootKey::new([1; 32])),
            2 => Some(RootKey::new([2; 32])),
            _ => None,
        }
    }
}

fn options(old: bool, key_id: u32) -> DbOptions {
    DbOptions::builder()
        .encryption(Arc::new(Keys { old }), key_id)
        .build()
        .expect("valid")
}

async fn open(storage: &FaultStorage, options: DbOptions) -> crate::Result<Db> {
    Db::open_with_storage(PATH, options, Arc::new(storage.clone())).await
}

async fn put_and_flush(db: &Db, key: &[u8]) {
    let mut batch = WriteBatch::new();
    batch
        .put(KeyspaceId::DEFAULT, key, b"value")
        .expect("entry");
    db.write(&batch, &WriteOptions::default())
        .await
        .expect("written");
    db.flush().await.expect("flushed");
}

async fn holds(db: &Db, key: &[u8]) -> bool {
    db.get(KeyspaceId::DEFAULT, key, &ReadOptions::default())
        .await
        .expect("read")
        .is_some()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_old_key_is_needed_until_no_file_names_it() {
    let storage = FaultStorage::default();
    let db = open(&storage, options(true, 1)).await.expect("opened");
    put_and_flush(&db, b"under-1").await;
    db.close().await.expect("closed");

    // New files take key 2; the table written under key 1 still names it.
    let db = open(&storage, options(true, 2)).await.expect("rotated");
    put_and_flush(&db, b"under-2").await;
    db.close().await.expect("closed");
    let early = open(&storage, options(false, 2)).await;
    assert!(
        matches!(early, Err(Error::KeyNotFound { key_id: 1 })),
        "{:?}",
        early.as_ref().err()
    );

    // Once compaction has rewritten every table, nothing names key 1 any more.
    let db = open(&storage, options(true, 2)).await.expect("reopened");
    db.compact_range(KeyspaceId::DEFAULT, b"", None)
        .await
        .expect("compacted");
    db.close().await.expect("closed");
    let db = open(&storage, options(false, 2))
        .await
        .expect("opens with key 2 alone");
    assert!(holds(&db, b"under-1").await);
    assert!(holds(&db, b"under-2").await);
    db.close().await.expect("closed");
}
