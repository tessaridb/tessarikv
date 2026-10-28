//! Blob files on storage: sealed when the store is encrypted, refused by name when damaged,
//! covered by checkpoints, destroy and checksum verification, and removed at open when a crash
//! left them with no table.

use std::path::Path;
use std::sync::Arc;

use super::tests::{fill, keyspace, open, read_everywhere};
use crate::db::Db;
use crate::db::flush_tests::{PATH, files_on_storage, key};
use crate::log::fault::FaultStorage;
use crate::storage::Storage as _;
use crate::{DbOptions, Error, KeyProvider, KeyspaceId, ReadOptions, RootKey};

#[derive(Debug)]
struct Key;

impl KeyProvider for Key {
    fn root_key(&self, key_id: u32) -> Option<RootKey> {
        (key_id == 1).then(|| RootKey::new([7; 32]))
    }
}

/// The names of the blob files on `storage`.
fn blob_files(storage: &FaultStorage) -> Vec<String> {
    let mut names: Vec<String> = storage
        .list(Path::new(PATH))
        .expect("listable")
        .into_iter()
        .filter(|name| name.ends_with(".blob"))
        .collect();
    names.sort();
    names
}

/// Replaces the file `name` by `change` applied to its bytes.
fn rewrite(storage: &FaultStorage, name: &str, change: impl FnOnce(&mut Vec<u8>)) {
    let path = Path::new(PATH).join(name);
    let mut bytes = storage.bytes(&path);
    change(&mut bytes);
    let mut file = storage.create(&path).expect("replaced");
    file.append(&bytes).expect("written");
    file.sync().expect("synced");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_encrypted_store_keeps_no_large_value_in_plain_bytes() {
    let shown = |storage: &FaultStorage| {
        blob_files(storage).into_iter().any(|name| {
            storage
                .bytes(&Path::new(PATH).join(name))
                .windows(9)
                .any(|window| window == b"value of ")
        })
    };
    for encrypted in [false, true] {
        let storage = FaultStorage::default();
        let options = DbOptions::builder()
            .default_keyspace(keyspace().build().expect("valid"))
            .encryption(Arc::new(Key), 1);
        let options = if encrypted {
            options
        } else {
            DbOptions::builder().default_keyspace(keyspace().build().expect("valid"))
        };
        let db = Db::open_with_storage(
            PATH,
            options.build().expect("valid"),
            Arc::new(storage.clone()),
        )
        .await
        .expect("opened");
        fill(&db, 0).await;
        assert_eq!(shown(&storage), !encrypted, "encrypted {encrypted}");
        read_everywhere(&db, 0).await;
        db.close().await.expect("closed");
    }
}

/// A store whose one blob file `change` damaged, reopened.
async fn damaged(change: impl FnOnce(&mut Vec<u8>)) -> (FaultStorage, Db) {
    let storage = FaultStorage::default();
    let db = open(&storage, keyspace()).await;
    fill(&db, 0).await;
    db.close().await.expect("closed");
    let names = blob_files(&storage);
    rewrite(&storage, names.first().expect("a blob file"), change);
    let db = open(&storage, keyspace()).await;
    (storage, db)
}

/// The refusal of a read of the large value at `key(0)`.
async fn refusal(db: &Db) -> Error {
    db.get(KeyspaceId::DEFAULT, &key(0), &ReadOptions::default())
        .await
        .expect_err("refused")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_damaged_blob_file_is_refused_by_name() {
    // A value's bytes flipped: the seal no longer holds.
    let (_storage, db) = damaged(|bytes| {
        if let Some(byte) = bytes.get_mut(4) {
            *byte ^= 0xff;
        }
    })
    .await;
    let refused = refusal(&db).await;
    assert!(
        matches!(
            &refused,
            Error::Corruption {
                what: "blob file",
                ..
            }
        ),
        "{refused:?}"
    );
    let verified = db.verify_checksums().await.expect_err("found");
    assert!(
        matches!(
            &verified,
            Error::Corruption {
                what: "blob file",
                ..
            }
        ),
        "{verified:?}"
    );
    db.close().await.expect("closed");
    // Cut short: the index points past the file.
    let (_storage, db) = damaged(|bytes| bytes.truncate(16)).await;
    let refused = refusal(&db).await;
    assert!(
        matches!(
            &refused,
            Error::Corruption { what: "blob file", detail, .. }
                if detail == "a blob index points past the file"
        ),
        "{refused:?}"
    );
    db.close().await.expect("closed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_checkpoint_holds_the_blob_files_and_destroy_removes_them() {
    let storage = FaultStorage::default();
    let db = open(&storage, keyspace()).await;
    fill(&db, 0).await;
    db.checkpoint("/copy").await.expect("checkpoint");
    db.close().await.expect("closed");
    let options = DbOptions::builder()
        .default_keyspace(keyspace().build().expect("valid"))
        .build()
        .expect("valid");
    let copy = Db::open_with_storage("/copy", options, Arc::new(storage.clone()))
        .await
        .expect("the copy opens");
    read_everywhere(&copy, 0).await;
    copy.verify_checksums().await.expect("whole");
    copy.close().await.expect("closed");
    Db::destroy_with_storage(PATH, Arc::new(storage.clone()))
        .await
        .expect("destroyed");
    assert_eq!(files_on_storage(&storage, ".blob"), 0, "no blob file left");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_blob_file_no_table_holds_is_removed_at_open() {
    let storage = FaultStorage::default();
    let db = open(&storage, keyspace()).await;
    fill(&db, 0).await;
    db.close().await.expect("closed");
    let held = blob_files(&storage);
    // What a crash leaves between writing a flush's blob file and naming its table.
    let stray = format!("{:020}.blob", u64::from(u32::MAX));
    let mut file = storage
        .create(&Path::new(PATH).join(&stray))
        .expect("created");
    file.append(b"left by a crash").expect("written");
    file.sync().expect("synced");
    drop(file);
    let db = open(&storage, keyspace()).await;
    assert_eq!(blob_files(&storage), held, "the stray file is removed");
    read_everywhere(&db, 0).await;
    db.close().await.expect("closed");
}
