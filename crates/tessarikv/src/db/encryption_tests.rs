//! An encrypted database: its tables are sealed, it opens only with its key, and a changed byte
//! is reported rather than read.

use std::path::Path;
use std::sync::Arc;

use super::Db;
use super::flush_tests::PATH;
use crate::log::fault::FaultStorage;
use crate::storage::Storage as _;
use crate::{
    DbOptions, Error, KeyProvider, KeyspaceId, ReadOptions, RootKey, WriteBatch, WriteOptions,
};

/// Keys by id, as a caller's key store would hand them out.
#[derive(Debug)]
struct Keys(Vec<(u32, u8)>);

impl KeyProvider for Keys {
    fn root_key(&self, key_id: u32) -> Option<RootKey> {
        self.0
            .iter()
            .find(|(id, _)| *id == key_id)
            .map(|(_, byte)| RootKey::new([*byte; 32]))
    }
}

fn encrypted(keys: Vec<(u32, u8)>, key_id: u32) -> DbOptions {
    DbOptions::builder()
        .encryption(Arc::new(Keys(keys)), key_id)
        .build()
        .expect("valid")
}

fn key(index: u32) -> Vec<u8> {
    format!("secret-key-{index:04}").into_bytes()
}

fn value(index: u32) -> Vec<u8> {
    format!("secret-value-{index:04}").into_bytes()
}

/// Writes `keys` in two flushes and merges them, so tables come from flushes and compactions.
async fn fill(db: &Db, keys: u32) {
    for half in [0..keys / 2, keys / 2..keys] {
        let mut batch = WriteBatch::new();
        for index in half {
            batch
                .put(KeyspaceId::DEFAULT, &key(index), &value(index))
                .expect("entry");
        }
        db.write(&batch, &WriteOptions::default())
            .await
            .expect("written");
        db.flush().await.expect("flushed");
    }
    db.compact_range(KeyspaceId::DEFAULT, b"", None)
        .await
        .expect("compacted");
}

async fn read_all(db: &Db, keys: u32) -> crate::Result<()> {
    for index in 0..keys {
        let found = db
            .get(KeyspaceId::DEFAULT, &key(index), &ReadOptions::default())
            .await?;
        assert_eq!(found, Some(value(index)), "key {index}");
    }
    Ok(())
}

fn table_files(storage: &FaultStorage) -> Vec<String> {
    storage
        .list(Path::new(PATH))
        .expect("listable")
        .into_iter()
        .filter(|name| name.ends_with(".sst"))
        .collect()
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}

/// Tables written by flushes and compactions hold no key or value, and read back after a reopen.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_encrypted_database_seals_its_tables_and_reads_them_back() {
    let storage = FaultStorage::default();
    let options = encrypted(vec![(1, 7)], 1);
    let db = Db::open_with_storage(PATH, options.clone(), Arc::new(storage.clone()))
        .await
        .expect("opened");
    fill(&db, 200).await;
    read_all(&db, 200).await.expect("read");
    db.close().await.expect("closed");
    let tables = table_files(&storage);
    assert!(!tables.is_empty());
    for name in &tables {
        let bytes = storage.bytes(&Path::new(PATH).join(name));
        assert!(!contains(&bytes, b"secret-"), "{name} shows plaintext");
    }
    let db = Db::open_with_storage(PATH, options, Arc::new(storage.clone()))
        .await
        .expect("reopened");
    read_all(&db, 200).await.expect("read after reopen");
    db.close().await.expect("closed");
}

/// A database without encryption writes keys in the clear: the scan above can see them.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn control_a_plain_database_shows_its_keys() {
    let storage = FaultStorage::default();
    let db = Db::open_with_storage(PATH, DbOptions::default(), Arc::new(storage.clone()))
        .await
        .expect("opened");
    fill(&db, 200).await;
    db.close().await.expect("closed");
    let shown = table_files(&storage)
        .iter()
        .any(|name| contains(&storage.bytes(&Path::new(PATH).join(name)), b"secret-"));
    assert!(shown);
}

/// The wrong key, a key the provider does not have, no key at all, and a key for a database
/// that was written without one are each refused when the database opens.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_encrypted_database_opens_only_with_its_key() {
    let storage = FaultStorage::default();
    let db = Db::open_with_storage(PATH, encrypted(vec![(1, 7)], 1), Arc::new(storage.clone()))
        .await
        .expect("opened");
    fill(&db, 20).await;
    db.close().await.expect("closed");
    let open = |options| Db::open_with_storage(PATH, options, Arc::new(storage.clone()));
    let wrong = open(encrypted(vec![(1, 8)], 1)).await;
    assert!(
        matches!(wrong, Err(Error::WrongKey { key_id: 1 })),
        "{:?}",
        wrong.as_ref().err()
    );
    let missing = open(encrypted(vec![(2, 7)], 2)).await;
    assert!(
        matches!(missing, Err(Error::KeyNotFound { key_id: 1 })),
        "{:?}",
        missing.as_ref().err()
    );
    let none = open(DbOptions::default()).await;
    assert!(
        matches!(none, Err(Error::KeyNotFound { key_id: 1 })),
        "{:?}",
        none.as_ref().err()
    );
    let plain = FaultStorage::default();
    let db = Db::open_with_storage(PATH, DbOptions::default(), Arc::new(plain.clone()))
        .await
        .expect("opened");
    db.close().await.expect("closed");
    let later = Db::open_with_storage(PATH, encrypted(vec![(1, 7)], 1), Arc::new(plain)).await;
    assert!(
        matches!(later, Err(Error::Unsupported { .. })),
        "{:?}",
        later.as_ref().err()
    );
}

/// A table byte changed on storage is reported as corruption, never read as data.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_changed_table_byte_is_corruption() {
    let storage = FaultStorage::default();
    let options = encrypted(vec![(1, 7)], 1);
    let db = Db::open_with_storage(PATH, options.clone(), Arc::new(storage.clone()))
        .await
        .expect("opened");
    fill(&db, 20).await;
    db.close().await.expect("closed");
    for name in table_files(&storage) {
        let path = Path::new(PATH).join(name);
        let mut bytes = storage.bytes(&path);
        // The first data block starts right after the 42-byte seal header.
        bytes[42] ^= 1;
        let mut file = storage.create(&path).expect("created");
        file.append(&bytes).expect("written");
        file.sync().expect("synced");
    }
    let db = Db::open_with_storage(PATH, options, Arc::new(storage.clone()))
        .await
        .expect("reopened: the index and filter are intact");
    let read = read_all(&db, 20).await;
    assert!(matches!(read, Err(Error::Corruption { .. })), "{read:?}");
    db.close().await.expect("closed");
}
