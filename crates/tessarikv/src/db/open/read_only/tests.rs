use std::path::Path;
use std::sync::Arc;

use crate::log::fault::FaultStorage;
use crate::storage::Storage;
use crate::{
    Db, DbOptions, Error, KeyspaceId, KeyspaceOptions, ReadOptions, WalRecovery, WriteBatch,
    WriteOptions,
};

const PATH: &str = "/db";

async fn open(storage: &FaultStorage, options: DbOptions) -> crate::Result<Db> {
    Db::open_with_storage(PATH, options, Arc::new(storage.clone())).await
}

async fn open_read_only(storage: &FaultStorage, options: DbOptions) -> crate::Result<Db> {
    Db::open_read_only_with_storage(PATH, options, Arc::new(storage.clone())).await
}

fn put(key: &str, value: &str) -> WriteBatch {
    let mut batch = WriteBatch::new();
    batch
        .put(KeyspaceId::DEFAULT, key.as_bytes(), value.as_bytes())
        .expect("small entry");
    batch
}

async fn get(db: &Db, key: &str) -> Option<String> {
    db.get(KeyspaceId::DEFAULT, key.as_bytes(), &ReadOptions::default())
        .await
        .expect("readable")
        .map(|value| String::from_utf8(value).expect("utf-8"))
}

/// Every file of the database with its bytes, in name order.
fn image(storage: &FaultStorage) -> Vec<(String, Vec<u8>)> {
    let mut names = storage.list(Path::new(PATH)).expect("listable");
    names.sort();
    names
        .into_iter()
        .map(|name| {
            let bytes = storage.bytes(&Path::new(PATH).join(&name));
            (name, bytes)
        })
        .collect()
}

/// A closed database holding `flushed` in a table file and `logged` only in its log.
async fn written(storage: &FaultStorage) {
    let db = open(storage, DbOptions::default()).await.expect("opened");
    db.write(&put("flushed", "1"), &WriteOptions::default())
        .await
        .expect("written");
    db.flush().await.expect("flushed");
    db.write(&put("logged", "2"), &WriteOptions::default())
        .await
        .expect("written");
    db.close().await.expect("closed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reads_see_the_tables_and_the_replayed_log() {
    let storage = FaultStorage::default();
    written(&storage).await;
    let db = open_read_only(&storage, DbOptions::default())
        .await
        .expect("opened read-only");
    assert_eq!(get(&db, "flushed").await.as_deref(), Some("1"));
    assert_eq!(get(&db, "logged").await.as_deref(), Some("2"));
    assert_eq!(get(&db, "absent").await, None);
    db.close().await.expect("closed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_change_is_refused_by_name() {
    let storage = FaultStorage::default();
    written(&storage).await;
    let db = open_read_only(&storage, DbOptions::default())
        .await
        .expect("opened read-only");
    let refused = |result: crate::Result<()>| match result {
        Err(Error::ReadOnly { what }) => what,
        other => panic!("expected a read-only refusal, got {other:?}"),
    };
    let write = db.write(&put("k", "v"), &WriteOptions::default()).await;
    assert_eq!(refused(write.map(drop)), "a write");
    let mut transaction = db.begin().expect("begun");
    transaction
        .put(KeyspaceId::DEFAULT, b"k", b"v")
        .expect("buffered");
    let commit = transaction.commit(&WriteOptions::default()).await;
    assert_eq!(refused(commit.map(drop)), "a write");
    assert_eq!(refused(db.flush().await), "a flush");
    assert_eq!(refused(db.sync_wal().await), "a log sync");
    let compacted = db.compact_range(KeyspaceId::DEFAULT, b"", None).await;
    assert_eq!(refused(compacted), "a compaction");
    let created = db.create_keyspace("more", KeyspaceOptions::default()).await;
    assert_eq!(refused(created.map(drop)), "creating a keyspace");
    let dropped = db.drop_keyspace(KeyspaceId::new(1)).await;
    assert_eq!(refused(dropped), "dropping a keyspace");
    assert_eq!(refused(db.checkpoint("/copy").await), "a checkpoint");
    db.wait_for_compact().await.expect("nothing to wait for");
    assert_eq!(get(&db, "k").await, None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_file_changes_and_a_torn_tail_is_read_up_to_and_left_in_place() {
    let storage = FaultStorage::default();
    written(&storage).await;
    let image_path = |name: &str| Path::new(PATH).join(name);
    // An orphan table, a leftover of an interrupted cut, and a torn log tail: each is
    // something a read-write open removes or repairs.
    for (name, bytes) in [("999.sst", &b"orphan"[..]), ("000007.log.cut", b"leftover")] {
        let mut file = storage.create(&image_path(name)).expect("created");
        file.append(bytes).expect("appended");
    }
    let segments: Vec<String> = image(&storage)
        .into_iter()
        .map(|(name, _)| name)
        .filter(|name| name.ends_with(".log"))
        .collect();
    let last = segments.last().expect("a log segment");
    storage.append_raw(&image_path(last), &[0xA5; 11]);
    let before = image(&storage);

    let db = open_read_only(&storage, DbOptions::default())
        .await
        .expect("opened read-only");
    assert_eq!(get(&db, "flushed").await.as_deref(), Some("1"));
    assert_eq!(get(&db, "logged").await.as_deref(), Some("2"));
    db.verify_checksums().await.expect("verified");
    db.close().await.expect("closed");
    assert_eq!(image(&storage), before, "a read-only open changed a file");

    let strict = DbOptions::builder()
        .wal_recovery(WalRecovery::Absolute)
        .build()
        .expect("valid");
    assert!(matches!(
        open_read_only(&storage, strict).await,
        Err(Error::Corruption { .. })
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn verification_reads_the_table_files() {
    let storage = FaultStorage::default();
    written(&storage).await;
    let table = image(&storage)
        .into_iter()
        .map(|(name, _)| name)
        .find(|name| name.ends_with(".sst"))
        .expect("a table file");
    let path = Path::new(PATH).join(&table);
    let mut bytes = storage.bytes(&path);
    // Inside the first data block, which an open does not read.
    bytes[2] ^= 0xFF;
    let mut file = storage.create(&path).expect("replaced");
    file.append(&bytes).expect("appended");
    drop(file);
    let db = open_read_only(&storage, DbOptions::default())
        .await
        .expect("opened read-only");
    assert!(matches!(
        db.verify_checksums().await,
        Err(Error::Corruption { .. })
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn keyspaces_must_match_and_nothing_is_created() {
    let storage = FaultStorage::default();
    written(&storage).await;
    let naming = DbOptions::builder()
        .keyspace("missing", KeyspaceOptions::default())
        .create_missing_keyspaces(true)
        .build()
        .expect("valid");
    assert!(matches!(
        open_read_only(&storage, naming).await,
        Err(Error::InvalidArgument { reason, .. })
            if reason == "the options name a keyspace it does not hold"
    ));
    assert!(matches!(
        open_read_only(&FaultStorage::default(), DbOptions::default()).await,
        Err(Error::NotFound { .. })
    ));
    assert!(matches!(
        Db::open_read_only(PATH, DbOptions::in_memory()).await,
        Err(Error::Unsupported { .. })
    ));
}

/// Keys 1 and 2.
#[derive(Debug)]
struct Keys;

impl crate::KeyProvider for Keys {
    fn root_key(&self, key_id: u32) -> Option<crate::RootKey> {
        matches!(key_id, 1 | 2)
            .then(|| crate::RootKey::new([u8::try_from(key_id).unwrap_or(0); 32]))
    }
}

fn encrypted(key_id: u32) -> DbOptions {
    DbOptions::builder()
        .encryption(Arc::new(Keys), key_id)
        .build()
        .expect("valid")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rotated_key_reads_an_encrypted_database_without_rewriting_its_key_check() {
    let storage = FaultStorage::default();
    let db = open(&storage, encrypted(1)).await.expect("opened");
    db.write(&put("sealed", "1"), &WriteOptions::default())
        .await
        .expect("written");
    db.close().await.expect("closed");
    let before = image(&storage);

    let db = open_read_only(&storage, encrypted(2))
        .await
        .expect("opened read-only");
    assert_eq!(get(&db, "sealed").await.as_deref(), Some("1"));
    db.close().await.expect("closed");
    assert_eq!(image(&storage), before, "a read-only open changed a file");
}
