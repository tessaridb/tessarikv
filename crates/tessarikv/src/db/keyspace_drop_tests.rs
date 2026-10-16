//! Dropping a keyspace: its keys and files go, its name is free again under a new id, and
//! nothing written to it, flushed or racing the drop holds the database back.

use std::path::Path;
use std::sync::Arc;

use super::Db;
use super::flush_job::table_path;
use super::flush_tests::{PATH, files_on_storage, until};
use crate::log::fault::FaultStorage;
use crate::storage::Storage as _;
use crate::{DbOptions, KeyspaceId, KeyspaceOptions, ReadOptions, WriteBatch, WriteOptions};

fn batch(keyspace: KeyspaceId, key: &[u8]) -> WriteBatch {
    let mut batch = WriteBatch::new();
    batch.put(keyspace, key, b"v").expect("entry");
    batch
}

async fn put(db: &Db, keyspace: KeyspaceId, key: &[u8]) -> crate::Result<()> {
    db.write(&batch(keyspace, key), &WriteOptions::default())
        .await
        .map(|_| ())
}

fn code<T>(result: crate::Result<T>) -> Option<&'static str> {
    result.err().map(|error| error.code())
}

fn with_users(create_missing: bool) -> DbOptions {
    DbOptions::builder()
        .keyspace("users", KeyspaceOptions::default())
        .create_missing_keyspaces(create_missing)
        .build()
        .expect("valid")
}

/// On storage a dropped keyspace refuses reads and writes at once, its table files are
/// removed once no reader holds them, and the database reopens without naming it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dropped_keyspace_refuses_access_loses_its_files_and_is_not_held_after_reopening() {
    let storage = FaultStorage::default();
    let open = |options| Db::open_with_storage(PATH, options, Arc::new(storage.clone()));
    let db = open(with_users(true)).await.expect("opened");
    let users = db.keyspace("users").expect("users");
    put(&db, users, b"k").await.expect("written");
    put(&db, KeyspaceId::DEFAULT, b"k").await.expect("written");
    db.flush().await.expect("flushed");
    let files: Vec<u64> = db
        .view
        .load()
        .files(users.get())
        .map(|file| file.meta.id)
        .collect();
    assert_eq!(files.len(), 1);
    db.drop_keyspace(users).await.expect("dropped");
    assert_eq!(db.keyspace("users"), None);
    assert_eq!(db.keyspaces().len(), 1, "the default keyspace alone");
    let read = db.get(users, b"k", &ReadOptions::default()).await;
    assert_eq!(code(read), Some("unknown_keyspace"));
    assert_eq!(code(put(&db, users, b"k").await), Some("unknown_keyspace"));
    let path = table_path(Path::new(PATH), files[0]);
    until("the dropped keyspace's table file is removed", || {
        !storage.exists(&path).expect("exists")
    })
    .await;
    assert!(
        db.get(KeyspaceId::DEFAULT, b"k", &ReadOptions::default())
            .await
            .expect("read")
            .is_some(),
        "the default keyspace keeps its keys"
    );
    db.close().await.expect("closed");

    assert_eq!(
        code(open(with_users(false)).await),
        Some("invalid_argument"),
        "a dropped keyspace is not held any more"
    );
    let db = open(DbOptions::default()).await.expect("reopened");
    assert_eq!(db.keyspace("users"), None);
    let again = db
        .create_keyspace("users", KeyspaceOptions::default())
        .await
        .expect("created");
    assert!(again.get() > users.get(), "a new keyspace takes a new id");
    let read = db.get(again, b"k", &ReadOptions::default()).await;
    assert_eq!(read.expect("read"), None, "the new keyspace starts empty");
}

/// The default keyspace is never dropped, and an id not held is refused, a second drop too.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_default_keyspace_and_ids_not_held_are_refused() {
    for in_memory in [true, false] {
        let db = if in_memory {
            Db::open("memory", DbOptions::in_memory()).await
        } else {
            Db::open_with_storage(
                PATH,
                DbOptions::default(),
                Arc::new(FaultStorage::default()),
            )
            .await
        }
        .expect("opened");
        assert_eq!(
            code(db.drop_keyspace(KeyspaceId::DEFAULT).await),
            Some("invalid_argument")
        );
        assert_eq!(
            code(db.drop_keyspace(KeyspaceId::new(9)).await),
            Some("unknown_keyspace")
        );
        let users = db
            .create_keyspace("users", KeyspaceOptions::default())
            .await
            .expect("created");
        db.drop_keyspace(users).await.expect("dropped");
        assert_eq!(
            code(db.drop_keyspace(users).await),
            Some("unknown_keyspace")
        );
    }
}

/// A keyspace dropped with writes still in memory leaves nothing for the flush to write, and
/// the flush goes through.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn writes_in_memory_of_a_dropped_keyspace_are_not_flushed() {
    let db = Db::open_with_storage(PATH, with_users(true), Arc::new(FaultStorage::default()))
        .await
        .expect("opened");
    let users = db.keyspace("users").expect("users");
    put(&db, users, b"k").await.expect("written");
    put(&db, KeyspaceId::DEFAULT, b"k").await.expect("written");
    db.drop_keyspace(users).await.expect("dropped");
    db.flush().await.expect("flushed");
    assert_eq!(db.view.load().files(users.get()).count(), 0);
    assert_eq!(db.view.load().files(0).count(), 1);
}

/// A write that found the keyspace before the drop and claims in an epoch begun after it, whose
/// tables leave the keyspace out, applies the rest of its batch: its entries for the dropped
/// keyspace go with it, as replaying the log would drop them.
#[tokio::test]
async fn a_write_racing_the_drop_applies_without_the_dropped_keyspace() {
    let db = Db::open("memory", DbOptions::in_memory())
        .await
        .expect("opened");
    let users = db
        .create_keyspace("users", KeyspaceOptions::default())
        .await
        .expect("created");
    db.drop_keyspace(users).await.expect("dropped");
    db.switch_memtable().expect("switched");
    let epoch = db.view.load().newest_epoch();
    let mut both = batch(users, b"k");
    both.put(KeyspaceId::DEFAULT, b"k", b"v").expect("entry");
    assert!(
        db.view.load().table(users.get(), epoch).is_err(),
        "new tables leave a dropped keyspace out"
    );
    db.apply(epoch, 1, &both)
        .expect("the dropped keyspace's entries are skipped");
}

/// A keyspace dropped while a range compaction of it runs: the request is refused, the job's
/// outputs are discarded rather than recorded, and the database goes on.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_keyspace_dropped_while_it_compacts_discards_the_compaction() {
    let storage = FaultStorage::default();
    let db = Arc::new(
        Db::open_with_storage(PATH, with_users(true), Arc::new(storage.clone()))
            .await
            .expect("opened"),
    );
    let users = db.keyspace("users").expect("users");
    for key in [&b"a"[..], b"b"] {
        put(&db, users, key).await.expect("written");
        db.flush().await.expect("flushed");
    }
    let tables = || files_on_storage(&storage, ".sst");
    assert_eq!(tables(), 2);
    storage.pause_syncs();
    let paused = Resume(storage.clone());
    let compacting = {
        let db = Arc::clone(&db);
        tokio::spawn(async move { db.compact_range(users, b"", None).await })
    };
    let written = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while tables() < 3 {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(written.is_ok(), "no output written: {} tables", tables());
    let dropping = {
        let db = Arc::clone(&db);
        tokio::spawn(async move { db.drop_keyspace(users).await })
    };
    // The drop's manifest edit waits at the gate too: it is queued ahead of the compaction's
    // commit, whichever finishes writing first.
    until("the drop waits for its manifest sync", || {
        storage.sync_waiting("MANIFEST")
    })
    .await;
    drop(paused);
    dropping.await.expect("joined").expect("dropped");
    let refused = tokio::time::timeout(std::time::Duration::from_secs(10), compacting)
        .await
        .expect("the request for the dropped keyspace is answered")
        .expect("joined");
    assert_eq!(code(refused), Some("unknown_keyspace"));
    until("every table of the dropped keyspace is removed", || {
        tables() == 0
    })
    .await;
    put(&db, KeyspaceId::DEFAULT, b"k").await.expect("written");
    db.flush().await.expect("the database goes on");
}

/// Lets syncs through again when dropped, so a failing test never leaves a thread waiting.
struct Resume(FaultStorage);

impl Drop for Resume {
    fn drop(&mut self) {
        self.0.resume_syncs();
    }
}
