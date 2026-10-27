//! A keyspace's compaction filter: compaction asks it about the versions no snapshot can read,
//! a removed version reads as deleted from then on without uncovering an older one, a snapshot
//! held across the compaction keeps its answer, and the filter is named in the options record.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::db::Db;
use crate::db::flush_tests::PATH;
use crate::db::manual_compaction_tests::stored;
use crate::log::fault::FaultStorage;
use crate::storage::Storage;
use crate::{
    CompactionFilter, DbOptions, FilterDecision, KeyspaceId, KeyspaceOptions, ReadOptions,
    WriteBatch, WriteOptions,
};

/// Removes every version whose value starts with `drop`, and counts what it is shown.
#[derive(Debug, Default)]
struct DropMarked {
    shown: AtomicU64,
}

impl CompactionFilter for DropMarked {
    fn name(&self) -> &str {
        "drop-marked"
    }

    fn filter(&self, _key: &[u8], value: &[u8]) -> FilterDecision {
        // Relaxed: a test counter, read after the compaction returned.
        self.shown.fetch_add(1, Ordering::Relaxed);
        if value.starts_with(b"drop") {
            FilterDecision::Remove
        } else {
            FilterDecision::Keep
        }
    }
}

fn options(filter: &Arc<DropMarked>) -> DbOptions {
    let shared: Arc<dyn CompactionFilter> = filter.clone();
    let keyspace = KeyspaceOptions::builder()
        .compaction_filter(shared)
        .build()
        .expect("valid");
    DbOptions::builder()
        .default_keyspace(keyspace)
        .build()
        .expect("valid")
}

async fn open(storage: &FaultStorage, filter: &Arc<DropMarked>) -> Db {
    Db::open_with_storage(PATH, options(filter), Arc::new(storage.clone()))
        .await
        .expect("opened")
}

async fn put(db: &Db, key: &[u8], value: &[u8]) {
    let mut batch = WriteBatch::new();
    batch.put(KeyspaceId::DEFAULT, key, value).expect("entry");
    db.write(&batch, &WriteOptions::default())
        .await
        .expect("written");
}

async fn delete(db: &Db, key: &[u8]) {
    let mut batch = WriteBatch::new();
    batch.delete(KeyspaceId::DEFAULT, key).expect("entry");
    db.write(&batch, &WriteOptions::default())
        .await
        .expect("written");
}

async fn read(db: &Db, key: &[u8]) -> Option<Vec<u8>> {
    db.get(KeyspaceId::DEFAULT, key, &ReadOptions::default())
        .await
        .expect("read")
}

async fn compact(db: &Db) {
    db.compact_range(KeyspaceId::DEFAULT, b"", None)
        .await
        .expect("compacted");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_removed_version_leaves_the_tables_and_a_kept_one_stays() {
    let storage = FaultStorage::default();
    let filter = Arc::new(DropMarked::default());
    let db = open(&storage, &filter).await;
    put(&db, b"a", b"keep-a").await;
    put(&db, b"b", b"drop-b").await;
    db.flush().await.expect("flushed");
    compact(&db).await;
    assert_eq!(read(&db, b"a").await.as_deref(), Some(&b"keep-a"[..]));
    assert_eq!(read(&db, b"b").await, None, "the filter removed it");
    let (versions, _) = stored(&db).await;
    let kept: Vec<&[u8]> = versions.keys().map(Vec::as_slice).collect();
    assert_eq!(kept, vec![&b"a"[..]], "nothing of the removed key is left");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_removed_version_does_not_uncover_an_older_one_below_it() {
    let storage = FaultStorage::default();
    let filter = Arc::new(DropMarked::default());
    let db = open(&storage, &filter).await;
    put(&db, b"k", b"keep-old").await;
    db.flush().await.expect("flushed");
    compact(&db).await;
    put(&db, b"k", b"drop-new").await;
    db.flush().await.expect("flushed");
    // Only the level-0 table compacts into the bottom one; the filter is shown "drop-new".
    compact(&db).await;
    assert_eq!(read(&db, b"k").await, None, "the older value stays hidden");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_snapshot_held_across_the_compaction_keeps_its_answer() {
    let storage = FaultStorage::default();
    let filter = Arc::new(DropMarked::default());
    let db = open(&storage, &filter).await;
    put(&db, b"k", b"drop-me").await;
    let snapshot = db.snapshot().expect("snapshot");
    db.flush().await.expect("flushed");
    compact(&db).await;
    let held = snapshot
        .get(KeyspaceId::DEFAULT, b"k", &ReadOptions::default())
        .await
        .expect("read");
    assert_eq!(
        held.as_deref(),
        Some(&b"drop-me"[..]),
        "the snapshot still reads it"
    );
    drop(snapshot);
    compact(&db).await;
    assert_eq!(
        read(&db, b"k").await,
        None,
        "removed once no snapshot reads it"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_filter_is_never_shown_a_delete() {
    let storage = FaultStorage::default();
    let filter = Arc::new(DropMarked::default());
    let db = open(&storage, &filter).await;
    put(&db, b"a", b"keep-a").await;
    db.flush().await.expect("flushed");
    delete(&db, b"a").await;
    put(&db, b"b", b"keep-b").await;
    db.flush().await.expect("flushed");
    compact(&db).await;
    // Relaxed: read after the compaction finished.
    assert_eq!(
        filter.shown.load(Ordering::Relaxed),
        1,
        "only b's value was shown"
    );
    assert_eq!(read(&db, b"a").await, None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_options_record_names_the_filter() {
    let storage = FaultStorage::default();
    let filter = Arc::new(DropMarked::default());
    let db = open(&storage, &filter).await;
    db.close().await.expect("closed");
    let names: Vec<String> = storage
        .list(Path::new(PATH))
        .expect("listable")
        .into_iter()
        .filter(|name| name.starts_with("OPTIONS-"))
        .collect();
    let last = names.iter().max().expect("an options record");
    let text = String::from_utf8(storage.bytes(&Path::new(PATH).join(last))).expect("utf-8");
    assert!(text.contains("drop-marked"), "{text}");
}
