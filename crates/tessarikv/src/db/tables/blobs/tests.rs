//! Large values in blob files: every read path reads them back, values below the threshold stay
//! in the tables, compaction carries indexes without rewriting values and empties the oldest blob
//! files, and a blob file outlives the last reader that can reach it.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::db::Db;
use crate::db::flush_tests::{PATH, files_on_storage, key, until, value};
use crate::log::fault::FaultStorage;
use crate::{
    CompactionFilter, DbOptions, FilterDecision, KeyspaceId, KeyspaceOptions,
    KeyspaceOptionsBuilder, ReadOptions, WriteBatch, WriteOptions,
};

const KEYS: u32 = 40;
const DEFAULT: KeyspaceId = KeyspaceId::DEFAULT;

/// Values of 1 KiB and more in blob files; no compaction of its own.
pub(super) fn keyspace() -> KeyspaceOptionsBuilder {
    KeyspaceOptions::builder()
        .memtable_bytes(1024 * 1024)
        .disable_auto_compactions(true)
        .blob_threshold(1024)
}

pub(super) async fn open(storage: &FaultStorage, keyspace: KeyspaceOptionsBuilder) -> Db {
    let options = DbOptions::builder()
        .default_keyspace(keyspace.build().expect("valid keyspace"))
        .build()
        .expect("valid options");
    Db::open_with_storage(PATH, options, Arc::new(storage.clone()))
        .await
        .expect("opened")
}

/// The value written to `key(index)` in `round`: large for even indexes, small for odd ones.
pub(super) fn written(index: u32, round: u32) -> Vec<u8> {
    let mut written = if index.is_multiple_of(2) {
        value(index)
    } else {
        b"small".to_vec()
    };
    written.extend_from_slice(&round.to_le_bytes());
    written
}

/// Writes every key in `round`, then flushes.
pub(super) async fn fill(db: &Db, round: u32) {
    let mut batch = WriteBatch::new();
    for index in 0..KEYS {
        batch
            .put(DEFAULT, &key(index), &written(index, round))
            .expect("entry");
    }
    db.write(&batch, &WriteOptions::default())
        .await
        .expect("written");
    db.flush().await.expect("flushed");
}

fn expected(round: u32) -> Vec<(Vec<u8>, Vec<u8>)> {
    (0..KEYS)
        .map(|index| (key(index), written(index, round)))
        .collect()
}

/// What `get`, a scan, a forward and a backward walk, a snapshot and a transaction read.
pub(super) async fn read_everywhere(db: &Db, round: u32) {
    let options = ReadOptions::default();
    let want = expected(round);
    for (key, value) in &want {
        let read = db.get(DEFAULT, key, &options).await.expect("read");
        assert_eq!(read.as_deref(), Some(value.as_slice()), "get {key:?}");
    }
    let page = db
        .scan(DEFAULT, b"", None, usize::MAX, &options)
        .await
        .expect("scanned");
    assert_eq!(page.entries, want, "scan");
    let mut iterator = db.iter(DEFAULT, &options).expect("iterator");
    let mut forward = Vec::new();
    iterator.seek_to_first().await.expect("moved");
    while iterator.valid() {
        forward.push((iterator.key().to_vec(), iterator.value().to_vec()));
        iterator.next().await.expect("moved");
    }
    assert_eq!(forward, want, "forward walk");
    let mut backward = Vec::new();
    iterator.seek_to_last().await.expect("moved");
    while iterator.valid() {
        backward.push((iterator.key().to_vec(), iterator.value().to_vec()));
        iterator.prev().await.expect("moved");
    }
    backward.reverse();
    assert_eq!(backward, want, "backward walk");
    let snapshot = db.snapshot().expect("snapshot");
    let (first, value) = want.first().expect("a key");
    let read = snapshot.get(DEFAULT, first, &options).await.expect("read");
    assert_eq!(read.as_ref(), Some(value), "snapshot get");
    let mut transaction = db.begin().expect("transaction");
    let read = transaction
        .get(DEFAULT, first, &options)
        .await
        .expect("read");
    assert_eq!(read.as_ref(), Some(value), "transaction get");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_large_value_reads_back_from_its_blob_file_on_every_read_path() {
    let storage = FaultStorage::default();
    let db = open(&storage, keyspace()).await;
    fill(&db, 0).await;
    assert_eq!(files_on_storage(&storage, ".blob"), 1, "one blob file");
    // The table keeps indexes: it is far smaller than the 20 values of 4 KiB it reaches.
    let table: u64 = db.view.load().files(0).map(|file| file.meta.size).sum();
    assert!(table < 20 * 1024, "the table holds {table} bytes");
    read_everywhere(&db, 0).await;
    db.close().await.expect("closed");
    let db = open(&storage, keyspace()).await;
    read_everywhere(&db, 0).await;
    db.close().await.expect("closed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn values_below_the_threshold_and_without_one_stay_in_the_table() {
    for keyspace in [
        keyspace().blob_threshold(64 * 1024),
        KeyspaceOptions::builder().memtable_bytes(1024 * 1024),
    ] {
        let storage = FaultStorage::default();
        let db = open(&storage, keyspace).await;
        fill(&db, 0).await;
        assert_eq!(files_on_storage(&storage, ".blob"), 0, "no blob file");
        read_everywhere(&db, 0).await;
        db.close().await.expect("closed");
    }
}

/// Every blob file the current view's tables hold.
fn held_blobs(db: &Db) -> Vec<u64> {
    let mut held: Vec<u64> = db
        .view
        .load()
        .files(0)
        .flat_map(|file| file.blobs.iter().map(|blob| blob.id()).collect::<Vec<_>>())
        .collect();
    held.sort_unstable();
    held.dedup();
    held
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_compaction_carries_indexes_and_writes_no_value_again() {
    let storage = FaultStorage::default();
    let db = open(&storage, keyspace().blob_gc_age_cutoff_percent(0)).await;
    fill(&db, 0).await;
    fill(&db, 1).await;
    let before = held_blobs(&db);
    db.compact_range(DEFAULT, b"", None)
        .await
        .expect("compacted");
    // The overwritten round's values are dead in its blob file, which the live ones keep.
    assert_eq!(held_blobs(&db), before.get(1..).unwrap_or_default());
    let table: u64 = db.view.load().files(0).map(|file| file.meta.size).sum();
    assert!(table < 20 * 1024, "compacted tables hold {table} bytes");
    read_everywhere(&db, 1).await;
    db.close().await.expect("closed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_compaction_empties_the_oldest_blob_files_and_removes_them_after_their_readers() {
    let storage = FaultStorage::default();
    let db = open(&storage, keyspace().blob_gc_age_cutoff_percent(100)).await;
    // Two overlapping tables, so the compaction merges rather than moves.
    fill(&db, 0).await;
    fill(&db, 0).await;
    let old = held_blobs(&db);
    // A walk of the view before the compaction reads the old blob file to its end.
    let mut held = db.iter(DEFAULT, &ReadOptions::default()).expect("iterator");
    held.seek_to_first().await.expect("moved");
    db.compact_range(DEFAULT, b"", None)
        .await
        .expect("compacted");
    let new = held_blobs(&db);
    assert!(
        new.iter().all(|id| !old.contains(id)),
        "{old:?} → {new:?}: every value moved"
    );
    assert_eq!(
        files_on_storage(&storage, ".blob"),
        old.len() + new.len(),
        "the old files are still read"
    );
    let mut walked = Vec::new();
    while held.valid() {
        walked.push((held.key().to_vec(), held.value().to_vec()));
        held.next().await.expect("moved");
    }
    assert_eq!(walked, expected(0), "the held walk");
    drop(held);
    until("the emptied blob files are removed", || {
        files_on_storage(&storage, ".blob") == new.len()
    })
    .await;
    read_everywhere(&db, 0).await;
    db.close().await.expect("closed");
}

/// Counts what it is shown and removes values ending in round 1's stamp.
#[derive(Default)]
struct RemoveRoundOne {
    shown_large: AtomicU64,
}

impl CompactionFilter for RemoveRoundOne {
    fn name(&self) -> &str {
        "remove-round-one"
    }

    fn filter(&self, _key: &[u8], value: &[u8]) -> FilterDecision {
        if value.len() >= 1024 {
            // Relaxed: a test counter, read after the compaction returned.
            self.shown_large.fetch_add(1, Ordering::Relaxed);
        }
        if value.ends_with(&1_u32.to_le_bytes()) {
            FilterDecision::Remove
        } else {
            FilterDecision::Keep
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_compaction_filter_is_shown_the_value_in_the_blob_file() {
    let filter = Arc::new(RemoveRoundOne::default());
    let storage = FaultStorage::default();
    let keyspace = keyspace()
        .blob_gc_age_cutoff_percent(0)
        .compaction_filter(Arc::clone(&filter) as Arc<dyn CompactionFilter>);
    let db = open(&storage, keyspace).await;
    fill(&db, 1).await;
    fill(&db, 1).await;
    db.compact_range(DEFAULT, b"", None)
        .await
        .expect("compacted");
    // Relaxed: read after the compaction returned.
    assert_eq!(
        filter.shown_large.load(Ordering::Relaxed),
        u64::from(KEYS / 2)
    );
    for index in 0..KEYS {
        let read = db
            .get(DEFAULT, &key(index), &ReadOptions::default())
            .await
            .expect("read");
        assert_eq!(read, None, "key {index} was removed");
    }
    db.close().await.expect("closed");
}
