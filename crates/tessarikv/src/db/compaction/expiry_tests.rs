//! Expiring writes: a put stamped with an expiry time reads as written until then and as
//! absent from then on, wherever it is held — an in-memory table, a table file, after a
//! compaction, after a crash — and an expired put hides the older versions of its key like a
//! delete.

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use crate::db::Db;
use crate::db::flush_tests::PATH;
use crate::db::manual_compaction_tests::stored;
use crate::log::fault::FaultStorage;
use crate::{DbOptions, KeyspaceId, ReadOptions, WriteBatch, WriteOptions};

const LIVE: &[u8] = b"live";
const GONE: &[u8] = b"gone";
const OLDER: &[u8] = b"older";

fn past() -> SystemTime {
    SystemTime::now() - Duration::from_secs(60)
}

fn future() -> SystemTime {
    SystemTime::now() + Duration::from_secs(3600)
}

async fn open(storage: &FaultStorage) -> Db {
    Db::open_with_storage(PATH, DbOptions::default(), Arc::new(storage.clone()))
        .await
        .expect("opened")
}

async fn put_expiring(db: &Db, key: &[u8], value: &[u8], expires_at: SystemTime) {
    let mut batch = WriteBatch::new();
    batch
        .put_expiring(KeyspaceId::DEFAULT, key, value, expires_at)
        .expect("entry");
    db.write(&batch, &WriteOptions::default())
        .await
        .expect("written");
}

async fn put(db: &Db, key: &[u8], value: &[u8]) {
    let mut batch = WriteBatch::new();
    batch.put(KeyspaceId::DEFAULT, key, value).expect("entry");
    db.write(&batch, &WriteOptions::default())
        .await
        .expect("written");
}

/// What every read path answers for `key`: get, multi_get, a scan, a forward and a backward
/// iterator.
async fn every_read(db: &Db, key: &[u8]) -> [Option<Vec<u8>>; 5] {
    let read = ReadOptions::default();
    let get = db.get(KeyspaceId::DEFAULT, key, &read).await.expect("get");
    let multi = db
        .multi_get(KeyspaceId::DEFAULT, &[key], &read)
        .await
        .expect("multi_get")
        .pop()
        .expect("one answer");
    let page = db
        .scan(KeyspaceId::DEFAULT, b"", None, 100, &read)
        .await
        .expect("scan");
    let scanned = page
        .entries
        .into_iter()
        .find(|(found, _)| found == key)
        .map(|(_, value)| value);
    let mut forward = db.iter(KeyspaceId::DEFAULT, &read).expect("iterator");
    forward.seek(key).await.expect("seek");
    let ahead = (forward.valid() && forward.key() == key).then(|| forward.value().to_vec());
    let mut backward = db.iter(KeyspaceId::DEFAULT, &read).expect("iterator");
    backward.seek_for_prev(key).await.expect("seek");
    let behind = (backward.valid() && backward.key() == key).then(|| backward.value().to_vec());
    [get, multi, scanned, ahead, behind]
}

async fn assert_reads(db: &Db, key: &[u8], expected: Option<&[u8]>, at: &str) {
    for (path, answer) in ["get", "multi_get", "scan", "next", "prev"]
        .iter()
        .zip(every_read(db, key).await)
    {
        assert_eq!(answer.as_deref(), expected, "{at}: {path} of {key:?}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_expired_put_reads_absent_and_a_live_one_reads_as_written_on_every_path() {
    let storage = FaultStorage::default();
    let db = open(&storage).await;
    put_expiring(&db, LIVE, b"value-live", future()).await;
    put_expiring(&db, GONE, b"value-gone", past()).await;
    assert_reads(&db, LIVE, Some(b"value-live"), "in memory").await;
    assert_reads(&db, GONE, None, "in memory").await;
    db.flush().await.expect("flushed");
    assert_reads(&db, LIVE, Some(b"value-live"), "in a table").await;
    assert_reads(&db, GONE, None, "in a table").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_expired_put_hides_the_older_value_before_and_after_compaction() {
    let storage = FaultStorage::default();
    let db = open(&storage).await;
    put(&db, OLDER, b"first").await;
    db.flush().await.expect("flushed");
    put_expiring(&db, OLDER, b"second", past()).await;
    assert_reads(&db, OLDER, None, "in memory over a table").await;
    db.flush().await.expect("flushed");
    assert_reads(&db, OLDER, None, "two tables").await;
    db.compact_range(KeyspaceId::DEFAULT, b"", None)
        .await
        .expect("compacted");
    assert_reads(&db, OLDER, None, "compacted").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compaction_removes_an_expired_put_and_keeps_a_live_one() {
    let storage = FaultStorage::default();
    let db = open(&storage).await;
    put(&db, OLDER, b"first").await;
    db.flush().await.expect("flushed");
    put_expiring(&db, OLDER, b"second", past()).await;
    put_expiring(&db, GONE, b"value-gone", past()).await;
    put_expiring(&db, LIVE, b"value-live", future()).await;
    db.flush().await.expect("flushed");
    db.compact_range(KeyspaceId::DEFAULT, b"", None)
        .await
        .expect("compacted");
    let (versions, _) = stored(&db).await;
    let kept: Vec<&[u8]> = versions.keys().map(Vec::as_slice).collect();
    assert_eq!(kept, vec![LIVE], "only the live put is left in any table");
    assert_reads(&db, LIVE, Some(b"value-live"), "compacted").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn expiry_holds_after_a_crash_and_after_a_compacted_reopen() {
    let storage = FaultStorage::default();
    let db = open(&storage).await;
    put(&db, OLDER, b"first").await;
    db.flush().await.expect("flushed");
    put_expiring(&db, OLDER, b"second", past()).await;
    put_expiring(&db, LIVE, b"value-live", future()).await;
    let crashed = storage.crash(Path::new(PATH), true);
    let reopened = open(&crashed).await;
    assert_reads(&reopened, OLDER, None, "replayed from the log").await;
    assert_reads(
        &reopened,
        LIVE,
        Some(b"value-live"),
        "replayed from the log",
    )
    .await;
    reopened.flush().await.expect("flushed");
    reopened
        .compact_range(KeyspaceId::DEFAULT, b"", None)
        .await
        .expect("compacted");
    let crashed = crashed.crash(Path::new(PATH), true);
    let reopened = open(&crashed).await;
    assert_reads(&reopened, OLDER, None, "compacted, crashed").await;
    assert_reads(&reopened, LIVE, Some(b"value-live"), "compacted, crashed").await;
}

#[test]
fn an_expiring_put_refuses_a_value_without_room_for_its_stamp() {
    let mut batch = WriteBatch::new();
    let largest = vec![0_u8; tessarikv_constants::MAX_VALUE_BYTES - 7];
    let refused = batch.put_expiring(KeyspaceId::DEFAULT, b"k", &largest, future());
    assert!(
        matches!(
            refused,
            Err(tessarikv_format::Error::TooLarge {
                what: "expiring value",
                ..
            })
        ),
        "{refused:?}"
    );
    assert!(batch.is_empty(), "nothing refused was added");
    let fits = vec![0_u8; tessarikv_constants::MAX_VALUE_BYTES - 8];
    batch
        .put_expiring(KeyspaceId::DEFAULT, b"k", &fits, future())
        .expect("the largest value that leaves room for the stamp");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_put_expiring_before_the_epoch_has_already_expired() {
    let storage = FaultStorage::default();
    let db = open(&storage).await;
    put_expiring(
        &db,
        GONE,
        b"value",
        SystemTime::UNIX_EPOCH - Duration::from_secs(1),
    )
    .await;
    assert_reads(&db, GONE, None, "in memory").await;
}
