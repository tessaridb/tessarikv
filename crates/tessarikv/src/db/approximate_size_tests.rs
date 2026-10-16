//! Approximate sizes of key ranges, from table metadata and indexes.

use std::sync::Arc;

use super::Db;
use super::flush_tests::{PATH, key, value};
use crate::log::fault::FaultStorage;
use crate::{DbOptions, KeyspaceId, KeyspaceOptions, WriteBatch, WriteOptions};

const KS: KeyspaceId = KeyspaceId::DEFAULT;
const KEYS: u32 = 400;

/// Keys `0..KEYS` in small tables at the bottom level.
async fn filled() -> Db {
    let keyspace = KeyspaceOptions::builder()
        .memtable_bytes(64 * 1024)
        .target_file_bytes(16 * 1024)
        .build()
        .expect("valid");
    let options = DbOptions::builder()
        .default_keyspace(keyspace)
        .wal_segment_bytes(1024 * 1024)
        .build()
        .expect("valid");
    let db = Db::open_with_storage(PATH, options, Arc::new(FaultStorage::default()))
        .await
        .expect("opened");
    for index in 0..KEYS {
        let mut batch = WriteBatch::new();
        batch.put(KS, &key(index), &value(index)).expect("entry");
        db.write(&batch, &WriteOptions::default())
            .await
            .expect("written");
    }
    db.compact_range(KS, b"", None).await.expect("compacted");
    db
}

fn size(db: &Db, start: &[u8], end: Option<&[u8]>) -> u64 {
    db.approximate_size(KS, start, end).expect("estimated")
}

/// The whole keyspace is the bytes of its table files; part of it is about its share of them.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_range_is_about_its_share_of_the_table_files() {
    let db = filled().await;
    let files: u64 = db.view.load().files(0).map(|file| file.meta.size).sum();
    assert!(db.view.load().files(0).count() > 2, "several tables");
    assert_eq!(size(&db, b"", None), files);
    let half = size(&db, &key(0), Some(&key(KEYS / 2)));
    assert!(
        half > files / 4 && half < files * 3 / 4,
        "half the keys hold about half the bytes: {half} of {files}"
    );
    let quarter = size(&db, &key(KEYS / 4), Some(&key(KEYS / 2)));
    assert!(quarter < half, "a narrower range holds fewer bytes");
    let tail = size(&db, &key(KEYS / 2), None);
    assert!(
        half.abs_diff(tail) < files / 4,
        "the two halves are alike: {half} and {tail}"
    );
}

/// Ranges holding no key hold no bytes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_range_holding_no_key_is_empty() {
    let db = filled().await;
    assert_eq!(size(&db, &key(10), Some(&key(10))), 0);
    assert_eq!(size(&db, b"zzz", None), 0);
    assert_eq!(size(&db, b"", Some(b"a")), 0);
}

/// In memory there are no table files, and an unknown keyspace is refused.
#[tokio::test]
async fn in_memory_nothing_is_on_files_and_unknown_keyspaces_are_refused() {
    let db = Db::open("memory", DbOptions::in_memory())
        .await
        .expect("opened");
    let mut batch = WriteBatch::new();
    batch.put(KS, b"k", b"v").expect("entry");
    db.write(&batch, &WriteOptions::default())
        .await
        .expect("written");
    assert_eq!(size(&db, b"", None), 0);
    let error = db
        .approximate_size(KeyspaceId::new(7), b"", None)
        .expect_err("refused");
    assert_eq!(error.code(), "unknown_keyspace");
}
