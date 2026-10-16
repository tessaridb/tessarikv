use std::collections::BTreeMap;
use std::sync::Arc;

use crate::{
    Db, DbOptions, Error, KeyspaceId, ReadOptions, SequenceNumber, StorageMode, WriteBatch,
    WriteOptions,
};

async fn memory_db() -> Db {
    Db::open("memory", DbOptions::in_memory()).await.unwrap()
}

fn batch(entries: &[(&[u8], Option<&[u8]>)]) -> WriteBatch {
    let mut batch = WriteBatch::new();
    for (key, value) in entries {
        match value {
            Some(value) => batch.put(KeyspaceId::DEFAULT, key, value).unwrap(),
            None => batch.delete(KeyspaceId::DEFAULT, key).unwrap(),
        }
    }
    batch
}

async fn get(db: &Db, key: &[u8]) -> Option<Vec<u8>> {
    db.get(KeyspaceId::DEFAULT, key, &ReadOptions::default())
        .await
        .unwrap()
}

#[tokio::test]
async fn written_values_are_read_back_and_deletes_hide_them() {
    let db = memory_db().await;
    let write = WriteOptions::default();
    db.write(&batch(&[(b"a", Some(b"1")), (b"b", Some(b"2"))]), &write)
        .await
        .unwrap();
    db.write(&batch(&[(b"a", None), (b"b", Some(b"3"))]), &write)
        .await
        .unwrap();
    assert_eq!(get(&db, b"a").await, None);
    assert_eq!(get(&db, b"b").await, Some(b"3".to_vec()));
    assert_eq!(get(&db, b"c").await, None);
}

#[tokio::test]
async fn each_entry_takes_the_next_sequence_number() {
    let db = memory_db().await;
    let write = WriteOptions::default();
    let first = db
        .write(&batch(&[(b"a", Some(b"1")), (b"b", Some(b"2"))]), &write)
        .await
        .unwrap();
    let second = db
        .write(&batch(&[(b"c", Some(b"3"))]), &write)
        .await
        .unwrap();
    assert_eq!((first.get(), second.get()), (2, 3));
    let empty = db.write(&WriteBatch::new(), &write).await.unwrap();
    assert_eq!(empty, second, "an empty batch takes no sequence number");
}

#[tokio::test]
async fn a_batch_naming_an_unknown_keyspace_is_refused_whole() {
    let db = memory_db().await;
    let mut mixed = batch(&[(b"a", Some(b"1"))]);
    mixed.put(KeyspaceId::new(9), b"b", b"2").unwrap();
    assert!(matches!(
        db.write(&mixed, &WriteOptions::default()).await,
        Err(Error::UnknownKeyspace { keyspace: 9 })
    ));
    assert_eq!(
        get(&db, b"a").await,
        None,
        "no entry of a refused batch is applied"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_database_on_the_file_system_keeps_its_writes_across_a_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let options = DbOptions::builder()
        .storage(StorageMode::Os)
        .build()
        .unwrap();
    let db = Db::open(dir.path(), options.clone()).await.unwrap();
    db.write(&batch(&[(b"k", Some(b"v"))]), &WriteOptions::default())
        .await
        .unwrap();
    assert!(matches!(
        Db::open(dir.path(), options.clone()).await,
        Err(Error::Locked { .. })
    ));
    db.close().await.unwrap();
    let reopened = Db::open(dir.path(), options).await.unwrap();
    assert_eq!(get(&reopened, b"k").await.as_deref(), Some(&b"v"[..]));
    reopened.close().await.unwrap();
}

#[tokio::test]
async fn a_scan_pages_through_live_keys_in_order() {
    let db = memory_db().await;
    let write = WriteOptions::default();
    for key in [b"d", b"a", b"c", b"e", b"b"] {
        db.write(&batch(&[(key, Some(key))]), &write).await.unwrap();
    }
    db.write(&batch(&[(b"c", None)]), &write).await.unwrap();
    let read = ReadOptions::default();
    let page = db
        .scan(KeyspaceId::DEFAULT, b"a", Some(b"e"), 2, &read)
        .await
        .unwrap();
    let keys: Vec<_> = page.entries.iter().map(|(key, _)| key.clone()).collect();
    assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec()]);
    let next = page.next.expect("a full page names where to continue");
    let rest = db
        .scan(KeyspaceId::DEFAULT, &next, Some(b"e"), 10, &read)
        .await
        .unwrap();
    let keys: Vec<_> = rest.entries.iter().map(|(key, _)| key.clone()).collect();
    assert_eq!(
        keys,
        vec![b"d".to_vec()],
        "the deleted key and the excluded end are skipped"
    );
    assert_eq!(rest.next, None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_writers_match_a_model_replayed_in_sequence_order() {
    let db = Arc::new(memory_db().await);
    let tasks = 8_u64;
    let batches = if cfg!(miri) { 4 } else { 300 };
    let handles: Vec<_> = (0..tasks)
        .map(|task| {
            let db = Arc::clone(&db);
            tokio::spawn(async move {
                let mut applied = Vec::new();
                let mut state = (task + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15);
                for _ in 0..batches {
                    let mut entries = Vec::new();
                    for _ in 0..1 + state % 4 {
                        state ^= state << 13;
                        state ^= state >> 7;
                        state ^= state << 17;
                        let key = (state % 64).to_be_bytes().to_vec();
                        let value = (state % 3 != 0).then(|| state.to_be_bytes().to_vec());
                        entries.push((key, value));
                    }
                    let mut write = WriteBatch::new();
                    for (key, value) in &entries {
                        match value {
                            Some(value) => write.put(KeyspaceId::DEFAULT, key, value).unwrap(),
                            None => write.delete(KeyspaceId::DEFAULT, key).unwrap(),
                        }
                    }
                    let last = db.write(&write, &WriteOptions::default()).await.unwrap();
                    applied.push((last.get(), entries));
                }
                applied
            })
        })
        .collect();
    let mut all = Vec::new();
    for handle in handles {
        all.extend(handle.await.unwrap());
    }
    all.sort_by_key(|(last, _)| *last);
    let mut model: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    for (_, entries) in &all {
        for (key, value) in entries {
            match value {
                Some(value) => model.insert(key.clone(), value.clone()),
                None => model.remove(key),
            };
        }
    }
    let page = db
        .scan(
            KeyspaceId::DEFAULT,
            b"",
            None,
            usize::MAX,
            &ReadOptions::default(),
        )
        .await
        .unwrap();
    let scanned: BTreeMap<_, _> = page.entries.into_iter().collect();
    assert_eq!(scanned, model);
    for key in model.keys() {
        assert_eq!(get(&db, key).await.as_ref(), model.get(key));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_scan_sees_each_batch_whole() {
    let db = Arc::new(memory_db().await);
    let writer = {
        let db = Arc::clone(&db);
        tokio::spawn(async move {
            for round in 0..if cfg!(miri) { 5_u64 } else { 2000 } {
                let value = round.to_be_bytes();
                db.write(
                    &batch(&[(b"left", Some(&value)), (b"right", Some(&value))]),
                    &WriteOptions::default(),
                )
                .await
                .unwrap();
            }
        })
    };
    while !writer.is_finished() {
        let page = db
            .scan(KeyspaceId::DEFAULT, b"", None, 10, &ReadOptions::default())
            .await
            .unwrap();
        if let [(_, left), (_, right)] = &page.entries[..] {
            assert_eq!(left, right, "a scan saw half a batch");
        }
        tokio::task::yield_now().await;
    }
    writer.await.unwrap();
    let _ = SequenceNumber::MAX;
}

/// The oldest snapshot still held is what keeps old versions from compaction: it follows the
/// holders as they come and go, transactions included.
#[tokio::test]
async fn the_oldest_held_snapshot_follows_its_holders() {
    let db = Db::open("horizon", DbOptions::in_memory()).await.unwrap();
    assert_eq!(db.oldest_snapshot(), None);
    let mut batch = WriteBatch::new();
    batch.put(KeyspaceId::DEFAULT, b"k", b"1").unwrap();
    db.write(&batch, &WriteOptions::default()).await.unwrap();
    let first = db.snapshot().unwrap();
    db.write(&batch, &WriteOptions::default()).await.unwrap();
    let txn = db.begin().unwrap();
    assert_eq!(db.oldest_snapshot(), Some(first.sequence()));
    drop(first);
    assert_eq!(db.oldest_snapshot(), Some(txn.sequence()));
    drop(txn);
    assert_eq!(db.oldest_snapshot(), None);
}
