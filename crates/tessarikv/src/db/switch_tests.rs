use std::collections::BTreeMap;
use std::sync::Arc;

use crate::{Db, DbOptions, KeyspaceId, ReadOptions, WriteBatch, WriteOptions};

async fn memory_db() -> Db {
    Db::open("memory", DbOptions::in_memory()).await.unwrap()
}

async fn put(db: &Db, key: &[u8], value: Option<&[u8]>) -> u64 {
    let mut batch = WriteBatch::new();
    match value {
        Some(value) => batch.put(KeyspaceId::DEFAULT, key, value).unwrap(),
        None => batch.delete(KeyspaceId::DEFAULT, key).unwrap(),
    }
    db.write(&batch, &WriteOptions::default())
        .await
        .unwrap()
        .get()
}

async fn get(db: &Db, key: &[u8]) -> Option<Vec<u8>> {
    db.get(KeyspaceId::DEFAULT, key, &ReadOptions::default())
        .await
        .unwrap()
}

async fn scan(db: &Db) -> Vec<(Vec<u8>, Vec<u8>)> {
    db.scan(KeyspaceId::DEFAULT, b"", None, 100, &ReadOptions::default())
        .await
        .unwrap()
        .entries
}

/// Every sequence each in-memory table holds, keyed by the table's epoch.
fn sequences_by_epoch(db: &Db) -> BTreeMap<u8, Vec<u64>> {
    let view = db.view.load();
    view.epoch_tables()
        .map(|(epoch, table)| {
            let mut versions = table.iter();
            versions.seek_to_first();
            let mut sequences = Vec::new();
            while let Some(entry) = versions.entry() {
                sequences.push(entry.sequence.get());
                versions.next();
            }
            (epoch, sequences)
        })
        .collect()
}

#[tokio::test]
async fn reads_see_every_table_and_the_newest_version_wins() {
    let db = memory_db().await;
    put(&db, b"a", Some(b"old")).await;
    put(&db, b"b", Some(b"kept")).await;
    put(&db, b"c", Some(b"doomed")).await;
    let switch = db.switch_memtable().expect("no switch in progress");
    put(&db, b"a", Some(b"new")).await;
    put(&db, b"c", None).await;
    put(&db, b"d", Some(b"fresh")).await;
    assert_eq!((switch.epoch(), switch.boundary()), (1, 4));
    assert_eq!(get(&db, b"a").await, Some(b"new".to_vec()));
    assert_eq!(get(&db, b"b").await, Some(b"kept".to_vec()));
    assert_eq!(
        get(&db, b"c").await,
        None,
        "a delete in the newer table hides the older value"
    );
    assert_eq!(
        scan(&db).await,
        vec![
            (b"a".to_vec(), b"new".to_vec()),
            (b"b".to_vec(), b"kept".to_vec()),
            (b"d".to_vec(), b"fresh".to_vec()),
        ]
    );
    assert_eq!(
        sequences_by_epoch(&db),
        BTreeMap::from([(0, vec![1, 2, 3]), (1, vec![4, 5, 6])]),
        "each table holds exactly its epoch's sequences"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_writes_land_in_the_table_of_their_epoch() {
    const WRITERS: u64 = 4;
    const WRITES: u64 = 2_000;
    let db = Arc::new(memory_db().await);
    let writers: Vec<_> = (0..WRITERS)
        .map(|writer| {
            let db = Arc::clone(&db);
            tokio::spawn(async move {
                for index in 0..WRITES {
                    let key = format!("{writer}-{index:05}");
                    put(&db, key.as_bytes(), Some(b"v")).await;
                }
            })
        })
        .collect();
    let mut boundaries = BTreeMap::new();
    for _ in 0..40 {
        if let Some(switch) = db.switch_memtable() {
            boundaries.insert(switch.epoch(), switch.boundary());
        }
        tokio::task::yield_now().await;
    }
    for writer in writers {
        writer.await.unwrap();
    }
    assert!(
        boundaries.len() > 1,
        "the switches ran while writes were in flight"
    );
    let by_epoch = sequences_by_epoch(&db);
    let total: usize = by_epoch.values().map(Vec::len).sum();
    assert_eq!(
        total,
        usize::try_from(WRITERS * WRITES).unwrap(),
        "no write lost or doubled"
    );
    for (epoch, sequences) in &by_epoch {
        let low = boundaries.get(epoch).copied().unwrap_or(1);
        let high = boundaries
            .range(epoch + 1..)
            .next()
            .map_or(u64::MAX, |(_, boundary)| *boundary);
        for sequence in sequences {
            assert!(
                (low..high).contains(sequence),
                "sequence {sequence} in epoch {epoch}, whose range is {low}..{high}"
            );
        }
    }
    for writer in 0..WRITERS {
        for index in (0..WRITES).step_by(97) {
            let key = format!("{writer}-{index:05}");
            assert_eq!(get(&db, key.as_bytes()).await, Some(b"v".to_vec()));
        }
    }
}

#[tokio::test]
async fn a_snapshot_reads_the_same_values_after_later_writes_and_switches() {
    let db = memory_db().await;
    put(&db, b"a", Some(b"1")).await;
    put(&db, b"b", Some(b"2")).await;
    let snapshot = db.snapshot().unwrap();
    put(&db, b"a", Some(b"changed")).await;
    put(&db, b"b", None).await;
    let _ = db.switch_memtable().expect("no switch in progress");
    put(&db, b"c", Some(b"3")).await;
    let read = ReadOptions::default();
    assert_eq!(snapshot.sequence().get(), 2);
    assert_eq!(
        snapshot
            .get(KeyspaceId::DEFAULT, b"a", &read)
            .await
            .unwrap(),
        Some(b"1".to_vec())
    );
    assert_eq!(
        snapshot
            .get(KeyspaceId::DEFAULT, b"b", &read)
            .await
            .unwrap(),
        Some(b"2".to_vec())
    );
    let page = snapshot
        .scan(KeyspaceId::DEFAULT, b"", None, 10, &read)
        .await
        .unwrap();
    assert_eq!(
        page.entries,
        vec![
            (b"a".to_vec(), b"1".to_vec()),
            (b"b".to_vec(), b"2".to_vec())
        ]
    );
    assert_eq!(get(&db, b"a").await, Some(b"changed".to_vec()));
    assert_eq!(db.snapshots.horizon(9).snapshots(), &[2]);
    drop(snapshot);
    assert!(
        db.snapshots.horizon(9).snapshots().is_empty(),
        "a dropped snapshot is released"
    );
}
