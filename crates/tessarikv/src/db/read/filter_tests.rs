use std::sync::Arc;

use crate::log::fault::FaultStorage;
use crate::{
    Db, DbOptions, FilterPolicy, KeyspaceId, KeyspaceOptions, ReadOptions, WriteBatch, WriteOptions,
};

const KEYS: u32 = 20_000;

fn key(index: u32) -> Vec<u8> {
    format!("key-{index:06}").into_bytes()
}

async fn write(db: &Db, edit: impl Fn(&mut WriteBatch, u32)) {
    let mut batch = WriteBatch::new();
    for index in 0..KEYS {
        edit(&mut batch, index);
    }
    db.write(&batch, &WriteOptions::default())
        .await
        .expect("written");
    db.flush().await.expect("flushed");
}

async fn check(db: &Db) {
    for index in 0..KEYS {
        let read = db
            .get(KeyspaceId::DEFAULT, &key(index), &ReadOptions::default())
            .await
            .expect("read");
        let expected = (index % 10 != 0).then(|| b"value".to_vec());
        assert_eq!(read, expected, "key {index}");
        let absent = format!("absent-{index}").into_bytes();
        let read = db
            .get(KeyspaceId::DEFAULT, &absent, &ReadOptions::default())
            .await
            .expect("read");
        assert_eq!(read, None, "absent {index}");
    }
}

/// Puts every key into one table and deletes every tenth in a newer one, reads both before and
/// after compaction, and returns the bytes the keyspace's tables take once compacted.
async fn table_bytes(policy: FilterPolicy) -> u64 {
    let keyspace = KeyspaceOptions::builder()
        .filter_policy(policy)
        .disable_auto_compactions(true)
        .build()
        .expect("valid");
    let options = DbOptions::builder()
        .default_keyspace(keyspace)
        .build()
        .expect("valid");
    let storage = Arc::new(FaultStorage::default());
    let db = Db::open_with_storage("/db", options, storage)
        .await
        .expect("opened");
    write(&db, |batch, index| {
        batch
            .put(KeyspaceId::DEFAULT, &key(index), b"value")
            .expect("entry");
    })
    .await;
    write(&db, |batch, index| {
        if index % 10 == 0 {
            batch
                .delete(KeyspaceId::DEFAULT, &key(index))
                .expect("entry");
        }
    })
    .await;
    // The deletes sit in the newer table: its filter must hold them, or the older put shows.
    check(&db).await;
    db.compact_range(KeyspaceId::DEFAULT, b"", None)
        .await
        .expect("compacted");
    check(&db).await;
    let bytes = db
        .properties(KeyspaceId::DEFAULT)
        .expect("held")
        .bytes_per_level
        .iter()
        .sum();
    db.close().await.expect("closed");
    bytes
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_ribbon_keyspace_reads_as_a_bloom_one_in_fewer_bytes() {
    let bloom = table_bytes(FilterPolicy::Bloom).await;
    let ribbon = table_bytes(FilterPolicy::Ribbon).await;
    // 18 000 keys at ten bits per key: the bloom filter takes 22 500 bytes and the ribbon about
    // 17 000, so the ribbon keyspace's tables are some 5 000 bytes smaller.
    assert!(
        ribbon.saturating_add(4_000) < bloom,
        "ribbon tables {ribbon} bytes, bloom tables {bloom}"
    );
}
