//! A first-in-first-out keyspace: tables stay in level 0, the oldest go once the tables hold
//! more than the limit, and a key reads its newest kept value or nothing.

use std::collections::BTreeMap;
use std::sync::Arc;

use super::Db;
use super::compaction_model_tests::value;
use super::flush_tests::{PATH, files_on_storage, key, until};
use crate::log::fault::FaultStorage;
use crate::{
    CompactionStyle, DbOptions, KeyspaceId, KeyspaceOptions, ReadOptions, WriteBatch, WriteOptions,
};

const LIMIT: u64 = 1 << 20;
const KEYS: u32 = 2000;
const WRITES: u32 = 3000;

fn options() -> DbOptions {
    let keyspace = KeyspaceOptions::builder()
        .memtable_bytes(64 * 1024)
        // Low enough that leveled stalls would hold writes for good: FIFO ignores them.
        .level0_compaction_trigger(2)
        .level0_slowdown_trigger(3)
        .level0_stop_trigger(4)
        .compaction(CompactionStyle::Fifo {
            max_table_bytes: LIMIT,
        })
        .build()
        .expect("valid");
    DbOptions::builder()
        .default_keyspace(keyspace)
        .wal_segment_bytes(1024 * 1024)
        .build()
        .expect("valid")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_oldest_tables_go_and_every_key_reads_its_newest_value_or_nothing() {
    let storage = FaultStorage::default();
    let db = Db::open_with_storage(PATH, options(), Arc::new(storage.clone()))
        .await
        .expect("opened");
    let mut model = BTreeMap::new();
    for index in 0..WRITES {
        let mut batch = WriteBatch::new();
        batch
            .put(KeyspaceId::DEFAULT, &key(index % KEYS), &value(index))
            .expect("small entry");
        db.write(&batch, &WriteOptions::default())
            .await
            .expect("written");
        model.insert(index % KEYS, value(index));
    }
    let held = || {
        db.view
            .load()
            .files(0)
            .map(|file| file.meta.size)
            .sum::<u64>()
    };
    until("the tables fit their limit", || held() <= LIMIT).await;
    assert!(
        db.view.load().files(0).all(|file| file.meta.level == 0),
        "nothing is merged below level 0"
    );
    let mut absent = 0;
    for index in 0..KEYS {
        let read = db
            .get(KeyspaceId::DEFAULT, &key(index), &ReadOptions::default())
            .await
            .expect("readable");
        match read {
            Some(found) => assert_eq!(Some(&found), model.get(&index), "key {index}"),
            None => absent += 1,
        }
    }
    assert!(absent > 0, "the oldest writes were dropped");
    for index in (WRITES - 50)..WRITES {
        let read = db
            .get(
                KeyspaceId::DEFAULT,
                &key(index % KEYS),
                &ReadOptions::default(),
            )
            .await
            .expect("readable");
        assert_eq!(read, Some(value(index)), "the newest writes are kept");
    }
    until("dropped table files removed from storage", || {
        files_on_storage(&storage, ".sst") == db.view.load().files(0).count()
    })
    .await;
    db.close().await.expect("closed");
}
