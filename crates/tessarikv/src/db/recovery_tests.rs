use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use crate::log::fault::FaultStorage;
use crate::log::segment_path;
use crate::{
    Db, DbOptions, Durability, Error, KeyspaceId, ReadOptions, WalRecovery, WriteBatch,
    WriteOptions,
};

const PATH: &str = "/db";

fn options() -> DbOptions {
    DbOptions::default()
}

async fn open(storage: &FaultStorage, options: DbOptions) -> crate::Result<Db> {
    Db::open_with_storage(PATH, options, Arc::new(storage.clone())).await
}

fn write_options(durability: Durability) -> WriteOptions {
    WriteOptions {
        durability,
        ..WriteOptions::default()
    }
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn synced_writes_survive_a_power_loss_and_buffered_ones_a_process_crash() {
    let storage = FaultStorage::default();
    let db = open(&storage, options()).await.expect("opened");
    db.write(&put("synced", "1"), &write_options(Durability::Sync))
        .await
        .expect("written");
    db.write(&put("buffered", "2"), &write_options(Durability::Buffered))
        .await
        .expect("written");
    db.write(&put("memory", "3"), &write_options(Durability::NoWal))
        .await
        .expect("written");

    let power_loss = storage.crash(Path::new(PATH), true);
    let reopened = open(&power_loss, options()).await.expect("recovered");
    assert_eq!(get(&reopened, "synced").await.as_deref(), Some("1"));
    assert_eq!(get(&reopened, "memory").await, None);

    let process_crash = storage.crash(Path::new(PATH), false);
    let reopened = open(&process_crash, options()).await.expect("recovered");
    assert_eq!(get(&reopened, "synced").await.as_deref(), Some("1"));
    assert_eq!(get(&reopened, "buffered").await.as_deref(), Some("2"));
    assert_eq!(get(&reopened, "memory").await, None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_synced_writers_are_all_recovered_in_commit_order() {
    let storage = FaultStorage::default();
    let db = Arc::new(open(&storage, options()).await.expect("opened"));
    let mut tasks = Vec::new();
    for writer in 0..8 {
        let db = Arc::clone(&db);
        tasks.push(tokio::spawn(async move {
            let mut acknowledged = Vec::new();
            for round in 0..25 {
                let key = format!("k{}", (writer * 7 + round) % 30);
                let value = format!("{writer}-{round}");
                let last = db
                    .write(&put(&key, &value), &write_options(Durability::Sync))
                    .await
                    .expect("written");
                acknowledged.push((last.get(), key, value));
            }
            acknowledged
        }));
    }
    let mut acknowledged = Vec::new();
    for task in tasks {
        acknowledged.extend(task.await.expect("joined"));
    }
    acknowledged.sort();
    let mut model = BTreeMap::new();
    for (_, key, value) in acknowledged {
        model.insert(key, value);
    }

    let reopened = open(&storage.crash(Path::new(PATH), true), options())
        .await
        .expect("recovered");
    for (key, value) in &model {
        assert_eq!(get(&reopened, key).await.as_ref(), Some(value), "{key}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_torn_tail_is_cut_off_and_the_log_continues_after_it() {
    let storage = FaultStorage::default();
    let db = open(&storage, options()).await.expect("opened");
    db.write(&put("before", "1"), &write_options(Durability::Sync))
        .await
        .expect("written");
    let image = storage.crash(Path::new(PATH), true);
    image.append_raw(&segment_path(Path::new(PATH), 1), &[0xA5; 11]);

    let reopened = open(&image, options()).await.expect("recovered");
    assert_eq!(get(&reopened, "before").await.as_deref(), Some("1"));
    reopened
        .write(&put("after", "2"), &write_options(Durability::Sync))
        .await
        .expect("written");

    let again = open(&image.crash(Path::new(PATH), true), options())
        .await
        .expect("recovered again");
    assert_eq!(get(&again, "before").await.as_deref(), Some("1"));
    assert_eq!(get(&again, "after").await.as_deref(), Some("2"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn absolute_recovery_refuses_a_damaged_log() {
    let storage = FaultStorage::default();
    let db = open(&storage, options()).await.expect("opened");
    db.write(&put("before", "1"), &write_options(Durability::Sync))
        .await
        .expect("written");
    let image = storage.crash(Path::new(PATH), true);
    image.append_raw(&segment_path(Path::new(PATH), 1), &[0xA5; 11]);
    let strict = DbOptions::builder()
        .wal_recovery(WalRecovery::Absolute)
        .build()
        .expect("valid");
    assert!(matches!(
        open(&image, strict).await,
        Err(Error::Corruption { .. })
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_sync_stops_writes_and_keeps_what_was_synced() {
    let storage = FaultStorage::default();
    let db = open(&storage, options()).await.expect("opened");
    db.write(&put("kept", "1"), &write_options(Durability::Sync))
        .await
        .expect("written");
    storage.fail_syncs();
    let failed = db
        .write(&put("lost", "2"), &write_options(Durability::Sync))
        .await;
    assert!(matches!(failed, Err(Error::LogFailed)));
    let later = db
        .write(&put("later", "3"), &write_options(Durability::Buffered))
        .await;
    assert!(matches!(later, Err(Error::LogFailed)));
    assert_eq!(get(&db, "lost").await, None);

    let reopened = open(&storage.crash(Path::new(PATH), true), options())
        .await
        .expect("recovered");
    assert_eq!(get(&reopened, "kept").await.as_deref(), Some("1"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn closing_makes_buffered_writes_durable() {
    let storage = FaultStorage::default();
    let db = open(&storage, options()).await.expect("opened");
    db.write(&put("buffered", "1"), &write_options(Durability::Buffered))
        .await
        .expect("written");
    db.close().await.expect("closed");
    let reopened = open(&storage.crash(Path::new(PATH), true), options())
        .await
        .expect("recovered");
    assert_eq!(get(&reopened, "buffered").await.as_deref(), Some("1"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn opening_respects_create_if_missing_and_error_if_exists() {
    let storage = FaultStorage::default();
    let no_create = DbOptions::builder()
        .create_if_missing(false)
        .build()
        .expect("valid");
    assert!(matches!(
        open(&storage, no_create).await,
        Err(Error::NotFound { .. })
    ));
    let db = open(&storage, options()).await.expect("created");
    db.write(&put("k", "v"), &write_options(Durability::Sync))
        .await
        .expect("written");
    db.close().await.expect("closed");
    let exclusive = DbOptions::builder()
        .error_if_exists(true)
        .build()
        .expect("valid");
    assert!(matches!(
        open(&storage, exclusive).await,
        Err(Error::AlreadyExists { .. })
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recovery_reports_each_segment_with_the_last_sequence_it_holds() {
    let storage = FaultStorage::default();
    let db = open(&storage, options()).await.expect("opened");
    for key in ["a", "b"] {
        db.write(&put(key, "1"), &write_options(Durability::Buffered))
            .await
            .expect("written");
    }
    db.close().await.expect("closed");
    let db = open(&storage, options()).await.expect("reopened");
    db.write(&put("c", "1"), &write_options(Durability::Buffered))
        .await
        .expect("written");
    db.close().await.expect("closed");

    let recovered = super::recover::recover(
        &storage,
        Path::new(PATH),
        &crate::memtable::Memtable::new(),
        WalRecovery::PointInTime,
        0,
    )
    .expect("recovered");
    let found: Vec<(u64, u64)> = recovered
        .segments
        .iter()
        .map(|segment| (segment.log_number, segment.last))
        .collect();
    assert_eq!(found, [(1, 2), (2, 3)]);
    assert_eq!(recovered.next_log_number, 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn batches_on_both_sides_of_the_direct_checksum_size_are_recovered() {
    let limit = tessarikv_constants::DIRECT_CHECKSUM_BYTES;
    let sizes = [1, limit - 64, limit, 2 * limit];
    let storage = FaultStorage::default();
    let db = open(&storage, options()).await.expect("opened");
    for (index, size) in sizes.iter().enumerate() {
        let value = "v".repeat(*size);
        db.write(
            &put(&format!("k{index}"), &value),
            &write_options(Durability::Sync),
        )
        .await
        .expect("written");
    }
    let reopened = open(&storage.crash(Path::new(PATH), true), options())
        .await
        .expect("recovered");
    for (index, size) in sizes.iter().enumerate() {
        let read = get(&reopened, &format!("k{index}")).await;
        assert_eq!(read.map(|value| value.len()), Some(*size), "batch {index}");
    }
}
