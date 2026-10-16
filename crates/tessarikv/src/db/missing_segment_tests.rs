//! A log segment that vanished from between two others: the open is refused in every recovery
//! mode, since the writes it held are gone and nothing else would say so.

use std::path::Path;
use std::sync::Arc;

use super::Db;
use crate::log::fault::FaultStorage;
use crate::log::segment_path;
use crate::storage::Storage as _;
use crate::{DbOptions, KeyspaceId, ReadOptions, WalRecovery, WriteBatch, WriteOptions};

const PATH: &str = "/db";
const SEGMENT_BYTES: u64 = 1024 * 1024;

fn options(recovery: WalRecovery) -> DbOptions {
    DbOptions::builder()
        .wal_segment_bytes(SEGMENT_BYTES)
        .wal_recovery(recovery)
        .build()
        .expect("valid")
}

async fn open(storage: &FaultStorage, recovery: WalRecovery) -> crate::Result<Db> {
    Db::open_with_storage(PATH, options(recovery), Arc::new(storage.clone())).await
}

/// Synced writes filling at least three segments; returns the log numbers on storage.
async fn three_segments(storage: &FaultStorage) -> Vec<u64> {
    let db = open(storage, WalRecovery::PointInTime)
        .await
        .expect("opened");
    let value = vec![7_u8; 64 * 1024];
    for index in 0_u32..64 {
        let mut batch = WriteBatch::new();
        batch
            .put(KeyspaceId::DEFAULT, &index.to_be_bytes(), &value)
            .expect("entry");
        db.write(&batch, &WriteOptions::default())
            .await
            .expect("written");
    }
    db.close().await.expect("closed");
    let mut numbers: Vec<u64> = storage
        .list(Path::new(PATH))
        .expect("listable")
        .iter()
        .filter_map(|name| name.strip_suffix(".log")?.parse().ok())
        .collect();
    numbers.sort_unstable();
    assert!(numbers.len() >= 3, "segments: {numbers:?}");
    numbers
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_segment_missing_between_two_others_refuses_the_open() {
    for recovery in [WalRecovery::PointInTime, WalRecovery::Absolute] {
        // Control: the same log, intact, replays.
        let intact = FaultStorage::default();
        three_segments(&intact).await;
        let reopened = open(&intact, recovery).await.expect("reopened");
        let first = reopened
            .get(
                KeyspaceId::DEFAULT,
                &0_u32.to_be_bytes(),
                &ReadOptions::default(),
            )
            .await
            .expect("read");
        assert!(first.is_some(), "an intact log replays");

        let storage = FaultStorage::default();
        let numbers = three_segments(&storage).await;
        let middle = numbers.get(1).copied().expect("three segments");
        storage
            .remove(&segment_path(Path::new(PATH), middle))
            .expect("removed");
        let refused = open(&storage, recovery).await;
        assert_eq!(
            refused.err().map(|error| error.code()),
            Some("corruption"),
            "{recovery:?}"
        );
    }
}
