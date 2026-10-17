//! A database dropped without being closed: its background work finishes before anything else
//! may open its directory.

use std::sync::Arc;
use std::time::Duration;

use super::Db;
use super::flush_tests::{PATH, key, open, options, value};
use crate::log::fault::FaultStorage;
use crate::{Durability, Error, KeyspaceId, ReadOptions, WriteBatch, WriteOptions};

/// Lets the paused syncs through when dropped, so a failing assertion does not leave the
/// runtime waiting on them.
struct Resume(FaultStorage);

impl Drop for Resume {
    fn drop(&mut self) {
        self.0.resume_syncs();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dropped_database_holds_its_directory_until_its_background_work_ends() {
    let storage = FaultStorage::default();
    let db = open(&storage).await;
    let buffered = WriteOptions {
        durability: Durability::Buffered,
        ..WriteOptions::default()
    };
    let write = |index: u32| {
        let mut batch = WriteBatch::new();
        batch
            .put(KeyspaceId::DEFAULT, &key(index), &value(index))
            .expect("small entry");
        let (db, buffered) = (&db, &buffered);
        async move { db.write(&batch, buffered).await }
    };
    // Most of a table, while syncs still pass: the log segment exists before the gate closes.
    let mut written = 0;
    while written < 10 {
        write(written).await.expect("written");
        written += 1;
    }
    // Background work that cannot finish: the next sync of any file waits at the paused gate.
    storage.pause_syncs();
    let resume = Resume(storage.clone());
    while !storage.sync_waiting("") {
        let landed = tokio::time::timeout(Duration::from_secs(1), write(written)).await;
        if landed.is_ok() {
            landed.expect("in time").expect("written");
            written += 1;
        }
        assert!(written < 200, "no background sync started");
        tokio::task::yield_now().await;
    }
    drop(db);

    // The background work is still running, and it still writes the directory. An open that is
    // not refused gets past the lock and waits behind the same paused syncs.
    let opened = tokio::time::timeout(
        Duration::from_secs(5),
        Db::open_with_storage(PATH, options(), Arc::new(storage.clone())),
    )
    .await;
    drop(resume);
    assert!(
        matches!(opened, Ok(Err(Error::Locked { .. }))),
        "a second database got past the lock while the first one's background work was running: \
         {:?}",
        opened.as_ref().map(|result| result.as_ref().err())
    );

    let reopened = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match Db::open_with_storage(PATH, options(), Arc::new(storage.clone())).await {
                Ok(db) => return db,
                Err(Error::Locked { .. }) => tokio::task::yield_now().await,
                Err(error) => panic!("reopening failed: {error:?}"),
            }
        }
    })
    .await
    .expect("the directory is released once the background work ends");
    for index in 0..written {
        let read = reopened
            .get(KeyspaceId::DEFAULT, &key(index), &ReadOptions::default())
            .await
            .expect("readable");
        assert_eq!(read, Some(value(index)), "key {index}");
    }
    reopened.close().await.expect("closed");
}
