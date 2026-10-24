//! A write is readable when it returns: it waits for every earlier batch to become visible.

use std::sync::Arc;
use std::time::Duration;

use tessarikv_sync::Claim;

use super::Db;
use super::visibility::Completion;
use crate::{DbOptions, KeyspaceId, ReadOptions, WriteBatch, WriteOptions};

/// Long enough for a write that does not wait to have returned many times over.
const WAITING: Duration = Duration::from_millis(200);
/// Bounds every wait a regression could leave unanswered.
const ANSWERED: Duration = Duration::from_secs(10);

async fn memory_db() -> Arc<Db> {
    Arc::new(
        Db::open("visibility", DbOptions::in_memory())
            .await
            .expect("opened"),
    )
}

async fn put(db: &Db, key: &[u8]) {
    let mut batch = WriteBatch::new();
    batch.put(KeyspaceId::DEFAULT, key, b"v").expect("entry");
    db.write(&batch, &WriteOptions::default())
        .await
        .expect("written");
}

async fn get(db: &Db, key: &[u8]) -> Option<Vec<u8>> {
    db.get(KeyspaceId::DEFAULT, key, &ReadOptions::default())
        .await
        .expect("read")
}

/// Completes a claim the test held open, as a writer finishing its batch would.
fn release(db: &Db, claim: Claim) {
    drop(Completion::new(&db.sequencer, None, &db.visibility, claim));
}

/// A batch claimed earlier and still applying holds a later write back: the later write does
/// not return until the earlier batch completes, and then its key reads back at once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_write_returns_only_once_every_earlier_batch_is_visible() {
    let db = memory_db().await;
    let held = db.sequencer.claim(1).expect("claimed");
    let writer = Arc::clone(&db);
    let mut write = tokio::spawn(async move { put(&writer, b"after").await });
    assert!(
        tokio::time::timeout(WAITING, &mut write).await.is_err(),
        "the write returned while an earlier batch was still applying"
    );
    release(&db, held);
    tokio::time::timeout(ANSWERED, write)
        .await
        .expect("the write returns once the earlier batch completes")
        .expect("the writing task");
    assert_eq!(get(&db, b"after").await.as_deref(), Some(&b"v"[..]));
}

/// A write dropped while it waits for an earlier batch stops counting as a waiter and holds
/// nothing back: once the earlier batch completes, later writes return and read back.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_write_dropped_while_it_waits_holds_nothing_back() {
    let db = memory_db().await;
    let held = db.sequencer.claim(1).expect("claimed");
    let writer = Arc::clone(&db);
    let write = tokio::spawn(async move { put(&writer, b"abandoned").await });
    tokio::time::timeout(ANSWERED, async {
        while !db.sequencer.has_waiters() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the write waits behind the held batch");
    write.abort();
    assert!(write.await.is_err_and(|error| error.is_cancelled()));
    assert!(
        !db.sequencer.has_waiters(),
        "the dropped write still counts"
    );
    release(&db, held);
    tokio::time::timeout(ANSWERED, put(&db, b"later"))
        .await
        .expect("a later write returns");
    assert_eq!(get(&db, b"later").await.as_deref(), Some(&b"v"[..]));
}

/// The race as it happens in use: a large batch is still being applied when a one-entry batch
/// claimed after it is written; the moment the small write returns, it is read back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_small_write_behind_a_slow_large_batch_is_read_back_when_it_returns() {
    let trials: u32 = std::env::var("TESSARIKV_STRESS_TRIALS")
        .ok()
        .and_then(|trials| trials.parse().ok())
        .unwrap_or(10);
    let value = vec![7_u8; 16 * 1024];
    for trial in 0..trials {
        let db = memory_db().await;
        let mut large = WriteBatch::new();
        for entry in 0..2_000_u32 {
            large
                .put(
                    KeyspaceId::DEFAULT,
                    format!("large/{entry}").as_bytes(),
                    &value,
                )
                .expect("entry");
        }
        let writer = Arc::clone(&db);
        let first =
            tokio::spawn(async move { writer.write(&large, &WriteOptions::default()).await });
        tokio::time::sleep(Duration::from_millis(1)).await;
        tokio::time::timeout(ANSWERED, put(&db, b"small"))
            .await
            .expect("the small write returns once the large batch is visible");
        assert_eq!(
            get(&db, b"small").await.as_deref(),
            Some(&b"v"[..]),
            "trial {trial}: the small write returned before it was readable"
        );
        first
            .await
            .expect("the large writing task")
            .expect("the large batch");
    }
}
