//! Syncs one at a time: before the next sync starts, the writers the last one released are let
//! back in, so their next frames share it, without holding a lone writer or waiting forever for
//! one that does not return.

use std::sync::Arc;
use std::time::Duration;

use super::fault::FaultStorage;
use super::tests::{decode, dir, frame, payload};
use super::*;

/// Opens the paused gate when dropped, so a failing test does not leave syncs hanging.
struct Resume(FaultStorage);

impl Drop for Resume {
    fn drop(&mut self) {
        self.0.resume_syncs();
    }
}

/// A log on `storage` whose syncs run one at a time, gathering for at most `gather`.
fn start(storage: &FaultStorage, gather: Duration) -> LogWriter {
    storage.serial_syncs();
    let files: Arc<dyn crate::storage::Storage> = Arc::new(storage.clone());
    let extras = LogExtras {
        gather: Some(gather),
        ..LogExtras::default()
    };
    LogWriter::start(files, dir(), 1, 1, 1 << 20, Vec::new(), extras).expect("inside a runtime")
}

async fn until(what: &str, done: impl Fn() -> bool) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while !done() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{what}"));
}

/// Hands the durable frame of `sequence` to the log here, and waits for it on its own task.
async fn durable(log: &Arc<LogWriter>, sequence: u64) -> tokio::task::JoinHandle<Result<()>> {
    let record = Record::Frame {
        frame: frame(&payload(sequence)),
        wait: Wait::Durable,
    };
    let wait = log
        .reserve()
        .await
        .expect("room")
        .send(sequence, sequence, record);
    assert_eq!(wait, Some(Wait::Durable));
    let log = Arc::clone(log);
    tokio::spawn(async move { log.wait(sequence, Wait::Durable).await })
}

fn written(log: &LogWriter) -> u64 {
    log.progress.borrow().written()
}

/// The writer a sync released comes back before the next sync starts, and shares it with the
/// writer that was already waiting.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_released_writer_shares_the_next_sync_with_the_one_waiting() {
    let storage = FaultStorage::default();
    let log = Arc::new(start(&storage, Duration::from_secs(60)));
    storage.pause_syncs();
    let resume = Resume(storage.clone());
    let first = durable(&log, 1).await;
    until("the first sync waits", || storage.syncs_waiting("") == 1).await;
    let second = durable(&log, 2).await;
    storage.let_newest_sync_through();
    first.await.expect("joined").expect("durable");
    let third = durable(&log, 3).await;
    until("the next sync waits", || storage.syncs_waiting("") == 1).await;
    assert_eq!(
        written(&log),
        3,
        "the next sync covers the returning writer"
    );
    assert!(
        !second.is_finished() && !third.is_finished(),
        "nothing the waiting sync covers is acknowledged"
    );
    let synced = storage.synced_bytes(&segment_path(&dir(), 1));
    assert_eq!(
        decode(&synced, 1),
        [payload(1)],
        "a power loss keeps the first frame only"
    );
    drop(resume);
    second.await.expect("joined").expect("durable");
    third.await.expect("joined").expect("durable");
    assert_eq!(storage.syncs_waiting(""), 0);
}

/// A lone writer is never held: its own next frame is the writer the last sync released.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lone_writer_is_not_held_by_the_gather() {
    let storage = FaultStorage::default();
    let log = Arc::new(start(&storage, Duration::from_secs(60)));
    tokio::time::timeout(Duration::from_secs(10), async {
        for sequence in 1..=5 {
            durable(&log, sequence)
                .await
                .await
                .expect("joined")
                .expect("durable");
        }
    })
    .await
    .expect("each sync starts as soon as the lone writer is back");
}

/// A released writer that does not come back holds the next sync only until the deadline.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_next_sync_starts_at_the_deadline_when_a_released_writer_stays_away() {
    let storage = FaultStorage::default();
    let log = Arc::new(start(&storage, Duration::from_millis(50)));
    storage.pause_syncs();
    let resume = Resume(storage.clone());
    let first = durable(&log, 1).await;
    until("the first sync waits", || storage.syncs_waiting("") == 1).await;
    let second = durable(&log, 2).await;
    storage.let_newest_sync_through();
    first.await.expect("joined").expect("durable");
    until("the next sync starts without the first writer", || {
        storage.syncs_waiting("") == 1
    })
    .await;
    assert_eq!(written(&log), 2);
    drop(resume);
    second.await.expect("joined").expect("durable");
}
