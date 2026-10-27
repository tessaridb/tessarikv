//! Syncs that overlap: a durable append does not wait for an earlier sync to finish before its
//! own starts, and a sync's result is published only after every earlier one succeeded.

use std::sync::Arc;
use std::time::Duration;

use super::fault::FaultStorage;
use super::tests::{dir, frame, payload, start};
use super::*;

/// Opens the paused gate when dropped, so a failing test does not leave syncs hanging.
struct Resume(FaultStorage);

impl Drop for Resume {
    fn drop(&mut self) {
        self.0.resume_syncs();
    }
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

/// A durable append of sequence `sequence`, running on its own task.
fn durable(log: &Arc<LogWriter>, sequence: u64) -> tokio::task::JoinHandle<Result<()>> {
    let log = Arc::clone(log);
    tokio::spawn(async move {
        let record = Record::Frame {
            frame: frame(&payload(sequence)),
            wait: Wait::Durable,
        };
        log.append(sequence, sequence, record).await
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_durable_append_starts_its_sync_while_an_earlier_one_is_still_running() {
    let storage = FaultStorage::default();
    let log = Arc::new(start(&storage, 1 << 20));
    storage.pause_syncs();
    let resume = Resume(storage.clone());
    let first = durable(&log, 1);
    until("the first sync waits", || storage.syncs_waiting("") == 1).await;
    let second = durable(&log, 2);
    until("the second sync starts beside the first", || {
        storage.syncs_waiting("") == 2
    })
    .await;
    drop(resume);
    first.await.expect("joined").expect("durable");
    second.await.expect("joined").expect("durable");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_later_sync_that_succeeds_is_not_reported_before_an_earlier_one_that_fails() {
    let storage = FaultStorage::default();
    let log = Arc::new(start(&storage, 1 << 20));
    storage.pause_syncs();
    let resume = Resume(storage.clone());
    let first = durable(&log, 1);
    until("the first sync waits", || storage.syncs_waiting("") == 1).await;
    let second = durable(&log, 2);
    until("the second sync waits", || storage.syncs_waiting("") == 2).await;
    // The second sync finishes and covers both frames while the first is still held.
    let segment = segment_path(&dir(), 1);
    storage.let_newest_sync_through();
    until("the second sync covered both frames", || {
        let written = storage.bytes(&segment).len();
        written > 0 && storage.synced_bytes(&segment).len() == written
    })
    .await;
    // The first sync then fails: neither writer may hear that its frame is durable.
    storage.fail_syncs();
    drop(resume);
    let first = first.await.expect("joined");
    let second = second.await.expect("joined");
    assert!(matches!(first, Err(Error::LogFailed)), "first: {first:?}");
    assert!(
        matches!(second, Err(Error::LogFailed)),
        "the later sync was reported before the earlier one failed: {second:?}"
    );
    assert!(log.failed());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn closing_waits_for_the_syncs_still_running() {
    let storage = FaultStorage::default();
    let log = start(&storage, 1 << 20);
    storage.pause_syncs();
    let resume = Resume(storage.clone());
    let record = Record::Frame {
        frame: frame(&payload(1)),
        wait: Wait::Durable,
    };
    let slot = log.reserve().await.expect("reserved");
    assert_eq!(slot.send(1, 1, record), Some(Wait::Durable));
    let progress = log.progress.clone();
    until("the frame's sync waits", || storage.syncs_waiting("") == 1).await;
    let closing = tokio::spawn(log.close());
    until("the closing sync waits beside it", || {
        storage.syncs_waiting("") == 2
    })
    .await;
    drop(resume);
    closing.await.expect("joined").expect("closed");
    assert!(
        progress.borrow().covers(1, Wait::Durable),
        "the log closed before its syncs finished"
    );
}
