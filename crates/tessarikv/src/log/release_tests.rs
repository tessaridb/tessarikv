//! Removing finished segments once their frames are released.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use tessarikv_format::FRAME_HEADER_BYTES;

use super::fault::FaultStorage;
use super::tests::{DIR, append, dir, frame, payload, start};
use super::*;
use crate::storage::Storage as _;

fn exists(storage: &FaultStorage, log_number: u64) -> bool {
    storage
        .exists(&segment_path(Path::new(DIR), log_number))
        .expect("exists")
}

/// Returns once segment `log_number` is gone, failing the test instead of hanging.
async fn removed(storage: &FaultStorage, log_number: u64) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while exists(storage, log_number) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("segment {log_number} is removed"));
}

/// A log whose segments hold two frames each, with sequences `1..=7` written: segments 1 to 3
/// are finished and hold 1-2, 3-4 and 5-6; segment 4 is being written and holds 7.
async fn seven_frames(storage: &FaultStorage) -> LogWriter {
    let small = payload(1).len() + FRAME_HEADER_BYTES;
    let log = start(storage, u64::try_from(small * 2).expect("fits"));
    for sequence in 1..=7 {
        let record = Record::Frame {
            frame: frame(&payload(sequence)),
            wait: Wait::Written,
        };
        append(&log, sequence, sequence, record)
            .await
            .expect("written");
    }
    for log_number in 1..=4 {
        assert!(exists(storage, log_number), "segment {log_number} written");
    }
    log
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_release_removes_the_finished_segments_it_covers_without_another_write() {
    let storage = FaultStorage::default();
    let log = seven_frames(&storage).await;
    // Segment 2 ends exactly at the released sequence.
    log.releaser().release_through(4);
    removed(&storage, 1).await;
    removed(&storage, 2).await;
    assert!(exists(&storage, 3), "segment 3 holds 5 and 6");
    log.close().await.expect("closed");
    assert!(exists(&storage, 3), "segment 3 holds 5 and 6");
    assert!(exists(&storage, 4), "segment 4 holds 7");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_segment_being_written_is_never_removed() {
    let storage = FaultStorage::default();
    let log = seven_frames(&storage).await;
    log.releaser().release_through(100);
    removed(&storage, 3).await;
    log.close().await.expect("closed");
    assert!(!exists(&storage, 1) && !exists(&storage, 2));
    assert!(exists(&storage, 4), "segment 4 was still being written");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_smaller_release_after_a_larger_one_takes_nothing_back() {
    let storage = FaultStorage::default();
    let log = seven_frames(&storage).await;
    log.releaser().release_through(6);
    log.releaser().release_through(2);
    log.close().await.expect("closed");
    for log_number in 1..=3 {
        assert!(
            !exists(&storage, log_number),
            "segment {log_number} removed"
        );
    }
    assert!(exists(&storage, 4));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn segments_found_on_storage_are_released_too() {
    let storage = FaultStorage::default();
    for log_number in 1..=2 {
        let mut file = storage
            .create(&segment_path(&dir(), log_number))
            .expect("created");
        file.append(b"replayed").expect("written");
    }
    let files: Arc<dyn crate::storage::Storage> = Arc::new(storage.clone());
    let finished = vec![
        FinishedSegment {
            log_number: 1,
            last: 3,
        },
        FinishedSegment {
            log_number: 2,
            last: 6,
        },
    ];
    let log = LogWriter::start(files, dir(), 3, 7, 1 << 20, finished, LogExtras::default())
        .expect("inside a runtime");
    log.releaser().release_through(5);
    removed(&storage, 1).await;
    log.close().await.expect("closed");
    assert!(exists(&storage, 2), "segment 2 holds 6");
}
