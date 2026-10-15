use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use tessarikv_format::{FRAME_HEADER_BYTES, FrameReader, LogRead};

use super::fault::FaultStorage;
use super::*;

pub(super) const DIR: &str = "/db/log";

pub(super) fn dir() -> PathBuf {
    PathBuf::from(DIR)
}

/// A distinct payload for sequence `sequence`.
pub(super) fn payload(sequence: u64) -> Vec<u8> {
    format!("batch at {sequence}").into_bytes()
}

/// A prepared frame carrying `payload`, its header left for the log writer.
pub(super) fn frame(payload: &[u8]) -> Frame {
    let mut bytes = vec![0; FRAME_HEADER_BYTES];
    bytes.extend_from_slice(payload);
    Frame::new(bytes).expect("test payloads are within the limit")
}

/// Appends through `log`, failing the test instead of hanging when the append never returns.
pub(super) async fn append(log: &LogWriter, first: u64, last: u64, record: Record) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(10), log.append(first, last, record))
        .await
        .expect("the append returns")
}

pub(super) fn start(storage: &FaultStorage, segment_bytes: u64) -> LogWriter {
    let files: Arc<dyn crate::storage::Storage> = Arc::new(storage.clone());
    LogWriter::start(files, dir(), 1, 1, segment_bytes, Vec::new()).expect("inside a runtime")
}

/// The payloads of one segment and whether it ended cleanly.
fn decode(bytes: &[u8], log_number: u64) -> Vec<Vec<u8>> {
    let mut reader = FrameReader::new(bytes, log_number);
    let mut payloads = Vec::new();
    loop {
        match reader.next_frame() {
            LogRead::Frame(frame) => payloads.push(frame.to_vec()),
            LogRead::End(end) => {
                assert_eq!(
                    end.kind,
                    tessarikv_format::EndKind::Clean,
                    "log {log_number}"
                );
                return payloads;
            }
        }
    }
}

/// Every payload in the log's segments, in segment order, and the segments' sizes.
fn read_log(storage: &FaultStorage) -> (Vec<Vec<u8>>, Vec<usize>) {
    let mut payloads = Vec::new();
    let mut sizes = Vec::new();
    let mut log_number = 1;
    loop {
        let path = segment_path(Path::new(DIR), log_number);
        if !crate::storage::Storage::exists(storage, &path).expect("exists") {
            return (payloads, sizes);
        }
        let bytes = storage.bytes(&path);
        sizes.push(bytes.len());
        payloads.extend(decode(&bytes, log_number));
        log_number += 1;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn frames_reach_the_log_in_sequence_order_whatever_order_they_arrive() {
    let storage = FaultStorage::default();
    let log = Arc::new(start(&storage, 1 << 20));
    let mut tasks = Vec::new();
    for sequence in (1..=64).rev() {
        let log = Arc::clone(&log);
        tasks.push(tokio::spawn(async move {
            let record = Record::Frame {
                frame: frame(&payload(sequence)),
                wait: Wait::Written,
            };
            append(&log, sequence, sequence, record).await
        }));
    }
    for task in tasks {
        task.await.expect("joined").expect("appended");
    }
    Arc::into_inner(log)
        .expect("sole owner")
        .close()
        .await
        .expect("closed");
    let (payloads, _) = read_log(&storage);
    assert_eq!(payloads, (1..=64).map(payload).collect::<Vec<_>>());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_durable_append_returns_only_after_a_sync_covers_it() {
    let storage = FaultStorage::default();
    let log = start(&storage, 1 << 20);
    for sequence in 1..=5 {
        let record = Record::Frame {
            frame: frame(&payload(sequence)),
            wait: Wait::Durable,
        };
        append(&log, sequence, sequence, record)
            .await
            .expect("durable");
        let synced = storage.synced_bytes(&segment_path(&dir(), 1));
        assert_eq!(
            decode(&synced, 1),
            (1..=sequence).map(payload).collect::<Vec<_>>()
        );
    }
    log.close().await.expect("closed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_written_append_is_in_the_file_when_it_returns() {
    let storage = FaultStorage::default();
    let log = start(&storage, 1 << 20);
    let record = Record::Frame {
        frame: frame(&payload(1)),
        wait: Wait::Written,
    };
    append(&log, 1, 1, record).await.expect("written");
    let bytes = storage.bytes(&segment_path(&dir(), 1));
    assert_eq!(decode(&bytes, 1), vec![payload(1)]);
    log.close().await.expect("closed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_skipped_range_lets_later_frames_through() {
    let storage = FaultStorage::default();
    let log = start(&storage, 1 << 20);
    append(&log, 1, 3, Record::Skip).await.expect("skipped");
    let record = Record::Frame {
        frame: frame(&payload(4)),
        wait: Wait::Durable,
    };
    append(&log, 4, 4, record).await.expect("durable");
    log.close().await.expect("closed");
    assert_eq!(read_log(&storage).0, vec![payload(4)]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn segments_roll_at_their_limit_and_an_oversized_frame_gets_its_own() {
    let storage = FaultStorage::default();
    let small = payload(1).len() + FRAME_HEADER_BYTES;
    let log = start(&storage, u64::try_from(small * 3).expect("fits"));
    let mut expected = Vec::new();
    for sequence in 1..=7 {
        let body = if sequence == 5 {
            vec![b'x'; small * 4]
        } else {
            payload(sequence)
        };
        let record = Record::Frame {
            frame: frame(&body),
            wait: Wait::Written,
        };
        append(&log, sequence, sequence, record)
            .await
            .expect("written");
        expected.push(body);
    }
    log.close().await.expect("closed");
    let (payloads, sizes) = read_log(&storage);
    assert_eq!(payloads, expected);
    assert!(sizes.len() >= 3, "segments: {sizes:?}");
    assert!(
        sizes.contains(&(small * 4 + FRAME_HEADER_BYTES)),
        "segments: {sizes:?}"
    );
    for size in sizes
        .iter()
        .filter(|size| **size != small * 4 + FRAME_HEADER_BYTES)
    {
        assert!(*size <= small * 3, "segments: {sizes:?}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_sync_fails_its_waiter_and_every_later_append() {
    let storage = FaultStorage::default();
    let log = start(&storage, 1 << 20);
    let first = Record::Frame {
        frame: frame(&payload(1)),
        wait: Wait::Durable,
    };
    append(&log, 1, 1, first)
        .await
        .expect("durable before the failure");
    storage.fail_syncs();
    let second = Record::Frame {
        frame: frame(&payload(2)),
        wait: Wait::Durable,
    };
    assert!(matches!(
        append(&log, 2, 2, second).await,
        Err(Error::LogFailed)
    ));
    let third = Record::Frame {
        frame: frame(&payload(3)),
        wait: Wait::Written,
    };
    assert!(matches!(
        append(&log, 3, 3, third).await,
        Err(Error::LogFailed)
    ));
    assert!(log.close().await.is_err());
    let synced = storage.synced_bytes(&segment_path(&dir(), 1));
    assert_eq!(decode(&synced, 1), vec![payload(1)]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn closing_writes_and_syncs_everything_sent() {
    let storage = FaultStorage::default();
    let log = start(&storage, 1 << 20);
    for sequence in 1..=10 {
        let record = Record::Frame {
            frame: frame(&payload(sequence)),
            wait: Wait::Written,
        };
        append(&log, sequence, sequence, record)
            .await
            .expect("written");
    }
    log.close().await.expect("closed");
    let synced = storage.synced_bytes(&segment_path(&dir(), 1));
    assert_eq!(
        decode(&synced, 1),
        (1..=10).map(payload).collect::<Vec<_>>()
    );
}
