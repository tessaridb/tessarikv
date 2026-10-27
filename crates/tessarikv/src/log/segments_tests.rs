//! Which drains the log may append on its own task: a plain append to an open segment only.

use std::path::PathBuf;
use std::sync::Arc;

use tessarikv_format::FRAME_HEADER_BYTES;

use super::FinishedSegment;
use super::frame::Frame;
use super::segments::Segments;
use crate::storage::{MemoryStorage, Storage};

fn frame(payload: usize) -> Frame {
    Frame::with_checksum(vec![7; FRAME_HEADER_BYTES + payload], 0).expect("a frame")
}

fn segments(limit: u64, finished: Vec<FinishedSegment>) -> Segments {
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new());
    Segments::new(storage, PathBuf::from("/db/log"), 3, limit, finished)
}

#[test]
fn no_open_segment_is_not_appended_in_place() {
    let segments = segments(1 << 20, Vec::new());
    assert!(!segments.appends_in_place(100, 0));
}

#[test]
fn an_append_that_fits_the_open_segment_is_made_in_place() {
    let mut segments = segments(1 << 20, Vec::new());
    segments.write(vec![(frame(100), 1)]).expect("written");
    assert!(segments.appends_in_place(100, 0));
}

#[test]
fn an_append_that_would_roll_the_segment_is_handed_over() {
    let mut segments = segments(1024, Vec::new());
    segments.write(vec![(frame(500), 1)]).expect("written");
    assert!(segments.appends_in_place(400, 0));
    assert!(!segments.appends_in_place(600, 0));
}

#[test]
fn a_released_segment_to_remove_is_handed_over() {
    let finished = vec![FinishedSegment {
        log_number: 2,
        last: 5,
    }];
    let mut segments = segments(1 << 20, finished);
    segments.write(vec![(frame(100), 9)]).expect("written");
    assert!(segments.appends_in_place(100, 4));
    // Released through 5: the finished segment is to be removed.
    assert!(!segments.appends_in_place(100, 5));
    // Released through 9: the current one is to be finished, synced and removed.
    assert!(!segments.appends_in_place(100, 9));
}
