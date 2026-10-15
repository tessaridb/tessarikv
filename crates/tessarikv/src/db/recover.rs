//! Rebuilding the in-memory table from the log when a database on storage opens.
//!
//! Segments are replayed in log-number order, frames in file order, which is commit order. A
//! frame's payload is the first sequence number its batch was committed at, then the batch's
//! encoding. Sequence numbers only grow along the log; a range that was never logged (a batch
//! written without the log, or one whose writer gave up) is simply absent.
//!
//! The replay stops at the first frame that is not valid. Under
//! [`WalRecovery::PointInTime`] the damaged segment is cut back to its valid prefix and every
//! later segment is removed, so the next writes continue a clean log; under
//! [`WalRecovery::Absolute`] opening fails and nothing is changed. A frame whose checksum holds
//! but whose payload does not decode is never a torn write, and fails the open in every mode.

use std::path::Path;

use tessarikv_format::{EndKind, FrameReader, LogRead, WriteBatch};

use super::write::apply;
use crate::log::segment_path;
use crate::memtable::Memtable;
use crate::storage::{Storage, read_all};
use crate::{Error, Result, WalRecovery};

/// Bytes of the sequence number at the start of a log payload.
pub(super) const SEQUENCE_BYTES: usize = 8;

/// What the replay found.
pub(super) struct Recovered {
    /// The last sequence number any recovered batch used (0 for none).
    pub(super) last_sequence: u64,
    /// The log number for the next segment.
    pub(super) next_log_number: u64,
}

/// Replays the log in `dir` into `table`. Runs on a blocking thread.
pub(super) fn recover(
    storage: &dyn Storage,
    dir: &Path,
    table: &Memtable,
    mode: WalRecovery,
) -> Result<Recovered> {
    let segments = segments(storage, dir)?;
    let next_log_number = match segments.last() {
        Some(last) => last.checked_add(1).ok_or(Error::SequenceExhausted)?,
        None => 1,
    };
    let mut last_sequence = 0;
    for (index, &log_number) in segments.iter().enumerate() {
        let path = segment_path(dir, log_number);
        let bytes = read_all(storage, &path, "write-ahead log")?;
        let mut frames = FrameReader::new(&bytes, log_number);
        let end = loop {
            match frames.next_frame() {
                LogRead::Frame(payload) => {
                    last_sequence = replay(table, payload, last_sequence, &path)?;
                }
                LogRead::End(end) => break end,
            }
        };
        if end.kind == EndKind::Clean {
            continue;
        }
        if mode == WalRecovery::Absolute {
            return Err(corruption(
                &path,
                end.offset,
                format!("{:?} frame ends the valid log", end.kind),
            ));
        }
        cut(storage, dir, log_number, &bytes, end.offset)?;
        for later in segments.get(index.saturating_add(1)..).unwrap_or_default() {
            storage.remove(&segment_path(dir, *later))?;
        }
        storage.sync_dir(dir)?;
        break;
    }
    Ok(Recovered {
        last_sequence,
        next_log_number,
    })
}

/// Applies one logged batch; returns its last sequence number.
fn replay(table: &Memtable, payload: &[u8], previous: u64, path: &Path) -> Result<u64> {
    let bad = |detail: &str| corruption(path, 0, detail.to_owned());
    let (sequence, encoded) = payload
        .split_first_chunk::<SEQUENCE_BYTES>()
        .ok_or_else(|| bad("log payload shorter than its sequence number"))?;
    let first = u64::from_le_bytes(*sequence);
    if first <= previous {
        return Err(bad("log sequence numbers go backwards"));
    }
    let batch = WriteBatch::from_bytes(encoded.to_vec())
        .map_err(|error| bad(&format!("logged batch does not decode: {error}")))?;
    let count = u64::from(batch.len());
    let last = count
        .checked_sub(1)
        .and_then(|offset| first.checked_add(offset))
        .ok_or_else(|| bad("logged batch is empty or past the sequence space"))?;
    apply(table, first, &batch)?;
    Ok(last)
}

/// The log numbers of the segments in `dir`, ascending. Leftovers of an interrupted cut are
/// removed.
fn segments(storage: &dyn Storage, dir: &Path) -> Result<Vec<u64>> {
    let mut numbers = Vec::new();
    for name in storage.list(dir)? {
        if name.ends_with(CUT_SUFFIX) {
            storage.remove(&dir.join(&name))?;
        } else if let Some(number) = name
            .strip_suffix(".log")
            .and_then(|stem| stem.parse::<u64>().ok())
        {
            numbers.push(number);
        }
    }
    numbers.sort_unstable();
    Ok(numbers)
}

/// The suffix of a segment's valid prefix while it replaces the segment.
const CUT_SUFFIX: &str = ".log.cut";

/// Replaces segment `log_number` with its first `valid` bytes: written beside it, synced, then
/// renamed over it, so a crash during the cut leaves either the old file or the new one.
fn cut(storage: &dyn Storage, dir: &Path, log_number: u64, bytes: &[u8], valid: u64) -> Result<()> {
    let path = segment_path(dir, log_number);
    let valid =
        usize::try_from(valid).map_err(|_| corruption(&path, valid, "offset".to_owned()))?;
    let prefix = bytes
        .get(..valid)
        .ok_or_else(|| corruption(&path, 0, "valid prefix past the file".to_owned()))?;
    let temporary = dir.join(format!("{log_number:020}{CUT_SUFFIX}"));
    let mut file = storage.create(&temporary)?;
    file.append(prefix)?;
    file.sync()?;
    drop(file);
    storage.rename(&temporary, &path)
}

fn corruption(path: &Path, offset: u64, detail: String) -> Error {
    Error::Corruption {
        what: "write-ahead log",
        path: Some(path.to_path_buf()),
        offset: Some(offset),
        detail,
    }
}
