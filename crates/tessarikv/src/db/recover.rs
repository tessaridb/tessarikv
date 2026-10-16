//! Rebuilding the in-memory table from the log when a database on storage opens.
//!
//! Segments are replayed in log-number order, frames in file order, which is commit order. A
//! frame's payload is the first sequence number its batch was committed at, then the batch's
//! encoding. Batches at or below the flushed sequence are already in table files and are passed
//! over; a batch never straddles it, since a flush cuts between two epochs and a batch's
//! sequences all belong to one. Sequence numbers only grow along the log; a range that was never logged (a batch
//! written without the log, or one whose writer gave up) is simply absent.
//! A whole segment is never absent: one missing between two others fails the open.
//!
//! The replay stops at the first frame that is not valid. Under
//! [`WalRecovery::PointInTime`] the damaged segment is cut back to its valid prefix and every
//! later segment is removed, so the next writes continue a clean log; under
//! [`WalRecovery::Absolute`] opening fails and nothing is changed. A frame whose checksum holds
//! but whose payload does not decode is never a torn write, and fails the open in every mode.
//!
//! In an encrypted database each segment starts with the seal header of the open that wrote it,
//! and each batch is sealed under that header's key with its first sequence number, which stays
//! in the clear, as the nonce. A batch failing authentication under a checksum that holds was
//! changed, and fails the open in every mode; a segment cut inside its header is a torn write.

use std::path::Path;

use tessarikv_format::{
    EndKind, FileKind, FileSealer, FrameReader, LogEnd, LogRead, SEAL_HEADER_BYTES, WriteBatch,
};
use tessarikv_types::SequenceNumber;

use crate::log::{FinishedSegment, segment_path};
use crate::memtable::Memtable;
use crate::sealing::Sealing;
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
    /// The segments left in place, in log-number order, with the last sequence each holds.
    pub(super) segments: Vec<FinishedSegment>,
}

/// Replays the log in `dir` into `tables`, one per keyspace by id, from above sequence
/// `flushed`. Runs on a blocking thread.
pub(super) fn recover(
    storage: &dyn Storage,
    dir: &Path,
    tables: &[(u32, Memtable)],
    mode: WalRecovery,
    flushed: u64,
    sealing: Option<&Sealing>,
) -> Result<Recovered> {
    let segments = segments(storage, dir)?;
    // Segments are created and removed in log-number order, so the ones left are consecutive;
    // a number missing between two of them is a segment removed from outside, with writes the
    // replay would otherwise skip without a word, whatever the recovery mode.
    for pair in segments.windows(2) {
        if let [before, after] = *pair
            && before.checked_add(1) != Some(after)
        {
            return Err(corruption(
                &segment_path(dir, before.saturating_add(1)),
                0,
                format!("log segment missing between segments {before} and {after}"),
            ));
        }
    }
    let next_log_number = match segments.last() {
        Some(last) => last.checked_add(1).ok_or(Error::SequenceExhausted)?,
        None => 1,
    };
    let mut last_sequence = 0;
    let mut kept = Vec::with_capacity(segments.len());
    for (index, &log_number) in segments.iter().enumerate() {
        let path = segment_path(dir, log_number);
        let bytes = read_all(storage, &path, "write-ahead log")?;
        // An encrypted segment starts with its seal header; one cut inside it is a torn write.
        let (start, sealer) = match sealing {
            Some(sealing) if bytes.len() >= SEAL_HEADER_BYTES => (
                SEAL_HEADER_BYTES,
                Some(sealing.open_file(&bytes, FileKind::Log, &path)?),
            ),
            Some(_) if !bytes.is_empty() => (bytes.len(), None),
            _ => (0, None),
        };
        let torn_header = sealing.is_some() && start > 0 && sealer.is_none();
        let mut frames = FrameReader::new(bytes.get(start..).unwrap_or_default(), log_number);
        let end = if torn_header {
            LogEnd {
                offset: 0,
                kind: EndKind::Torn,
            }
        } else {
            loop {
                match frames.next_frame() {
                    LogRead::Frame(payload) => {
                        let sealer = sealer.as_deref();
                        last_sequence =
                            replay(tables, payload, sealer, last_sequence, flushed, &path)?;
                    }
                    LogRead::End(end) => {
                        let offset = u64::try_from(start)
                            .ok()
                            .and_then(|start| end.offset.checked_add(start))
                            .ok_or_else(|| corruption(&path, end.offset, "offset".to_owned()))?;
                        break LogEnd { offset, ..end };
                    }
                }
            }
        };
        kept.push(FinishedSegment {
            log_number,
            last: last_sequence,
        });
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
        segments: kept,
    })
}

/// Applies one logged batch unless it is at or below `flushed`, each entry to its keyspace's
/// table (an entry of a keyspace no longer held is skipped: the keyspace was dropped after it);
/// returns its last sequence number.
fn replay(
    tables: &[(u32, Memtable)],
    payload: &[u8],
    sealer: Option<&FileSealer>,
    previous: u64,
    flushed: u64,
    path: &Path,
) -> Result<u64> {
    let bad = |detail: &str| corruption(path, 0, detail.to_owned());
    let (sequence, encoded) = payload
        .split_first_chunk::<SEQUENCE_BYTES>()
        .ok_or_else(|| bad("log payload shorter than its sequence number"))?;
    let first = u64::from_le_bytes(*sequence);
    if first <= previous {
        return Err(bad("log sequence numbers go backwards"));
    }
    // A sealed batch failing authentication under a checksum that held was changed, not torn.
    let encoded = match sealer {
        Some(sealer) => sealer
            .open(first, encoded)
            .map_err(|_| bad("logged batch fails authentication"))?,
        None => encoded.to_vec(),
    };
    let batch = WriteBatch::from_bytes(encoded)
        .map_err(|error| bad(&format!("logged batch does not decode: {error}")))?;
    let count = u64::from(batch.len());
    let last = count
        .checked_sub(1)
        .and_then(|offset| first.checked_add(offset))
        .ok_or_else(|| bad("logged batch is empty or past the sequence space"))?;
    if first > flushed {
        let mut sequence = first;
        for entry in &batch {
            let number = SequenceNumber::new(sequence).map_err(|_| Error::SequenceExhausted)?;
            let keyspace = entry.keyspace.get();
            if let Ok(at) = tables.binary_search_by_key(&keyspace, |(id, _)| *id)
                && let Some((_, table)) = tables.get(at)
            {
                table.insert(
                    number,
                    entry.kind,
                    entry.key,
                    entry.value.unwrap_or_default(),
                )?;
            }
            sequence = sequence.checked_add(1).ok_or(Error::SequenceExhausted)?;
        }
    }
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
