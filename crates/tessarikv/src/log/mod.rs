//! The write-ahead log: every logged batch, in commit order, in a sequence of segment files.
//!
//! Writers prepare their frame themselves, payload and payload checksum included, so the
//! expensive per-byte work runs on every writer at once. They hand the frame, with the range of
//! sequence numbers its batch was committed at, to one log writer task over a bounded channel.
//! That task puts frames in sequence order, whatever order they arrive in, names the segment each
//! lands in, writes contiguous runs of them with one call, and syncs when a writer waits for
//! durability: one sync covers every frame written before it (group commit).
//!
//! The order of frames on storage is therefore the commit order, and a frame is written only
//! after every earlier sequence has been written or skipped (a batch that is not logged still
//! sends a skip, so the order has no holes). A crash leaves a valid prefix of the log: after a
//! process crash, everything written; after a power loss, at least everything synced.
//!
//! A segment is closed once the next frame would take it past its size limit, and the next one
//! is created; a frame larger than the limit gets a segment of its own. A failed write or sync
//! stops the log for good ([`Error::LogFailed`]): after a failed sync the operating system may
//! have dropped the unsynced pages, so retrying could report data durable that is gone, and the
//! only safe recovery is to reopen and replay the log from storage.
//!
//! Once every frame up to a sequence number is held in a table file that a synced manifest
//! names, [`LogRelease::release_through`] lets the log remove the finished segments that hold
//! nothing later. The segment being written is never removed.

mod segments;
mod writer;

#[cfg(test)]
pub(crate) mod fault;
#[cfg(test)]
mod release_tests;
#[cfg(test)]
mod tests;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use tessarikv_constants::LOG_QUEUE_ENTRIES;
use tessarikv_format::{FRAME_HEADER_BYTES, frame_bytes};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

use crate::storage::Storage;
use crate::{Error, Result};

use writer::{Progress, Released, Writer};

/// The file of segment `log_number` in `dir`.
pub(crate) fn segment_path(dir: &Path, log_number: u64) -> PathBuf {
    dir.join(format!("{log_number:020}.log"))
}

/// A segment no longer written to, and the last sequence number its frames carry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FinishedSegment {
    /// The segment's log number.
    pub(crate) log_number: u64,
    /// The last sequence number of its last frame; a segment without frames carries the last
    /// sequence of the segments before it.
    pub(crate) last: u64,
}

/// A frame ready to be logged: header space, then the payload, with the payload's checksum.
pub(crate) struct Frame {
    bytes: Vec<u8>,
    checksum: u32,
}

impl Frame {
    /// A frame from `bytes`: [`FRAME_HEADER_BYTES`] of header space, which the log writer
    /// fills in, followed by the payload, whose
    /// [`payload_checksum`](tessarikv_format::payload_checksum) is `checksum`.
    ///
    /// # Errors
    ///
    /// [`Error::Unsupported`] for a payload larger than a frame carries.
    pub(crate) fn with_checksum(bytes: Vec<u8>, checksum: u32) -> Result<Self> {
        let payload_len =
            bytes
                .len()
                .checked_sub(FRAME_HEADER_BYTES)
                .ok_or(Error::Unsupported {
                    what: "a log frame without room for its header",
                })?;
        frame_bytes(payload_len).map_err(|_| Error::Unsupported {
            what: "a log frame larger than the frame limit",
        })?;
        Ok(Self { bytes, checksum })
    }

    /// A frame from `bytes`, as [`Frame::with_checksum`], computing the checksum.
    #[cfg(test)]
    pub(crate) fn new(bytes: Vec<u8>) -> Result<Self> {
        let checksum =
            tessarikv_format::payload_checksum(bytes.get(FRAME_HEADER_BYTES..).unwrap_or_default());
        Self::with_checksum(bytes, checksum)
    }
}

/// What a writer waits for after handing its frame over.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Wait {
    /// The frame is handed to the operating system: it survives a process crash.
    Written,
    /// The frame is on stable storage: it survives a power loss.
    Durable,
}

/// What a committed range of sequence numbers puts in the log.
pub(crate) enum Record {
    /// A frame, and what its writer waits for.
    Frame {
        /// The prepared frame.
        frame: Frame,
        /// When `append` returns.
        wait: Wait,
    },
    /// Nothing: the range is passed over, so later frames are not held back.
    Skip,
}

/// A committed range on its way to the log writer.
struct Entry {
    first: u64,
    last: u64,
    frame: Option<Frame>,
    durable: bool,
}

/// A reserved place in the queue to the log writer.
pub(crate) struct Slot<'a>(mpsc::Permit<'a, Entry>);

impl Slot<'_> {
    /// Hands sequences `first..=last` and their record to the log writer, without waiting.
    /// Returns what the writer should then wait for, if anything.
    pub(crate) fn send(self, first: u64, last: u64, record: Record) -> Option<Wait> {
        let (frame, wait) = match record {
            Record::Frame { frame, wait } => (Some(frame), Some(wait)),
            Record::Skip => (None, None),
        };
        self.0.send(Entry {
            first,
            last,
            frame,
            durable: wait == Some(Wait::Durable),
        });
        wait
    }
}

/// Releases log segments once flushed tables hold their frames.
pub(crate) struct LogRelease(Arc<Released>);

impl LogRelease {
    /// Lets the log remove every finished segment whose frames all carry sequence numbers at
    /// or below `sequence`; called once a table holding them is named by a synced manifest. The
    /// removal happens on the log writer's next round, and at the latest when the log closes. A
    /// smaller `sequence` than one already released changes nothing.
    pub(crate) fn release_through(&self, sequence: u64) {
        // Release: everything this thread did before (the table and manifest syncs) happens
        // before the removal; pairs with the Acquire load in `Writer::released_through`.
        self.0
            .through
            .fetch_max(sequence, std::sync::atomic::Ordering::Release);
        self.0.notice.notify_one();
    }
}

/// The handle writers append through; the log writer task runs until it is closed.
pub(crate) struct LogWriter {
    entries: mpsc::Sender<Entry>,
    progress: watch::Receiver<Progress>,
    released: Arc<Released>,
    task: JoinHandle<Result<()>>,
}

impl LogWriter {
    /// Starts the log writer task on the current tokio runtime. Its first segment is
    /// `log_number` in `dir`, created when the first frame arrives; it expects the first entry
    /// at sequence `next_sequence`, and closes a segment before it grows past `segment_bytes`.
    /// `finished` lists the segments already in `dir`, in log-number order, so they are removed
    /// once released.
    ///
    /// # Errors
    ///
    /// [`Error::Unsupported`] when called outside a tokio runtime.
    pub(crate) fn start(
        storage: Arc<dyn Storage>,
        dir: PathBuf,
        log_number: u64,
        next_sequence: u64,
        segment_bytes: u64,
        finished: Vec<FinishedSegment>,
    ) -> Result<Self> {
        let runtime = tokio::runtime::Handle::try_current().map_err(|_| Error::Unsupported {
            what: "a database outside a tokio runtime",
        })?;
        let (entries, receiver) = mpsc::channel(LOG_QUEUE_ENTRIES);
        let (publisher, progress) = watch::channel(Progress::new(next_sequence));
        let released = Arc::new(Released::default());
        let writer = Writer::new(
            storage,
            dir,
            log_number,
            next_sequence,
            segment_bytes,
            finished,
            Arc::clone(&released),
        );
        let task = runtime.spawn(writer.run(receiver, publisher));
        Ok(Self {
            entries,
            progress,
            released,
            task,
        })
    }

    /// A place in the queue to the log writer, waited for when the queue is full.
    ///
    /// A writer reserves before it claims its sequence numbers, so that once they are claimed
    /// handing them over cannot wait or be cancelled: a claimed range that never reached the
    /// log would hold back every later one.
    ///
    /// # Errors
    ///
    /// [`Error::LogFailed`] or [`Error::Closed`] when the log no longer takes records.
    pub(crate) async fn reserve(&self) -> Result<Slot<'_>> {
        self.entries
            .reserve()
            .await
            .map(Slot)
            .map_err(|_| self.stopped())
    }

    /// Returns once the record ending at sequence `last` has reached `wait`.
    ///
    /// # Errors
    ///
    /// [`Error::LogFailed`] when the log stopped before the record reached `wait`,
    /// [`Error::Closed`] when the log was closed.
    pub(crate) async fn wait(&self, last: u64, wait: Wait) -> Result<()> {
        let mut progress = self.progress.clone();
        let reached = progress
            .wait_for(|progress| progress.covers(last, wait) || progress.failed)
            .await
            .map(|progress| progress.covers(last, wait));
        match reached {
            Ok(true) => Ok(()),
            Ok(false) => Err(Error::LogFailed),
            Err(_) => Err(self.stopped()),
        }
    }

    /// Reserves, hands sequences `first..=last` over and waits as the record asks.
    #[cfg(test)]
    pub(crate) async fn append(&self, first: u64, last: u64, record: Record) -> Result<()> {
        match self.reserve().await?.send(first, last, record) {
            Some(wait) => self.wait(last, wait).await,
            None => Ok(()),
        }
    }

    /// The handle a flush releases log segments through.
    pub(crate) fn releaser(&self) -> LogRelease {
        LogRelease(Arc::clone(&self.released))
    }

    /// Stops accepting records, writes and syncs every record already handed over, and waits
    /// for the log writer task to finish.
    ///
    /// # Errors
    ///
    /// [`Error::LogFailed`] when the log had stopped, or the final write or sync failed.
    pub(crate) async fn close(self) -> Result<()> {
        drop(self.entries);
        self.task.await.map_err(|_| Error::LogFailed)?
    }

    /// The error for a log that no longer takes records.
    fn stopped(&self) -> Error {
        if self.progress.borrow().failed {
            Error::LogFailed
        } else {
            Error::Closed
        }
    }
}
