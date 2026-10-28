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

mod frame;
mod segments;
mod slot;
mod syncs;
mod writer;

#[cfg(test)]
pub(crate) mod fault;
#[cfg(test)]
mod overlap_tests;
#[cfg(test)]
mod release_tests;
#[cfg(test)]
mod segments_tests;
#[cfg(test)]
mod sync_gate;
#[cfg(test)]
mod tests;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use tessarikv_constants::LOG_QUEUE_ENTRIES;
use tessarikv_format::{FileSealer, SealHeader};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

use crate::statistics::Statistics;
use crate::storage::{Storage, StorageLock};
use crate::{Error, Result};

pub(crate) use frame::Frame;
pub(crate) use slot::{OwnedSlot, Slot};
pub(crate) use writer::LogRelease;
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

/// The handle writers append through; the log writer task runs until it is closed.
pub(crate) struct LogWriter {
    entries: mpsc::Sender<Entry>,
    progress: watch::Receiver<Progress>,
    released: Arc<Released>,
    task: JoinHandle<Result<()>>,
    /// The key writers seal their frames with, when the log is encrypted.
    sealer: Option<Arc<FileSealer>>,
}

/// What a log needs beyond where its segments go: the seal of an encrypted log, and where it
/// counts the bytes it writes and its syncs.
#[derive(Default)]
pub(crate) struct LogExtras {
    pub(crate) seal: Option<(SealHeader, Arc<FileSealer>)>,
    pub(crate) statistics: Option<Arc<Statistics>>,
    /// The directory lock, held until the writer task ends, which is after the handle is
    /// dropped: the frames already queued are still written.
    pub(crate) lock: Option<Arc<dyn StorageLock>>,
    pub(crate) events: Option<Arc<crate::options::Reporter>>,
}

impl LogWriter {
    /// Starts the log writer task on the current tokio runtime. Its first segment is
    /// `log_number` in `dir`, created when the first frame arrives; it expects the first entry
    /// at sequence `next_sequence`, and closes a segment before it grows past `segment_bytes`.
    /// `finished` lists the segments already in `dir`, in log-number order, so they are removed
    /// once released. With `seal`, every segment this writer creates starts with its header, and
    /// writers seal their frames with its key (see [`LogWriter::sealer`]).
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
        extras: LogExtras,
    ) -> Result<Self> {
        let LogExtras {
            seal,
            statistics,
            lock,
            events,
        } = extras;
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
        )
        .with_seal_header(seal.as_ref().map(|(header, _)| header.encode()))
        .with_statistics(statistics)
        .with_events(events);
        let task = runtime.spawn(async move {
            let _held = lock;
            writer.run(receiver, publisher).await
        });
        Ok(Self {
            entries,
            progress,
            released,
            task,
            sealer: seal.map(|(_, sealer)| sealer),
        })
    }

    /// The key a writer seals its frame's batch with, when the log is encrypted: the batch
    /// encoding is sealed as the unit at its first sequence number, which stays in the clear.
    pub(crate) fn sealer(&self) -> Option<&FileSealer> {
        self.sealer.as_deref()
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

    /// Returns once every record written before the call is on stable storage, those written
    /// without a sync included: the log syncs what it has written, waiting writes too.
    ///
    /// # Errors
    ///
    /// [`Error::LogFailed`] when the log stopped before the sync, or the sync failed;
    /// [`Error::Closed`] when the log was closed.
    pub(crate) async fn sync(&self) -> Result<()> {
        let written = self.progress.borrow().written();
        // The task syncs everything it has written by the time it serves the notice, which is
        // at least `written`: only the task writes, and `written` only grows.
        self.released.sync.notify_one();
        self.wait(written, Wait::Durable).await
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

    /// Whether the log stopped after a failed write or sync.
    pub(crate) fn failed(&self) -> bool {
        self.progress.borrow().failed
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
