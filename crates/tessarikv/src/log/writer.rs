//! The log writer task: frames in sequence order, into segments, written and synced.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tessarikv_constants::LOG_QUEUE_ENTRIES;
use tokio::sync::{Notify, mpsc, watch};

use super::segments::Segments;
use super::{Entry, FinishedSegment, Wait};
use crate::storage::Storage;
use crate::{Error, Result};

/// How far the log has got, in sequence numbers.
#[derive(Debug, Clone, Copy)]
pub(super) struct Progress {
    /// Every sequence up to this one is written or skipped.
    written: u64,
    /// Every frame up to this sequence is on stable storage.
    durable: u64,
    /// The log stopped after a failed write or sync.
    pub(super) failed: bool,
}

impl Progress {
    /// Nothing written yet; the first entry is expected at `next_sequence`.
    pub(super) fn new(next_sequence: u64) -> Self {
        let before = next_sequence.saturating_sub(1);
        Self {
            written: before,
            durable: before,
            failed: false,
        }
    }

    /// Whether a frame ending at `last` has reached `wait`.
    pub(super) fn covers(&self, last: u64, wait: Wait) -> bool {
        match wait {
            Wait::Written => self.written >= last,
            Wait::Durable => self.durable >= last,
        }
    }
}

/// How far the log may remove finished segments, and the notice that wakes the task for it.
#[derive(Default)]
pub(super) struct Released {
    /// Finished segments whose frames are all at or below this sequence may be removed.
    pub(super) through: AtomicU64,
    /// Wakes the task when `through` moves while no entry arrives.
    pub(super) notice: Notify,
}

/// The task's state: entries waiting for their turn, and the segment files.
pub(super) struct Writer {
    /// The first sequence of the next entry to write.
    next: u64,
    /// Entries that arrived before an earlier one, by first sequence.
    pending: BTreeMap<u64, Entry>,
    /// The files; lent to a blocking thread while it does I/O.
    segments: Option<Segments>,
    released: Arc<Released>,
}

impl Writer {
    pub(super) fn new(
        storage: Arc<dyn Storage>,
        dir: PathBuf,
        log_number: u64,
        next_sequence: u64,
        segment_bytes: u64,
        finished: Vec<FinishedSegment>,
        released: Arc<Released>,
    ) -> Self {
        Self {
            next: next_sequence,
            pending: BTreeMap::new(),
            segments: Some(Segments::new(
                storage,
                dir,
                log_number,
                segment_bytes,
                finished,
            )),
            released,
        }
    }

    /// Serves entries until every sender is gone, then syncs and returns. A failure is
    /// published before it is returned, so no waiter is left waiting.
    pub(super) async fn run(
        mut self,
        mut entries: mpsc::Receiver<Entry>,
        progress: watch::Sender<Progress>,
    ) -> Result<()> {
        let result = self.serve(&mut entries, &progress).await;
        if result.is_err() {
            progress.send_modify(|progress| progress.failed = true);
        }
        result
    }

    async fn serve(
        &mut self,
        entries: &mut mpsc::Receiver<Entry>,
        progress: &watch::Sender<Progress>,
    ) -> Result<()> {
        let released = Arc::clone(&self.released);
        loop {
            tokio::select! {
                // Entries first: a closed channel ends the loop, which syncs and removes too.
                biased;
                entry = entries.recv() => {
                    let Some(entry) = entry else { break };
                    self.pending.insert(entry.first, entry);
                    // Take whatever else is already queued, so one write and one sync serve all
                    // of it; bounded, so a steady stream of writers cannot postpone the write
                    // forever.
                    for _ in 1..LOG_QUEUE_ENTRIES {
                        let Ok(entry) = entries.try_recv() else { break };
                        self.pending.insert(entry.first, entry);
                    }
                    self.write_ready(progress, false).await?;
                }
                () = released.notice.notified() => self.remove_released().await?,
            }
        }
        self.write_ready(progress, true).await
    }

    /// The sequence finished segments may be removed through.
    fn released_through(&self) -> u64 {
        // Acquire: pairs with the Release `fetch_max` in `LogRelease::release_through`, so the
        // caller's table and manifest syncs happen before any removal this value allows.
        self.released.through.load(Ordering::Acquire)
    }

    /// Removes the released finished segments, when there are any.
    async fn remove_released(&mut self) -> Result<()> {
        let through = self.released_through();
        let segments = self.segments.as_ref().ok_or(Error::LogFailed)?;
        if !segments.has_removable(through) {
            return Ok(());
        }
        let mut segments = self.segments.take().ok_or(Error::LogFailed)?;
        let segments = tokio::task::spawn_blocking(move || {
            segments.remove_through(through);
            segments
        })
        .await
        .map_err(|_| Error::LogFailed)?;
        self.segments = Some(segments);
        Ok(())
    }

    /// Writes the entries that continue the log without a gap; syncs when one of them waits
    /// for durability, or when `closing`.
    async fn write_ready(
        &mut self,
        progress: &watch::Sender<Progress>,
        closing: bool,
    ) -> Result<()> {
        let mut frames = Vec::new();
        let mut sync = closing;
        let mut last = None;
        while let Some(entry) = self.pending.remove(&self.next) {
            self.next = entry.last.checked_add(1).ok_or(Error::SequenceExhausted)?;
            sync |= entry.durable;
            last = Some(entry.last);
            frames.extend(entry.frame.map(|frame| (frame, entry.last)));
        }
        if last.is_none() && !closing {
            return Ok(());
        }
        let through = self.released_through();
        let mut segments = self.segments.take().ok_or(Error::LogFailed)?;
        let (segments, written) = tokio::task::spawn_blocking(move || {
            let written = segments.write(frames, sync);
            if written.is_ok() {
                segments.remove_through(through);
            }
            (segments, written)
        })
        .await
        .map_err(|_| Error::LogFailed)?;
        self.segments = Some(segments);
        written?;
        progress.send_modify(|progress| {
            if let Some(last) = last {
                progress.written = last;
            }
            if sync {
                progress.durable = progress.written;
            }
        });
        Ok(())
    }
}
