//! The log writer task: frames in sequence order, into segments, written and synced.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tessarikv_constants::{LOG_QUEUE_ENTRIES, LOG_SYNC_GATHER_MICROS};
use tessarikv_format::SEAL_HEADER_BYTES;
use tokio::sync::{Notify, mpsc, watch};

use super::gather::Gather;
use super::progress::Progress;
use super::segments::Segments;
use super::syncs::{Syncs, durable, sync_beside};
use super::{Entry, FinishedSegment};
use crate::options::Reporter;
use crate::statistics::Statistics;
use crate::storage::Storage;
use crate::{Error, Result};

/// How far the log may remove finished segments, and the notices that wake the task.
#[derive(Default)]
pub(super) struct Released {
    /// Finished segments whose frames are all at or below this sequence may be removed.
    pub(super) through: AtomicU64,
    /// Wakes the task when `through` moves while no entry arrives.
    pub(super) notice: Notify,
    /// Wakes the task to sync everything it has written (see `LogWriter::sync`).
    pub(super) sync: Notify,
}

/// Releases log segments once flushed tables hold their frames.
pub(crate) struct LogRelease(pub(super) Arc<Released>);

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

/// The task's state: entries waiting for their turn, and the segment files.
pub(super) struct Writer {
    /// The first sequence of the next entry to write.
    next: u64,
    /// Entries that arrived before an earlier one, by first sequence.
    pending: BTreeMap<u64, Entry>,
    /// The files; lent to a blocking thread while it does I/O.
    segments: Option<Segments>,
    released: Arc<Released>,
    /// Told when the log stops on a failure.
    events: Option<Arc<Reporter>>,
    /// The writers a sync made one at a time released, waited for before the next.
    gather: Gather,
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
            events: None,
            gather: Gather::new(Duration::from_micros(LOG_SYNC_GATHER_MICROS)),
        }
    }

    /// Starts every segment it creates with `header`, a seal header, when there is one.
    pub(super) fn with_seal_header(mut self, header: Option<[u8; SEAL_HEADER_BYTES]>) -> Self {
        if let Some(segments) = self.segments.as_mut() {
            segments.header = header;
        }
        self
    }

    /// Counts the bytes it writes and its syncs in `statistics`, when there are any.
    pub(super) fn with_statistics(mut self, statistics: Option<Arc<Statistics>>) -> Self {
        if let Some(segments) = self.segments.as_mut() {
            segments.statistics = statistics;
        }
        self
    }

    /// Lets the next sync made one at a time wait at most `window` for the writers the last one
    /// released.
    pub(super) fn with_gather(mut self, window: Duration) -> Self {
        self.gather = Gather::new(window);
        self
    }

    /// Reports the failure that stops the log to `events`, when there are any.
    pub(super) fn with_events(mut self, events: Option<Arc<Reporter>>) -> Self {
        self.events = events;
        self
    }

    /// Serves entries until every sender is gone, then syncs and returns. A failure is
    /// published before it is returned, so no waiter is left waiting.
    pub(super) async fn run(
        mut self,
        mut entries: mpsc::Receiver<Entry>,
        progress: watch::Sender<Progress>,
    ) -> Result<()> {
        let result = self.serve(&mut entries, &progress).await;
        if let Err(error) = &result {
            progress.send_modify(|progress| progress.failed = true);
            if let Some(events) = &self.events {
                events.background_error(error);
            }
        }
        result
    }

    async fn serve(
        &mut self,
        entries: &mut mpsc::Receiver<Entry>,
        progress: &watch::Sender<Progress>,
    ) -> Result<()> {
        let released = Arc::clone(&self.released);
        let mut syncs = Syncs::default();
        loop {
            tokio::select! {
                // A sync asked for first, so a steady stream of entries cannot postpone it; then
                // the oldest running sync's result; then entries: a closed channel ends the loop,
                // which syncs and removes too.
                biased;
                () = released.sync.notified() => {
                    self.write_ready(entries, progress, &mut syncs, true).await?;
                }
                through = syncs.oldest(), if !syncs.is_empty() => durable(progress, through?),
                entry = entries.recv() => {
                    let Some(entry) = entry else { break };
                    self.pending.insert(entry.first, entry);
                    // Take whatever else is already queued, so one write and one sync serve all
                    // of it; bounded, so a steady stream of writers cannot postpone the write
                    // forever.
                    let mut taken = 1_usize;
                    for _ in 1..LOG_QUEUE_ENTRIES {
                        let Ok(entry) = entries.try_recv() else { break };
                        self.pending.insert(entry.first, entry);
                        taken = taken.saturating_add(1);
                    }
                    self.gather.arrived(taken);
                    self.write_ready(entries, progress, &mut syncs, false).await?;
                }
                () = released.notice.notified() => self.remove_released().await?,
            }
        }
        self.write_ready(entries, progress, &mut syncs, true)
            .await?;
        while !syncs.is_empty() {
            durable(progress, syncs.oldest().await?);
        }
        Ok(())
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
        let (segments, removed) = tokio::task::spawn_blocking(move || {
            let removed = segments.remove_through(through);
            (segments, removed)
        })
        .await
        .map_err(|_| Error::LogFailed)?;
        self.segments = Some(segments);
        removed
    }

    /// Writes the entries that continue the log without a gap; syncs when one of them waits
    /// for durability, or when `force_sync` (closing, or a sync asked for). Where the storage can
    /// sync beside the writes, the sync is started and left running in `syncs`. Otherwise it is
    /// made here, and the entries queued meanwhile are taken and written next, once the writers
    /// it released are back (see [`Gather`]).
    async fn write_ready(
        &mut self,
        entries: &mut mpsc::Receiver<Entry>,
        progress: &watch::Sender<Progress>,
        syncs: &mut Syncs,
        force_sync: bool,
    ) -> Result<()> {
        let mut force_sync = force_sync;
        loop {
            if !force_sync && self.pending.values().any(|entry| entry.durable) {
                self.gather.wait(entries, &mut self.pending).await;
            }
            let Some(released) = self.write_next(progress, syncs, force_sync).await? else {
                return Ok(());
            };
            let mut queued = 0_usize;
            for _ in 0..LOG_QUEUE_ENTRIES {
                let Ok(entry) = entries.try_recv() else { break };
                self.pending.insert(entry.first, entry);
                queued = queued.saturating_add(1);
            }
            self.gather.ended(released, queued);
            if queued == 0 {
                return Ok(());
            }
            force_sync = false;
        }
    }

    /// One round of [`Writer::write_ready`]: the durable writers a sync made here released.
    async fn write_next(
        &mut self,
        progress: &watch::Sender<Progress>,
        syncs: &mut Syncs,
        force_sync: bool,
    ) -> Result<Option<usize>> {
        let mut frames = Vec::new();
        let mut sync = force_sync;
        let mut last = None;
        let mut durable_entries = 0_usize;
        while let Some(entry) = self.pending.remove(&self.next) {
            self.next = entry.last.checked_add(1).ok_or(Error::SequenceExhausted)?;
            sync |= entry.durable;
            durable_entries = durable_entries.saturating_add(usize::from(entry.durable));
            last = Some(entry.last);
            frames.extend(entry.frame.map(|frame| (frame, entry.last)));
        }
        if last.is_none() && !force_sync {
            return Ok(None);
        }
        let through = self.released_through();
        let mut segments = self.segments.take().ok_or(Error::LogFailed)?;
        let bytes = frames.iter().fold(0_u64, |total, (frame, _)| {
            total.saturating_add(u64::try_from(frame.bytes.len()).unwrap_or(u64::MAX))
        });
        // A plain append only copies into the page cache, so it is made here, on this task:
        // handing it to a blocking thread costs two thread hand-offs, more than the copy. A write
        // that rolls to a new segment or removes one is handed over. After an append made here
        // the task yields: the writers it has just woken may be queued behind it on this worker,
        // and a full queue of entries would otherwise keep it running. The sync, when one is
        // needed, comes after the write and runs beside the task.
        let (segments, written) = if segments.appends_in_place(bytes, through) {
            let written = segments.write(frames);
            tokio::task::yield_now().await;
            (segments, written)
        } else {
            tokio::task::spawn_blocking(move || {
                let written = segments
                    .write(frames)
                    .and_then(|()| segments.remove_through(through));
                (segments, written)
            })
            .await
            .map_err(|_| Error::LogFailed)?
        };
        self.segments = Some(segments);
        written?;
        progress.send_modify(|progress| {
            if let Some(last) = last {
                progress.written_through(last);
            }
        });
        if sync && sync_beside(&mut self.segments, progress, syncs).await? {
            return Ok(Some(durable_entries));
        }
        Ok(None)
    }
}
