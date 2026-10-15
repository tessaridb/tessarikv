//! The log writer task: frames in sequence order, into segments, written and synced.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use tessarikv_constants::LOG_QUEUE_ENTRIES;
use tessarikv_format::{FRAME_HEADER_BYTES, frame_header};
use tokio::sync::{mpsc, watch};

use super::{Entry, Frame, Wait, segment_path};
use crate::storage::{Storage, WritableFile};
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

/// The task's state: entries waiting for their turn, and the segment files.
pub(super) struct Writer {
    /// The first sequence of the next entry to write.
    next: u64,
    /// Entries that arrived before an earlier one, by first sequence.
    pending: BTreeMap<u64, Entry>,
    /// The files; lent to a blocking thread while it does I/O.
    segments: Option<Segments>,
}

impl Writer {
    pub(super) fn new(
        storage: Arc<dyn Storage>,
        dir: PathBuf,
        log_number: u64,
        next_sequence: u64,
        segment_bytes: u64,
    ) -> Self {
        Self {
            next: next_sequence,
            pending: BTreeMap::new(),
            segments: Some(Segments {
                storage,
                dir,
                log_number,
                file: None,
                limit: segment_bytes,
                buffer: Vec::new(),
            }),
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
        while let Some(entry) = entries.recv().await {
            self.pending.insert(entry.first, entry);
            // Take whatever else is already queued, so one write and one sync serve all of it;
            // bounded, so a steady stream of writers cannot postpone the write forever.
            for _ in 1..LOG_QUEUE_ENTRIES {
                let Ok(entry) = entries.try_recv() else { break };
                self.pending.insert(entry.first, entry);
            }
            self.write_ready(progress, false).await?;
        }
        self.write_ready(progress, true).await
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
            frames.extend(entry.frame);
        }
        if last.is_none() && !closing {
            return Ok(());
        }
        let mut segments = self.segments.take().ok_or(Error::LogFailed)?;
        let (segments, written) = tokio::task::spawn_blocking(move || {
            let written = segments.write(frames, sync);
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

/// The segment files: the current one, and where the next one goes.
struct Segments {
    storage: Arc<dyn Storage>,
    dir: PathBuf,
    /// The log number of the current segment, or of the next one when none is open.
    log_number: u64,
    file: Option<Box<dyn WritableFile>>,
    /// Bytes a segment may hold unless one frame alone is larger.
    limit: u64,
    /// Frames gathered for one append; kept between writes for its capacity.
    buffer: Vec<u8>,
}

impl Segments {
    /// Appends `frames` in order, rolling to a new segment where the limit requires, then syncs
    /// the current segment when `sync`. Runs on a blocking thread.
    fn write(&mut self, frames: Vec<Frame>, sync: bool) -> Result<()> {
        for frame in frames {
            let mut bytes = frame.bytes;
            let size = u64::try_from(bytes.len()).map_err(|_| Error::LogFailed)?;
            let used = self.used()?;
            if used > 0
                && used
                    .checked_add(size)
                    .is_none_or(|total| total > self.limit)
            {
                self.roll()?;
            }
            if self.file.is_none() {
                self.open()?;
            }
            let payload_len = bytes.len().saturating_sub(FRAME_HEADER_BYTES);
            let header = frame_header(self.log_number, payload_len, frame.checksum)
                .map_err(|_| Error::LogFailed)?;
            bytes
                .get_mut(..FRAME_HEADER_BYTES)
                .ok_or(Error::LogFailed)?
                .copy_from_slice(&header);
            self.buffer.extend_from_slice(&bytes);
        }
        self.append()?;
        if sync && let Some(file) = self.file.as_mut() {
            file.sync()?;
        }
        Ok(())
    }

    /// Bytes in the current segment, gathered ones included.
    fn used(&self) -> Result<u64> {
        let written = self.file.as_ref().map_or(0, |file| file.len());
        let gathered = u64::try_from(self.buffer.len()).map_err(|_| Error::LogFailed)?;
        written.checked_add(gathered).ok_or(Error::LogFailed)
    }

    /// Writes the gathered frames to the current segment.
    fn append(&mut self) -> Result<()> {
        if let Some(file) = self.file.as_mut()
            && !self.buffer.is_empty()
        {
            file.append(&self.buffer)?;
        }
        self.buffer.clear();
        Ok(())
    }

    /// Finishes the current segment, synced, and moves to the next log number.
    fn roll(&mut self) -> Result<()> {
        self.append()?;
        if let Some(mut file) = self.file.take() {
            file.sync()?;
        }
        self.log_number = self.log_number.checked_add(1).ok_or(Error::LogFailed)?;
        Ok(())
    }

    /// Creates the current segment and makes its directory entry durable.
    fn open(&mut self) -> Result<()> {
        self.storage.create_dir_all(&self.dir)?;
        self.file = Some(
            self.storage
                .create(&segment_path(&self.dir, self.log_number))?,
        );
        self.storage.sync_dir(&self.dir)
    }
}
