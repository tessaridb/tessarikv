//! The segment files of the log: frames appended in order, rolled at the size limit, and
//! finished segments removed once released.

use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::Arc;

use tessarikv_format::{FRAME_HEADER_BYTES, SEAL_HEADER_BYTES, frame_header};

use super::{FinishedSegment, Frame, segment_path};
use crate::statistics::{Statistics, Ticker};
use crate::storage::{FileSyncer, Storage, WritableFile};
use crate::{Error, Result};

/// The segment files: the current one, and where the next one goes.
pub(super) struct Segments {
    storage: Arc<dyn Storage>,
    dir: PathBuf,
    /// The log number of the current segment, or of the next one when none is open.
    log_number: u64,
    file: Option<Box<dyn WritableFile>>,
    /// Bytes a segment may hold unless one frame alone is larger.
    limit: u64,
    /// Frames gathered for one append; kept between writes for its capacity.
    buffer: Vec<u8>,
    /// The last sequence of the frames in the current segment.
    current_last: u64,
    /// The segments rolled away from, oldest first, until they are removed.
    finished: VecDeque<FinishedSegment>,
    /// The seal header every new segment starts with, when the log is encrypted.
    pub(super) header: Option<[u8; SEAL_HEADER_BYTES]>,
    /// Where the bytes written and the syncs are counted, when the database keeps statistics.
    pub(super) statistics: Option<Arc<Statistics>>,
}

impl Segments {
    /// No segment open yet; the first one is `log_number` in `dir`. `finished` lists the
    /// segments already there, oldest first.
    pub(super) fn new(
        storage: Arc<dyn Storage>,
        dir: PathBuf,
        log_number: u64,
        limit: u64,
        finished: Vec<FinishedSegment>,
    ) -> Self {
        Self {
            storage,
            dir,
            log_number,
            file: None,
            limit,
            buffer: Vec::new(),
            current_last: 0,
            finished: finished.into(),
            header: None,
            statistics: None,
        }
    }

    /// Appends `frames` in order, rolling to a new segment where the limit requires; a finished
    /// segment is synced as it is rolled away from.
    pub(super) fn write(&mut self, frames: Vec<(Frame, u64)>) -> Result<()> {
        for (frame, last) in frames {
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
                if let Some(header) = &self.header {
                    self.buffer.extend_from_slice(header);
                }
            }
            let payload_len = bytes.len().saturating_sub(FRAME_HEADER_BYTES);
            let header = frame_header(self.log_number, payload_len, frame.checksum)
                .map_err(|_| Error::LogFailed)?;
            bytes
                .get_mut(..FRAME_HEADER_BYTES)
                .ok_or(Error::LogFailed)?
                .copy_from_slice(&header);
            self.buffer.extend_from_slice(&bytes);
            self.current_last = last;
        }
        self.append()
    }

    /// A handle that syncs the current segment beside later appends, when its storage has one.
    pub(super) fn syncer(&self) -> Option<Arc<dyn FileSyncer>> {
        self.file.as_ref().and_then(|file| file.syncer())
    }

    /// Syncs the current segment here, for a storage without a [`FileSyncer`].
    pub(super) fn sync_current(&mut self) -> Result<()> {
        if let Some(file) = self.file.as_mut() {
            file.sync()?;
            self.count(Ticker::WalSyncs, 1);
        }
        Ok(())
    }

    /// Whether `bytes` more of frames are a plain append to the open segment: it takes them
    /// without rolling, and nothing released through `through` is to be removed. Such a write
    /// neither syncs nor creates nor removes a file.
    pub(super) fn appends_in_place(&self, bytes: u64, through: u64) -> bool {
        self.file.is_some()
            && !self.has_removable(through)
            && self
                .used()
                .ok()
                .and_then(|used| used.checked_add(bytes))
                .is_some_and(|total| total <= self.limit)
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
            let bytes = u64::try_from(self.buffer.len()).unwrap_or(u64::MAX);
            self.count(Ticker::WalBytes, bytes);
        }
        self.buffer.clear();
        Ok(())
    }

    /// Finishes the current segment, synced, and moves to the next log number.
    fn roll(&mut self) -> Result<()> {
        self.append()?;
        if let Some(mut file) = self.file.take() {
            file.sync()?;
            self.count(Ticker::WalSyncs, 1);
        }
        self.finished.push_back(FinishedSegment {
            log_number: self.log_number,
            last: self.current_last,
        });
        self.log_number = self.log_number.checked_add(1).ok_or(Error::LogFailed)?;
        Ok(())
    }

    /// Adds `amount` to `ticker`, when the database keeps statistics.
    fn count(&self, ticker: Ticker, amount: u64) {
        if let Some(statistics) = &self.statistics {
            statistics.record(ticker, amount);
        }
    }

    /// Whether a segment holds nothing past `through`: the oldest finished one, or the current
    /// one, which [`Segments::remove_through`] then finishes.
    pub(super) fn has_removable(&self, through: u64) -> bool {
        self.finished
            .front()
            .is_some_and(|segment| segment.last <= through)
            || self.current_released(through)
    }

    /// Whether the current segment is open and holds nothing past `through`.
    fn current_released(&self, through: u64) -> bool {
        self.file.is_some() && self.current_last <= through
    }

    /// Removes the segments, oldest first, while every frame in them is at or below `through`;
    /// the current one is finished first when that holds for it too, so a flush that released
    /// every frame leaves no log behind. Runs on a blocking thread. A segment whose removal fails
    /// stays first in line for the next round: nothing acknowledged depends on it being gone.
    ///
    /// # Errors
    ///
    /// The storage's error when the current segment cannot be synced as it is finished.
    pub(super) fn remove_through(&mut self, through: u64) -> Result<()> {
        if self.current_released(through) {
            // Synced although every frame is in a synced table: a segment whose removal fails, or
            // is cut short by a crash, must not end the replay at a torn tail before later ones.
            self.roll()?;
        }
        while let Some(segment) = self.finished.front().copied() {
            if segment.last > through {
                break;
            }
            match self
                .storage
                .remove(&segment_path(&self.dir, segment.log_number))
            {
                Ok(()) | Err(Error::NotFound { .. }) => {
                    self.finished.pop_front();
                }
                Err(_) => break,
            }
        }
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
