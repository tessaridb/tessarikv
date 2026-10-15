//! The segment files of the log: frames appended in order, rolled at the size limit, and
//! finished segments removed once released.

use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::Arc;

use tessarikv_format::{FRAME_HEADER_BYTES, frame_header};

use super::{FinishedSegment, Frame, segment_path};
use crate::storage::{Storage, WritableFile};
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
        }
    }

    /// Appends `frames` in order, rolling to a new segment where the limit requires, then syncs
    /// the current segment when `sync`. Runs on a blocking thread.
    pub(super) fn write(&mut self, frames: Vec<(Frame, u64)>, sync: bool) -> Result<()> {
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
        self.finished.push_back(FinishedSegment {
            log_number: self.log_number,
            last: self.current_last,
        });
        self.log_number = self.log_number.checked_add(1).ok_or(Error::LogFailed)?;
        Ok(())
    }

    /// Whether the oldest finished segment holds nothing past `through`.
    pub(super) fn has_removable(&self, through: u64) -> bool {
        self.finished
            .front()
            .is_some_and(|segment| segment.last <= through)
    }

    /// Removes finished segments, oldest first, while every frame in them is at or below
    /// `through`. Runs on a blocking thread. A segment whose removal fails stays first in line
    /// for the next round: nothing acknowledged depends on it being gone.
    pub(super) fn remove_through(&mut self, through: u64) {
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
