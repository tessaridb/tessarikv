//! A reserved place in the queue to the log writer, borrowed from the log or owned.

use tokio::sync::mpsc;

use super::{Entry, LogWriter, Record, Wait};
use crate::Result;

/// A reserved place in the queue to the log writer.
pub(crate) struct Slot<'a>(pub(super) mpsc::Permit<'a, Entry>);

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

/// A reserved place in the queue to the log writer that does not borrow the log, for a range
/// claimed by another task than the one that reserved it.
pub(crate) struct OwnedSlot(mpsc::OwnedPermit<Entry>);

impl OwnedSlot {
    /// Passes over `sequence`: it holds nothing the log keeps, and later frames go on.
    pub(crate) fn skip(self, sequence: u64) {
        self.0.send(Entry {
            first: sequence,
            last: sequence,
            frame: None,
            durable: false,
        });
    }
}

impl LogWriter {
    /// As [`LogWriter::reserve`], for a place handed to another task.
    ///
    /// # Errors
    ///
    /// [`crate::Error::LogFailed`] or [`crate::Error::Closed`] when the log no longer takes
    /// records.
    pub(crate) async fn reserve_owned(&self) -> Result<OwnedSlot> {
        self.entries
            .clone()
            .reserve_owned()
            .await
            .map(OwnedSlot)
            .map_err(|_| self.stopped())
    }
}
