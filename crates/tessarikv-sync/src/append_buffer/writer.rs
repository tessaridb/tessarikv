//! The single appender.

use super::{BufferError, Shared, position};
use crate::shim::{Arc, Ordering};

/// The one handle allowed to append. Not `Clone`: a second writer would break the protocol.
pub struct AppendWriter {
    shared: Arc<Shared>,
    /// The length after the last append; equal to `published`, kept here to avoid a load.
    len: u64,
}

impl AppendWriter {
    pub(super) const fn new(shared: Arc<Shared>) -> Self {
        Self { shared, len: 0 }
    }

    /// Appends `data` and publishes it to every reader.
    ///
    /// # Errors
    ///
    /// [`BufferError::Overflow`] when the length would pass `u64::MAX`; nothing is published.
    pub fn append(&mut self, data: &[u8]) -> Result<(), BufferError> {
        let added = u64::try_from(data.len()).map_err(|_| BufferError::Overflow)?;
        let end = self.len.checked_add(added).ok_or(BufferError::Overflow)?;
        let mut at = self.len;
        let mut rest = data;
        while !rest.is_empty() {
            let (index, within) = position(at, self.shared.chunk_bytes)?;
            let room = self
                .shared
                .chunk_bytes
                .checked_sub(within)
                .ok_or(BufferError::Overflow)?;
            let (piece, tail) = rest.split_at(room.min(rest.len()));
            let chunk = self
                .shared
                .chunk_for_write(index)
                .ok_or(BufferError::Overflow)?;
            // SAFETY: `at` is at or above the published length, so no reader reads these bytes
            // until the Release store below publishes them.
            unsafe { chunk.bytes.write(within, piece) }.ok_or(BufferError::Overflow)?;
            at = at
                .checked_add(u64::try_from(piece.len()).map_err(|_| BufferError::Overflow)?)
                .ok_or(BufferError::Overflow)?;
            rest = tail;
        }
        // Release: the linearization point; publishes the bytes and pointers above.
        self.shared.published.store(end, Ordering::Release);
        self.len = end;
        Ok(())
    }

    /// The length after the last append.
    #[must_use]
    pub const fn len(&self) -> u64 {
        self.len
    }

    /// Whether nothing has been appended.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// A new reader of this buffer.
    #[must_use]
    pub fn reader(&self) -> super::AppendReader {
        super::AppendReader::new(Arc::clone(&self.shared))
    }
}
