//! A reading handle; clone it freely.

use super::{BufferError, Shared, position};
use crate::shim::{Arc, Ordering};

/// A handle that reads published bytes. Reads never wait for the writer.
#[derive(Clone)]
pub struct AppendReader {
    shared: Arc<Shared>,
}

impl AppendReader {
    pub(super) const fn new(shared: Arc<Shared>) -> Self {
        Self { shared }
    }

    /// How many bytes are published.
    #[must_use]
    pub fn len(&self) -> u64 {
        // Acquire: pairs with the writer's Release publish (ledger #6 ← #3).
        self.shared.published.load(Ordering::Acquire)
    }

    /// Whether nothing is published.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Fills `buf` with the bytes starting at `offset`.
    ///
    /// # Errors
    ///
    /// [`BufferError::OutOfRange`] when the range is not entirely published; `buf` is then
    /// left unspecified.
    pub fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<(), BufferError> {
        let available = self.len();
        let wanted = u64::try_from(buf.len()).map_err(|_| BufferError::Overflow)?;
        let requested_end = offset.checked_add(wanted).ok_or(BufferError::Overflow)?;
        if requested_end > available {
            return Err(BufferError::OutOfRange {
                requested_end,
                available,
            });
        }
        let mut at = offset;
        let mut rest = buf;
        while !rest.is_empty() {
            let (index, within) = position(at, self.shared.chunk_bytes)?;
            let room = self
                .shared
                .chunk_bytes
                .checked_sub(within)
                .ok_or(BufferError::Overflow)?;
            let take = room.min(rest.len());
            let (piece, tail) = std::mem::take(&mut rest).split_at_mut(take);
            let chunk = self.shared.chunk(index).ok_or(BufferError::OutOfRange {
                requested_end,
                available,
            })?;
            // SAFETY: every byte read lies below `available`, which the Acquire load above
            // synchronized with; the writer never writes below the published length.
            unsafe { chunk.bytes.read(within, piece) }.ok_or(BufferError::Overflow)?;
            at = at
                .checked_add(u64::try_from(take).map_err(|_| BufferError::Overflow)?)
                .ok_or(BufferError::Overflow)?;
            rest = tail;
        }
        Ok(())
    }
}
