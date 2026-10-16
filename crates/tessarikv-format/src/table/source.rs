//! Where a table's data blocks come from when they are read.
//!
//! A table keeps its index, filter and properties in memory and asks a [`BlockSource`] for each
//! data block as a walk reaches it. A source answers at once: with the block when it has it at
//! hand, or with "not at hand" when getting it would mean waiting (reading storage, for
//! instance). "Not at hand" never means "absent": the walk then parks on that block, its owner
//! fetches the block however it likes, and [`super::TableIter::resume`] continues the walk.
//! This keeps the format crate free of any runtime and lets a caller do the waiting on a
//! thread that may block.

use std::fmt;
use std::sync::Arc;

use super::block::{Block, Compare};
use super::handle::{BlockHandle, read_block, unseal};
use crate::error::{Error, Result};
use crate::seal::FileSealer;

/// Data blocks of one table file, answered without waiting.
pub trait BlockSource: Send + Sync + fmt::Debug {
    /// The data block at `handle`, or `None` when it is not at hand without waiting.
    ///
    /// # Errors
    ///
    /// A damaged block, when the source decodes one it holds.
    fn block(&self, handle: BlockHandle) -> Result<Option<Block>>;
}

/// The block in `sealed`, the exact bytes a [`BlockHandle`] names in a table file, ordered by
/// `compare`; `at` is the handle's offset and `sealer` the file's key when the file is sealed.
/// Its checksum is verified and its payload decompressed and checked.
///
/// # Errors
///
/// [`Error::Unauthenticated`] for a sealed block opened with another key or at another offset,
/// or changed; [`Error::Malformed`] or [`Error::Truncated`] for bytes that are not a whole,
/// intact block.
pub fn unseal_block(
    sealed: &[u8],
    at: u64,
    sealer: Option<&FileSealer>,
    compare: Compare,
) -> Result<Block> {
    Block::new(unseal(sealed, at, sealer)?, compare)
}

/// Why a table could not be opened through reads.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum OpenError<E> {
    /// The bytes read are not a whole table of this format.
    #[error(transparent)]
    Format(Error),
    /// A read failed; the reader's own error.
    #[error("reading the table failed")]
    Read(E),
}

/// Every data block of a table whose whole file is in memory: always at hand.
#[derive(Debug)]
pub(super) struct Resident {
    pub(super) file: Arc<[u8]>,
    pub(super) compare: Compare,
    pub(super) sealer: Option<Arc<FileSealer>>,
}

impl BlockSource for Resident {
    fn block(&self, handle: BlockHandle) -> Result<Option<Block>> {
        Block::new(
            read_block(&self.file, handle, self.sealer.as_deref())?,
            self.compare,
        )
        .map(Some)
    }
}
