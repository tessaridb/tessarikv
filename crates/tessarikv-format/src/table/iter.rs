//! Walking a table in key order, in either direction, one data block at a time.

use std::cmp::Ordering;
use std::sync::Arc;

use super::block::{Block, BlockIter, Compare};
use super::handle::BlockHandle;
use super::source::BlockSource;
use crate::error::{Error, Result};

/// A position in a [`super::Table`]: on an entry, past either end, or waiting for a data block.
///
/// A move that reaches a data block its source does not have at hand stops there:
/// [`TableIter::pending`] names the block, and [`TableIter::resume`] with that block finishes
/// the move exactly as if the block had been at hand. While waiting the iterator is not on an
/// entry and reports no error. A damaged block ends the iteration and [`TableIter::status`]
/// reports it.
#[derive(Debug)]
pub struct TableIter {
    source: Arc<dyn BlockSource>,
    compare: Compare,
    index: BlockIter,
    data: Option<BlockIter>,
    pending: Option<(BlockHandle, Want)>,
    error: Option<Error>,
}

/// Where in a data block a move lands once the block is at hand.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Want {
    /// Its first entry, then onward.
    First,
    /// Its first entry not less than the target, then onward.
    Seek(Vec<u8>),
    /// Its last entry, then backward.
    Last,
    /// Its last entry not greater than the target, then backward.
    ForPrev(Vec<u8>),
}

impl TableIter {
    pub(super) fn new(source: Arc<dyn BlockSource>, compare: Compare, index: BlockIter) -> Self {
        Self {
            source,
            compare,
            index,
            data: None,
            pending: None,
            error: None,
        }
    }

    /// Whether the iterator is on an entry.
    pub fn valid(&self) -> bool {
        // A waiting move holds no block, so it is never on an entry.
        self.error.is_none() && self.data.as_ref().is_some_and(BlockIter::valid)
    }

    /// The current key; empty when not on an entry.
    pub fn key(&self) -> &[u8] {
        if self.valid() {
            self.data.as_ref().map_or(&[], BlockIter::key)
        } else {
            &[]
        }
    }

    /// The current value; empty when not on an entry.
    pub fn value(&self) -> &[u8] {
        if self.valid() {
            self.data.as_ref().map_or(&[], BlockIter::value)
        } else {
            &[]
        }
    }

    /// Why the iteration stopped early, if it did.
    ///
    /// # Errors
    ///
    /// The error of the damaged block.
    pub fn status(&self) -> Result<()> {
        if let Some(error) = &self.error {
            return Err(error.clone());
        }
        self.index.status()?;
        self.data.as_ref().map_or(Ok(()), BlockIter::status)
    }

    /// The data block the last move is waiting for, if it is waiting.
    pub fn pending(&self) -> Option<BlockHandle> {
        self.pending.as_ref().map(|(handle, _)| *handle)
    }

    /// Finishes the waiting move with `block`, the block [`TableIter::pending`] named. The move
    /// may then wait again, for a later block. Without a waiting move this does nothing.
    pub fn resume(&mut self, block: Block) {
        if let Some((_, want)) = self.pending.take() {
            self.enter(block, want);
        }
    }

    /// Moves to the first entry.
    pub fn seek_to_first(&mut self) {
        self.reset();
        self.index.seek_to_first();
        self.load(Want::First);
    }

    /// Moves to the first entry whose key is not less than `target`.
    pub fn seek(&mut self, target: &[u8]) {
        self.reset();
        self.index.seek(target);
        self.load(Want::Seek(target.to_vec()));
    }

    /// Moves to the next entry. Waiting, or past the end, it does nothing.
    pub fn next(&mut self) {
        if self.pending.is_some() {
            return;
        }
        if let Some(data) = &mut self.data {
            data.next();
            self.forward();
        }
    }

    /// Moves to the last entry.
    pub fn seek_to_last(&mut self) {
        self.reset();
        self.index.seek_to_last();
        self.load(Want::Last);
    }

    /// Moves to the last entry whose key is not greater than `target`.
    pub fn seek_for_prev(&mut self, target: &[u8]) {
        self.reset();
        // The first block whose last key is not less than the target holds the answer, or its
        // predecessor does; past every block, the answer is the last entry.
        self.index.seek(target);
        if self.index.valid() {
            self.load(Want::ForPrev(target.to_vec()));
        } else if self.index.status().is_ok() {
            self.index.seek_to_last();
            self.load(Want::Last);
        }
    }

    /// Moves to the previous entry; before the first one the iterator is no longer valid.
    /// Waiting, it does nothing.
    pub fn prev(&mut self) {
        if self.pending.is_some() {
            return;
        }
        if let Some(data) = &mut self.data {
            data.prev();
            self.backward();
        }
    }

    /// Forgets the current block and any waiting move.
    fn reset(&mut self) {
        self.data = None;
        self.pending = None;
    }

    /// Opens the data block the index is on and lands in it as `want` says, or waits for it;
    /// past the last block there is none.
    fn load(&mut self, want: Want) {
        self.data = None;
        if self.error.is_some() || !self.index.valid() {
            return;
        }
        let handle = match BlockHandle::decode(self.index.value()) {
            Ok(handle) => handle,
            Err(error) => {
                self.error = Some(error);
                return;
            }
        };
        match self.source.block(handle) {
            Ok(Some(block)) => self.enter(block, want),
            Ok(None) => self.pending = Some((handle, want)),
            Err(error) => self.error = Some(error),
        }
    }

    /// Lands in `block` as `want` says and moves on past finished blocks.
    fn enter(&mut self, block: Block, want: Want) {
        let mut data = block.iter();
        let forward = match &want {
            Want::First => {
                data.seek_to_first();
                true
            }
            Want::Seek(target) => {
                data.seek(target);
                true
            }
            Want::Last => {
                data.seek_to_last();
                false
            }
            Want::ForPrev(target) => {
                data.seek(target);
                if !data.valid() {
                    if data.status().is_ok() {
                        data.seek_to_last();
                    }
                } else if (self.compare)(data.key(), target) == Ordering::Greater {
                    data.prev();
                }
                false
            }
        };
        self.data = Some(data);
        if forward {
            self.forward();
        } else {
            self.backward();
        }
    }

    /// Moves past data blocks that have no entries left, stopping at an error or a wait.
    fn forward(&mut self) {
        while self.finished() {
            self.index.next();
            self.load(Want::First);
        }
    }

    /// Moves back past data blocks that have no entries left, stopping at an error or a wait.
    fn backward(&mut self) {
        while self.finished() {
            self.index.prev();
            self.load(Want::Last);
        }
    }

    /// Whether the current block has no entry left to stand on and no error to report, so the
    /// walk must go on to a neighbouring block.
    fn finished(&self) -> bool {
        match &self.data {
            Some(data) => !data.valid() && data.status().is_ok(),
            None => false,
        }
    }
}
