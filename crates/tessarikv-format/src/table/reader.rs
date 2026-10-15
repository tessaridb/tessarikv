//! Reading a table: opening it, testing its filter, and walking it in key order.

use std::cmp::Ordering;
use std::sync::Arc;

use super::block::{Block, BlockIter, Compare};
use super::builder::{ENTRIES_PROPERTY, RANGE_DELETES_PROPERTY};
use super::filter::BloomFilter;
use super::handle::{BlockHandle, Footer, read_block};
use crate::error::{Error, Result};

/// An open table. Cloning it shares its bytes and decoded metadata.
#[derive(Debug, Clone)]
pub struct Table {
    file: Arc<[u8]>,
    index: Block,
    filter: BloomFilter,
    filter_key: fn(&[u8]) -> &[u8],
    compare: Compare,
    entries: u64,
    range_deletes: Option<Block>,
}

impl Table {
    /// Opens the table in `file`, ordered by `compare`, whose filter was built from
    /// `filter_key` of each key. The footer, index, filter and properties are verified now; each
    /// data block is verified when it is read.
    ///
    /// # Errors
    ///
    /// [`Error::Truncated`], [`Error::Malformed`] or [`Error::UnsupportedVersion`] for a file
    /// that is not a whole table of this format.
    pub fn open(file: Vec<u8>, compare: Compare, filter_key: fn(&[u8]) -> &[u8]) -> Result<Self> {
        let footer = Footer::decode(&file)?;
        let index = Block::new(read_block(&file, footer.index)?, compare)?;
        let filter = BloomFilter::new(read_block(&file, footer.filter)?)?;
        let properties = Block::new(read_block(&file, footer.properties)?, |left, right| {
            left.cmp(right)
        })?;
        let mut property = properties.iter();
        property.seek(ENTRIES_PROPERTY);
        let entries = if property.valid() && property.key() == ENTRIES_PROPERTY {
            property
                .value()
                .try_into()
                .map(u64::from_le_bytes)
                .map_err(|_| Error::Malformed {
                    what: "table properties",
                    reason: "entry count is not eight bytes",
                })?
        } else {
            return Err(Error::Malformed {
                what: "table properties",
                reason: "no entry count",
            });
        };
        property.seek(RANGE_DELETES_PROPERTY);
        let range_deletes = if property.valid() && property.key() == RANGE_DELETES_PROPERTY {
            let handle = BlockHandle::decode(property.value())?;
            Some(Block::new(read_block(&file, handle)?, compare)?)
        } else {
            None
        };
        property.status()?;
        Ok(Self {
            file: file.into(),
            index,
            filter,
            filter_key,
            compare,
            entries,
            range_deletes,
        })
    }

    /// The table's range deletes as `(key, end)`, in key order.
    ///
    /// # Errors
    ///
    /// [`Error::Malformed`] for a damaged range-delete block.
    pub fn range_deletes(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let Some(block) = &self.range_deletes else {
            return Ok(Vec::new());
        };
        let mut deletes = Vec::new();
        let mut iter = block.iter();
        iter.seek_to_first();
        while iter.valid() {
            deletes.push((iter.key().to_vec(), iter.value().to_vec()));
            iter.next();
        }
        iter.status()?;
        Ok(deletes)
    }

    /// Whether `key` may be in the table: `false` means it certainly is not.
    pub fn may_contain(&self, key: &[u8]) -> bool {
        self.filter.may_contain((self.filter_key)(key))
    }

    /// Entries in the table.
    pub fn entries(&self) -> u64 {
        self.entries
    }

    /// An iterator over the table, positioned nowhere until a seek.
    pub fn iter(&self) -> TableIter {
        TableIter {
            file: Arc::clone(&self.file),
            compare: self.compare,
            index: self.index.iter(),
            data: None,
            error: None,
        }
    }
}

/// A position in a [`Table`]: on an entry, or past the end. A damaged block ends the iteration
/// and [`TableIter::status`] reports it.
#[derive(Debug)]
pub struct TableIter {
    file: Arc<[u8]>,
    compare: Compare,
    index: BlockIter,
    data: Option<BlockIter>,
    error: Option<Error>,
}

impl TableIter {
    /// Whether the iterator is on an entry.
    pub fn valid(&self) -> bool {
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

    /// Moves to the first entry.
    pub fn seek_to_first(&mut self) {
        self.index.seek_to_first();
        self.load();
        if let Some(data) = &mut self.data {
            data.seek_to_first();
        }
        self.skip_finished_blocks();
    }

    /// Moves to the first entry whose key is not less than `target`.
    pub fn seek(&mut self, target: &[u8]) {
        self.index.seek(target);
        self.load();
        if let Some(data) = &mut self.data {
            data.seek(target);
        }
        self.skip_finished_blocks();
    }

    /// Moves to the next entry.
    pub fn next(&mut self) {
        if let Some(data) = &mut self.data {
            data.next();
        }
        self.skip_finished_blocks();
    }

    /// Moves to the last entry.
    pub fn seek_to_last(&mut self) {
        self.index.seek_to_last();
        self.load();
        if let Some(data) = &mut self.data {
            data.seek_to_last();
        }
        self.skip_finished_blocks_backward();
    }

    /// Moves to the last entry whose key is not greater than `target`.
    pub fn seek_for_prev(&mut self, target: &[u8]) {
        self.seek(target);
        if self.valid() {
            if (self.compare)(self.key(), target) == Ordering::Greater {
                self.prev();
            }
        } else if self.status().is_ok() {
            self.seek_to_last();
        }
    }

    /// Moves to the previous entry; before the first one the iterator is no longer valid.
    pub fn prev(&mut self) {
        if let Some(data) = &mut self.data {
            data.prev();
        }
        self.skip_finished_blocks_backward();
    }

    /// Opens the data block the index is on, or none past the last.
    fn load(&mut self) {
        self.data = None;
        if self.error.is_some() || !self.index.valid() {
            return;
        }
        let block = BlockHandle::decode(self.index.value())
            .and_then(|handle| read_block(&self.file, handle))
            .and_then(|payload| Block::new(payload, self.compare));
        match block {
            Ok(block) => self.data = Some(block.iter()),
            Err(error) => self.error = Some(error),
        }
    }

    /// Moves past data blocks that have no entries left, stopping at an error.
    fn skip_finished_blocks(&mut self) {
        loop {
            match &self.data {
                Some(data) if data.valid() => return,
                Some(data) if data.status().is_err() => return,
                None => return,
                Some(_) => {}
            }
            self.index.next();
            self.load();
            if let Some(data) = &mut self.data {
                data.seek_to_first();
            }
        }
    }

    /// Moves back past data blocks that have no entries left, stopping at an error.
    fn skip_finished_blocks_backward(&mut self) {
        loop {
            match &self.data {
                Some(data) if data.valid() => return,
                Some(data) if data.status().is_err() => return,
                None => return,
                Some(_) => {}
            }
            self.index.prev();
            self.load();
            if let Some(data) = &mut self.data {
                data.seek_to_last();
            }
        }
    }
}
