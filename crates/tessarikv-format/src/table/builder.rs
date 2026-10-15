//! Writing a table.

use std::cmp::Ordering;

use super::block::{BlockBuilder, Compare};
use super::filter::BloomBuilder;
use super::handle::{Footer, write_block};
use super::{Compression, TableOptions};
use crate::error::{Error, Result};

/// The properties block's key for the entry count.
pub(super) const ENTRIES_PROPERTY: &[u8] = b"entries";

/// Builds a table from entries added in strictly increasing key order.
#[derive(Debug)]
pub struct TableBuilder {
    options: TableOptions,
    compare: Compare,
    filter_key: fn(&[u8]) -> &[u8],
    out: Vec<u8>,
    data: BlockBuilder,
    index: BlockBuilder,
    filter: BloomBuilder,
    last_key: Vec<u8>,
    entries: u64,
}

impl TableBuilder {
    /// An empty table ordered by `compare`, whose filter holds `filter_key` of each key (the
    /// part a point lookup knows exactly, such as the user key of an internal key).
    pub fn new(options: TableOptions, compare: Compare, filter_key: fn(&[u8]) -> &[u8]) -> Self {
        Self {
            options,
            compare,
            filter_key,
            out: Vec::new(),
            data: BlockBuilder::new(options.restart_interval, compare),
            index: BlockBuilder::new(1, compare),
            filter: BloomBuilder::new(options.bloom_bits_per_key),
            last_key: Vec::new(),
            entries: 0,
        }
    }

    /// Appends an entry.
    ///
    /// # Errors
    ///
    /// [`Error::Malformed`] when `key` does not sort after the previous key;
    /// [`Error::TooLarge`] for a key or value longer than a u32 length.
    pub fn add(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        if self.entries > 0 && (self.compare)(key, &self.last_key) != Ordering::Greater {
            return Err(Error::Malformed {
                what: "table",
                reason: "keys are not strictly increasing",
            });
        }
        if !self.data.is_empty() && self.data.estimated_size() >= self.options.block_bytes {
            self.close_data_block()?;
        }
        self.data.add(key, value)?;
        self.filter.add((self.filter_key)(key));
        self.last_key.clear();
        self.last_key.extend_from_slice(key);
        self.entries = self.entries.saturating_add(1);
        Ok(())
    }

    /// Entries added so far.
    pub fn entries(&self) -> u64 {
        self.entries
    }

    /// Bytes the table would take if finished now, roughly.
    pub fn estimated_size(&self) -> usize {
        self.out.len().saturating_add(self.data.estimated_size())
    }

    /// The finished table file.
    ///
    /// # Errors
    ///
    /// [`Error::TooLarge`] when a block outgrows the format's length fields.
    pub fn finish(mut self) -> Result<Vec<u8>> {
        if !self.data.is_empty() {
            self.close_data_block()?;
        }
        let filter = write_block(&mut self.out, &self.filter.finish(), Compression::None);
        let index = write_block(&mut self.out, &self.index.finish(), Compression::None);
        let mut properties = BlockBuilder::new(1, |left, right| left.cmp(right));
        properties.add(ENTRIES_PROPERTY, &self.entries.to_le_bytes())?;
        let properties = write_block(&mut self.out, &properties.finish(), Compression::None);
        let footer = Footer {
            filter,
            index,
            properties,
        };
        self.out.extend_from_slice(&footer.encode());
        Ok(self.out)
    }

    /// Writes the current data block and indexes it under its last key.
    fn close_data_block(&mut self) -> Result<()> {
        let payload = self.data.finish();
        let handle = write_block(&mut self.out, &payload, self.options.compression);
        self.index.add(&self.last_key, &handle.encode())
    }
}
