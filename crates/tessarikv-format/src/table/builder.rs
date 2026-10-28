//! Writing a table.

use std::cmp::Ordering;
use std::sync::Arc;

use super::block::{BlockBuilder, Compare};
use super::filter::{FilterBuilder, Probing};
use super::handle::{Footer, write_block};
use super::prefix::{PREFIX_FILTER_PROPERTY, PrefixFilterBuilder};
use super::{Compression, TableOptions};
use crate::error::{Error, Result};
use crate::seal::{FileSealer, SealHeader};

/// The properties block's key for the blob files the table's entries point into: their
/// numbers, each a little-endian u64, in increasing order.
pub(super) const BLOB_FILES_PROPERTY: &[u8] = b"blob_files";

/// The properties block's key for the entry count.
pub(super) const ENTRIES_PROPERTY: &[u8] = b"entries";

/// The properties block's key for the handle of the range-delete block, present when the table
/// holds range deletes.
pub(super) const RANGE_DELETES_PROPERTY: &[u8] = b"range_deletes";

/// Builds a table from entries added in strictly increasing key order.
#[derive(Debug)]
pub struct TableBuilder {
    options: TableOptions,
    compare: Compare,
    filter_key: fn(&[u8]) -> &[u8],
    /// Finished bytes not yet taken; they follow the `drained` bytes taken before.
    out: Vec<u8>,
    drained: u64,
    data: BlockBuilder,
    index: BlockBuilder,
    filter: FilterBuilder,
    last_key: Vec<u8>,
    entries: u64,
    range_deletes: BlockBuilder,
    last_range_key: Vec<u8>,
    prefixes: Option<PrefixFilterBuilder>,
    blob_files: Vec<u64>,
    sealer: Option<Arc<FileSealer>>,
    /// A prefix-filter property value written in place of the real one.
    #[cfg(test)]
    prefix_entry: Option<Vec<u8>>,
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
            drained: 0,
            data: BlockBuilder::new(options.restart_interval, compare),
            index: BlockBuilder::new(1, compare),
            filter: FilterBuilder::new(options.filter, options.bloom_bits_per_key),
            last_key: Vec::new(),
            entries: 0,
            range_deletes: BlockBuilder::new(options.restart_interval, compare),
            last_range_key: Vec::new(),
            prefixes: options
                .prefix_length
                .and_then(|length| PrefixFilterBuilder::new(length, options.bloom_bits_per_key)),
            blob_files: Vec::new(),
            sealer: None,
            #[cfg(test)]
            prefix_entry: None,
        }
    }

    /// An empty sealed table, as [`TableBuilder::new`]: the file starts with `header`, and each
    /// block and the footer are encrypted by `sealer`, the key `header` derives for a table.
    pub fn sealed(
        options: TableOptions,
        compare: Compare,
        filter_key: fn(&[u8]) -> &[u8],
        sealer: Arc<FileSealer>,
        header: &SealHeader,
    ) -> Self {
        let mut builder = Self::new(options, compare, filter_key);
        builder.out.extend_from_slice(&header.encode());
        builder.sealer = Some(sealer);
        builder
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
        if let Some(prefixes) = self.prefixes.as_mut() {
            prefixes.add((self.filter_key)(key));
        }
        self.last_key.clear();
        self.last_key.extend_from_slice(key);
        self.entries = self.entries.saturating_add(1);
        Ok(())
    }

    /// Appends a range delete: `key` is where it starts (in the table's key order, as for
    /// [`TableBuilder::add`]), `end` where it stops. Range deletes are kept in a block of their
    /// own, apart from the entries.
    ///
    /// # Errors
    ///
    /// [`Error::Malformed`] when `key` does not sort after the previous range delete's key;
    /// [`Error::TooLarge`] as for [`TableBuilder::add`].
    pub fn add_range_delete(&mut self, key: &[u8], end: &[u8]) -> Result<()> {
        if !self.range_deletes.is_empty()
            && (self.compare)(key, &self.last_range_key) != Ordering::Greater
        {
            return Err(Error::Malformed {
                what: "table",
                reason: "range deletes are not strictly increasing",
            });
        }
        self.range_deletes.add(key, end)?;
        self.last_range_key.clear();
        self.last_range_key.extend_from_slice(key);
        Ok(())
    }

    /// Records that entries of the table point into blob file `file` (see
    /// [`super::BlobIndex`]); [`super::Table::blob_files`] reads the set back.
    pub fn references_blob(&mut self, file: u64) {
        if let Err(at) = self.blob_files.binary_search(&file) {
            self.blob_files.insert(at, file);
        }
    }

    /// Entries added so far.
    pub fn entries(&self) -> u64 {
        self.entries
    }

    /// Bytes the table would take if finished now, roughly.
    pub fn estimated_size(&self) -> usize {
        usize::try_from(self.drained)
            .unwrap_or(usize::MAX)
            .saturating_add(self.out.len())
            .saturating_add(self.data.estimated_size())
    }

    /// The bytes of the table finished so far and not yet taken, which continue the file
    /// after the [`TableBuilder::drained`] bytes taken before. A writer that appends each piece
    /// as it comes, and finally what [`TableBuilder::finish`] returns, writes the same file as
    /// one built whole, while holding only the block being filled and the index and filter.
    pub fn take_finished(&mut self) -> Vec<u8> {
        let taken = std::mem::take(&mut self.out);
        self.drained = self
            .drained
            .saturating_add(u64::try_from(taken.len()).unwrap_or(u64::MAX));
        taken
    }

    /// Bytes taken so far by [`TableBuilder::take_finished`]: where the next piece, or what
    /// [`TableBuilder::finish`] returns, begins in the file.
    pub fn drained(&self) -> u64 {
        self.drained
    }

    /// The rest of the table file: all of it when nothing was taken before.
    ///
    /// # Errors
    ///
    /// [`Error::TooLarge`] when a block outgrows the format's length fields.
    pub fn finish(mut self) -> Result<Vec<u8>> {
        if !self.data.is_empty() {
            self.close_data_block()?;
        }
        let filter = write_block(
            &mut self.out,
            self.drained,
            &self.filter.finish(),
            Compression::None,
            self.sealer.as_deref(),
        )?;
        let index = write_block(
            &mut self.out,
            self.drained,
            &self.index.finish(),
            Compression::None,
            self.sealer.as_deref(),
        )?;
        let range_deletes = (!self.range_deletes.is_empty()).then(|| {
            write_block(
                &mut self.out,
                self.drained,
                &self.range_deletes.finish(),
                Compression::None,
                self.sealer.as_deref(),
            )
        });
        let range_deletes = range_deletes.transpose()?;
        let prefix_entry = self.prefix_filter()?;
        let mut properties = BlockBuilder::new(1, |left, right| left.cmp(right));
        if !self.blob_files.is_empty() {
            let files: Vec<u8> = self
                .blob_files
                .iter()
                .flat_map(|file| file.to_le_bytes())
                .collect();
            properties.add(BLOB_FILES_PROPERTY, &files)?;
        }
        properties.add(ENTRIES_PROPERTY, &self.entries.to_le_bytes())?;
        if let Some(entry) = prefix_entry {
            properties.add(PREFIX_FILTER_PROPERTY, &entry)?;
        }
        if let Some(handle) = range_deletes {
            properties.add(RANGE_DELETES_PROPERTY, &handle.encode())?;
        }
        let properties = write_block(
            &mut self.out,
            self.drained,
            &properties.finish(),
            Compression::None,
            self.sealer.as_deref(),
        )?;
        let footer = Footer {
            filter,
            index,
            properties,
            probing: Probing::Remixed,
        };
        let footer = footer.encode();
        match &self.sealer {
            Some(sealer) => {
                let at = self
                    .drained
                    .saturating_add(u64::try_from(self.out.len()).unwrap_or(u64::MAX));
                self.out.extend_from_slice(&sealer.seal(at, &footer)?);
            }
            None => self.out.extend_from_slice(&footer),
        }
        Ok(self.out)
    }

    /// Writes the prefix filter's block, when there is one, and returns its property value.
    fn prefix_filter(&mut self) -> Result<Option<Vec<u8>>> {
        #[cfg(test)]
        if let Some(entry) = self.prefix_entry.take() {
            return Ok(Some(entry));
        }
        let Some(prefixes) = self.prefixes.as_mut() else {
            return Ok(None);
        };
        let Some(payload) = prefixes.finish() else {
            return Ok(None);
        };
        let handle = write_block(
            &mut self.out,
            self.drained,
            &payload,
            Compression::None,
            self.sealer.as_deref(),
        )?;
        Ok(Some(prefixes.entry(handle).to_vec()))
    }

    /// Writes `value` as the prefix filter's property in place of the real one.
    #[cfg(test)]
    pub(super) fn prefix_entry_for_tests(&mut self, value: &[u8]) {
        self.prefix_entry = Some(value.to_vec());
    }

    /// Writes the current data block and indexes it under its last key.
    fn close_data_block(&mut self) -> Result<()> {
        let payload = self.data.finish();
        let handle = write_block(
            &mut self.out,
            self.drained,
            &payload,
            self.options.compression,
            self.sealer.as_deref(),
        )?;
        self.index.add(&self.last_key, &handle.encode())
    }
}
