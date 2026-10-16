//! Opening a table: its footer and metadata blocks, read once and kept in memory.

use std::sync::Arc;

use super::block::{Block, Compare};
use super::builder::{ENTRIES_PROPERTY, RANGE_DELETES_PROPERTY};
use super::filter::BloomFilter;
use super::handle::{BlockHandle, FOOTER_BYTES, Footer, unseal};
use super::iter::TableIter;
use super::source::{BlockSource, OpenError, Resident};
use crate::error::{Error, Result};

/// An open table: its index, filter and properties in memory, its data blocks read through a
/// [`BlockSource`]. Cloning it shares everything it holds.
#[derive(Debug, Clone)]
pub struct Table {
    source: Arc<dyn BlockSource>,
    index: Block,
    filter: Arc<BloomFilter>,
    filter_key: fn(&[u8]) -> &[u8],
    compare: Compare,
    entries: u64,
    range_deletes: Option<Block>,
}

impl Table {
    /// Opens the table in `file`, ordered by `compare`, whose filter was built from
    /// `filter_key` of each key. The whole file stays in memory, so every data block is always
    /// at hand. The footer, index, filter and properties are verified now; each data block is
    /// verified when it is read.
    ///
    /// # Errors
    ///
    /// [`Error::Truncated`], [`Error::Malformed`] or [`Error::UnsupportedVersion`] for a file
    /// that is not a whole table of this format.
    pub fn open(file: Vec<u8>, compare: Compare, filter_key: fn(&[u8]) -> &[u8]) -> Result<Self> {
        let file: Arc<[u8]> = file.into();
        let len = u64::try_from(file.len()).unwrap_or(u64::MAX);
        let resident = Arc::new(Resident {
            file: Arc::clone(&file),
            compare,
        });
        let read = |offset: u64, buf: &mut [u8]| {
            let copied = usize::try_from(offset)
                .ok()
                .and_then(|start| file.get(start..start.checked_add(buf.len())?))
                .map(|bytes| buf.copy_from_slice(bytes));
            copied.ok_or(())
        };
        Self::open_with(len, read, resident, compare, filter_key).map_err(|error| match error {
            OpenError::Format(error) => error,
            // Reads come from the bytes themselves and stay inside them: the footer and every
            // handle are checked against the length before a read.
            OpenError::Read(()) => Error::Malformed {
                what: "table",
                reason: "a block lies outside the file",
            },
        })
    }

    /// Opens a table file of `len` bytes through `read`, which fills a buffer from a position
    /// of the file; data blocks then come from `source`. The footer, index, filter, properties
    /// and range deletes are read and verified now, once, and stay in memory. Every position
    /// read is checked against `len` first, so a damaged footer cannot ask for more bytes than
    /// the file holds.
    ///
    /// # Errors
    ///
    /// [`OpenError::Read`] with `read`'s error, and [`OpenError::Format`] for a file that is not
    /// a whole table of this format.
    pub fn open_with<E>(
        len: u64,
        mut read: impl FnMut(u64, &mut [u8]) -> std::result::Result<(), E>,
        source: Arc<dyn BlockSource>,
        compare: Compare,
        filter_key: fn(&[u8]) -> &[u8],
    ) -> std::result::Result<Self, OpenError<E>> {
        let footer_bytes = u64::try_from(FOOTER_BYTES).unwrap_or(u64::MAX);
        let Some(footer_at) = len.checked_sub(footer_bytes) else {
            return Err(OpenError::Format(Error::Truncated {
                what: "table",
                needed: footer_bytes,
                available: len,
            }));
        };
        let mut tail = [0; FOOTER_BYTES];
        read(footer_at, &mut tail).map_err(OpenError::Read)?;
        let footer = Footer::decode(&tail).map_err(OpenError::Format)?;
        let mut block = |handle: BlockHandle| -> std::result::Result<Vec<u8>, OpenError<E>> {
            let end = handle.end_within(footer_at).map_err(OpenError::Format)?;
            let size = usize::try_from(end.saturating_sub(handle.offset())).map_err(|_| {
                OpenError::Format(Error::Malformed {
                    what: "table block",
                    reason: "larger than memory can address",
                })
            })?;
            let mut sealed = vec![0; size];
            read(handle.offset(), &mut sealed).map_err(OpenError::Read)?;
            unseal(&sealed).map_err(OpenError::Format)
        };
        let index = block(footer.index)?;
        let filter = block(footer.filter)?;
        let properties = block(footer.properties)?;
        let (entries, range_deletes) = properties_of(properties).map_err(OpenError::Format)?;
        let range_deletes = range_deletes.map(&mut block).transpose()?;
        let decoded = (|| {
            Ok(Self {
                source,
                index: Block::new(index, compare)?,
                filter: Arc::new(BloomFilter::with_probing(filter, footer.probing)?),
                filter_key,
                compare,
                entries,
                range_deletes: range_deletes
                    .map(|payload| Block::new(payload, compare))
                    .transpose()?,
            })
        })();
        decoded.map_err(OpenError::Format)
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

    /// Where the data block that would hold `key` starts in the file, read from the index alone;
    /// the end of the data when every key is below `key`. The bytes between two keys are the
    /// difference of their offsets, to within a block.
    ///
    /// # Errors
    ///
    /// [`Error::Malformed`] for a damaged index entry.
    pub fn offset_of(&self, key: &[u8]) -> Result<u64> {
        let mut index = self.index.iter();
        index.seek(key);
        if index.valid() {
            return Ok(BlockHandle::decode(index.value())?.offset());
        }
        index.status()?;
        index.seek_to_last();
        if !index.valid() {
            index.status()?;
            return Ok(0);
        }
        let last = BlockHandle::decode(index.value())?;
        last.offset()
            .checked_add(last.size())
            .ok_or(Error::Malformed {
                what: "table index",
                reason: "a block ends past the largest offset",
            })
    }

    /// An iterator over the table, positioned nowhere until a seek.
    pub fn iter(&self) -> TableIter {
        TableIter::new(Arc::clone(&self.source), self.compare, self.index.iter())
    }
}

/// The entry count and the range-delete block's handle, from the properties block.
fn properties_of(payload: Vec<u8>) -> Result<(u64, Option<BlockHandle>)> {
    let properties = Block::new(payload, |left, right| left.cmp(right))?;
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
        Some(BlockHandle::decode(property.value())?)
    } else {
        None
    };
    property.status()?;
    Ok((entries, range_deletes))
}
