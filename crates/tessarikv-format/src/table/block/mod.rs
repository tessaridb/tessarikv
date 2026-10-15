//! Data and index blocks: sorted key-value entries with shared key prefixes.
//!
//! A block is a run of entries followed by its restart array:
//!
//! ```text
//! entry*  [restart offset: u32]*  [restart count: u32]
//! entry = [shared: varint][unshared: varint][value_len: varint][key suffix][value]
//! ```
//!
//! An entry stores only the part of its key that differs from the previous key: the first
//! `shared` bytes are the previous key's. Every `restart_interval` entries the prefix sharing
//! restarts (`shared` is 0) and the entry's offset is recorded in the restart array, so a seek
//! binary-searches the restart points and then walks forward through at most one interval.
//! Integers in the trailer are little-endian u32.
//!
//! A block is decoded as untrusted input: the restart array is checked against the block before
//! anything else, and an entry that points past the entries or claims more shared bytes than the
//! previous key has stops the iterator with an error instead of reading out of bounds.

mod iter;

#[cfg(test)]
mod tests;

use std::cmp::Ordering;
use std::sync::Arc;

use crate::error::{Error, Result};
use crate::varint;

pub use iter::BlockIter;

/// How keys in a block are ordered.
pub type Compare = fn(&[u8], &[u8]) -> Ordering;

/// Bytes of one restart offset, and of the restart count.
const U32_BYTES: usize = 4;

/// Builds one block from entries added in strictly increasing key order.
#[derive(Debug)]
pub struct BlockBuilder {
    buffer: Vec<u8>,
    restarts: Vec<u32>,
    restart_interval: usize,
    since_restart: usize,
    last_key: Vec<u8>,
    compare: Compare,
}

impl BlockBuilder {
    /// An empty block that restarts prefix sharing every `restart_interval` entries (at least
    /// one), ordered by `compare`.
    pub fn new(restart_interval: usize, compare: Compare) -> Self {
        Self {
            buffer: Vec::new(),
            restarts: Vec::new(),
            restart_interval: restart_interval.max(1),
            since_restart: 0,
            last_key: Vec::new(),
            compare,
        }
    }

    /// Appends an entry.
    ///
    /// # Errors
    ///
    /// [`Error::Malformed`] when `key` does not sort after the previous key;
    /// [`Error::TooLarge`] for a key or value longer than a u32 length.
    pub fn add(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let first = self.restarts.is_empty();
        if !first && (self.compare)(key, &self.last_key) != Ordering::Greater {
            return Err(Error::Malformed {
                what: "block",
                reason: "keys are not strictly increasing",
            });
        }
        let shared = if first || self.since_restart >= self.restart_interval {
            self.restarts.push(length(self.buffer.len(), "block")?);
            self.since_restart = 0;
            0
        } else {
            common_prefix(&self.last_key, key)
        };
        let suffix = key.get(shared..).unwrap_or_default();
        varint::put_u32(&mut self.buffer, length(shared, "block key")?);
        varint::put_u32(&mut self.buffer, length(suffix.len(), "block key")?);
        varint::put_u32(&mut self.buffer, length(value.len(), "block value")?);
        self.buffer.extend_from_slice(suffix);
        self.buffer.extend_from_slice(value);
        self.last_key.truncate(shared);
        self.last_key.extend_from_slice(suffix);
        self.since_restart = self.since_restart.saturating_add(1);
        Ok(())
    }

    /// Whether no entry has been added since the last [`finish`](Self::finish).
    pub fn is_empty(&self) -> bool {
        self.restarts.is_empty()
    }

    /// Bytes the block would take if finished now.
    pub fn estimated_size(&self) -> usize {
        let trailer = self
            .restarts
            .len()
            .saturating_add(1)
            .saturating_mul(U32_BYTES);
        self.buffer.len().saturating_add(trailer)
    }

    /// The finished block; the builder is empty again afterwards.
    pub fn finish(&mut self) -> Vec<u8> {
        let mut block = std::mem::take(&mut self.buffer);
        for restart in &self.restarts {
            block.extend_from_slice(&restart.to_le_bytes());
        }
        let count = u32::try_from(self.restarts.len()).unwrap_or(u32::MAX);
        block.extend_from_slice(&count.to_le_bytes());
        self.restarts.clear();
        self.since_restart = 0;
        self.last_key.clear();
        block
    }
}

/// A decoded block, ready to be searched. Cloning it shares its bytes.
#[derive(Debug, Clone)]
pub struct Block {
    bytes: Arc<[u8]>,
    /// Where the entries end and the restart array begins.
    entries_end: usize,
    restart_count: usize,
    compare: Compare,
}

impl Block {
    /// Decodes `bytes` as a block ordered by `compare`.
    ///
    /// # Errors
    ///
    /// [`Error::Truncated`] for fewer bytes than a trailer; [`Error::Malformed`] for a restart
    /// array that does not fit the block, or restart offsets out of order or past the entries.
    pub fn new(bytes: Vec<u8>, compare: Compare) -> Result<Self> {
        let malformed = |reason| Error::Malformed {
            what: "block",
            reason,
        };
        let Some(count_at) = bytes.len().checked_sub(U32_BYTES) else {
            return Err(Error::Truncated {
                what: "block",
                needed: 4,
                available: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
            });
        };
        let restart_count =
            usize::try_from(read_u32(&bytes, count_at).ok_or(malformed("trailer"))?)
                .map_err(|_| malformed("restart count"))?;
        let entries_end = restart_count
            .checked_mul(U32_BYTES)
            .and_then(|size| count_at.checked_sub(size))
            .ok_or(malformed("restart array larger than the block"))?;
        let mut previous = None;
        for index in 0..restart_count {
            let at = index
                .checked_mul(U32_BYTES)
                .and_then(|offset| entries_end.checked_add(offset))
                .ok_or(malformed("restart array"))?;
            let restart = usize::try_from(read_u32(&bytes, at).ok_or(malformed("restart array"))?)
                .map_err(|_| malformed("restart offset"))?;
            if restart >= entries_end || previous.is_some_and(|before| restart <= before) {
                return Err(malformed(
                    "restart offsets out of order or past the entries",
                ));
            }
            previous = Some(restart);
        }
        if restart_count == 0 && entries_end != 0 {
            return Err(malformed("entries without a restart point"));
        }
        if restart_count > 0 && previous_first(&bytes, entries_end) != Some(0) {
            return Err(malformed("the first restart point is not the first entry"));
        }
        Ok(Self {
            bytes: bytes.into(),
            entries_end,
            restart_count,
            compare,
        })
    }

    /// An iterator over the block's entries, positioned nowhere until a seek. It shares the
    /// block's bytes, so it may outlive this handle.
    pub fn iter(&self) -> BlockIter {
        BlockIter::new(self.clone())
    }

    /// The offset of restart point `index`.
    fn restart(&self, index: usize) -> Option<usize> {
        let at = index
            .checked_mul(U32_BYTES)
            .and_then(|offset| self.entries_end.checked_add(offset))?;
        usize::try_from(read_u32(&self.bytes, at)?).ok()
    }
}

/// The first restart offset, which must be the first entry.
fn previous_first(bytes: &[u8], entries_end: usize) -> Option<usize> {
    usize::try_from(read_u32(bytes, entries_end)?).ok()
}

/// The little-endian u32 at `at`.
fn read_u32(bytes: &[u8], at: usize) -> Option<u32> {
    let field = bytes.get(at..at.checked_add(U32_BYTES)?)?;
    Some(u32::from_le_bytes(field.try_into().ok()?))
}

/// A length as a u32 field.
fn length(len: usize, what: &'static str) -> Result<u32> {
    u32::try_from(len).map_err(|_| Error::TooLarge {
        what,
        size: u64::try_from(len).unwrap_or(u64::MAX),
        limit: u64::from(u32::MAX),
    })
}

/// Bytes `left` and `right` share at their start.
fn common_prefix(left: &[u8], right: &[u8]) -> usize {
    left.iter()
        .zip(right)
        .take_while(|(left, right)| left == right)
        .count()
}
