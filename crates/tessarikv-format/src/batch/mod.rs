//! The write batch: an ordered list of puts, deletes and range deletes that the engine applies
//! atomically.
//!
//! A batch is the unit of atomicity. Every entry in it becomes visible together or not at all,
//! across keyspaces, and the whole batch is logged as one record. Each entry receives its own
//! sequence number when the batch is committed, in the order the entries were added; the batch
//! itself carries no sequence number.
//!
//! The encoding is also the log payload, so it is versioned and bounded:
//!
//! ```text
//! [version: u8 = 2][count: u32]
//! per entry: [kind: u8][keyspace: u32][key_len: u32][key] then, for a put, [value_len: u32][value]
//!            and, for a range delete, [end_len: u32][end]
//! ```
//!
//! All integers are little-endian. Keys are at most [`MAX_KEY_BYTES`], values at most
//! [`MAX_VALUE_BYTES`], and the encoded batch at most [`MAX_BATCH_BYTES`]; an entry that would
//! break a bound is refused and leaves the batch unchanged. Version 1 batches, which hold no range
//! deletes, are still read.

mod codec;

#[cfg(test)]
mod tests;

use tessarikv_constants::{MAX_BATCH_BYTES, MAX_KEY_BYTES, MAX_VALUE_BYTES};
use tessarikv_types::{KeyspaceId, ValueKind};

use crate::{Error, Result};

/// The batch encoding version this build writes.
const FORMAT_VERSION: u8 = 2;

/// The first version, without range deletes; still read.
const FORMAT_VERSION_1: u8 = 1;

/// Bytes before the first entry: the version and the entry count.
const HEADER_BYTES: usize = 5;

/// An ordered list of puts and deletes, applied atomically by one commit.
///
/// # Example
///
/// ```
/// use tessarikv_format::WriteBatch;
/// use tessarikv_types::KeyspaceId;
///
/// let mut batch = WriteBatch::new();
/// batch.put(KeyspaceId::DEFAULT, b"apple", b"red")?;
/// batch.delete(KeyspaceId::DEFAULT, b"pear")?;
/// assert_eq!(batch.len(), 2);
/// # Ok::<(), tessarikv_format::Error>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteBatch {
    /// The encoding, header included; always valid.
    rep: Vec<u8>,
    count: u32,
}

/// One entry of a [`WriteBatch`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BatchEntry<'a> {
    /// The keyspace the entry writes to.
    pub keyspace: KeyspaceId,
    /// [`ValueKind::Value`] for a put, [`ValueKind::Delete`] for a delete,
    /// [`ValueKind::RangeDelete`] for a range delete.
    pub kind: ValueKind,
    /// The user key.
    pub key: &'a [u8],
    /// The value of a put, the end of a range delete; `None` for a delete.
    pub value: Option<&'a [u8]>,
}

impl Default for WriteBatch {
    fn default() -> Self {
        Self::new()
    }
}

impl WriteBatch {
    /// An empty batch.
    #[must_use]
    pub fn new() -> Self {
        let mut rep = Vec::with_capacity(HEADER_BYTES);
        rep.push(FORMAT_VERSION);
        rep.extend_from_slice(&0_u32.to_le_bytes());
        Self { rep, count: 0 }
    }

    /// Adds `key ↦ value` in `keyspace`.
    ///
    /// # Errors
    ///
    /// [`Error::TooLarge`] when the key, the value, the entry count or the encoded batch would
    /// exceed its limit; the batch is then unchanged.
    pub fn put(&mut self, keyspace: KeyspaceId, key: &[u8], value: &[u8]) -> Result<()> {
        self.append(keyspace, ValueKind::Value, key, Some(value))
    }

    /// Removes `key` from `keyspace`: older versions of it become invisible.
    ///
    /// # Errors
    ///
    /// As for [`WriteBatch::put`].
    pub fn delete(&mut self, keyspace: KeyspaceId, key: &[u8]) -> Result<()> {
        self.append(keyspace, ValueKind::Delete, key, None)
    }

    /// Removes every key in `[start, end)` from `keyspace`: older versions of them become
    /// invisible, and a key written later in the range is visible again.
    ///
    /// # Errors
    ///
    /// [`Error::Malformed`] when `start` is not below `end`, and as for [`WriteBatch::put`].
    pub fn delete_range(&mut self, keyspace: KeyspaceId, start: &[u8], end: &[u8]) -> Result<()> {
        if start >= end {
            return Err(empty_range());
        }
        check_size("range end", end.len(), MAX_KEY_BYTES)?;
        self.append(keyspace, ValueKind::RangeDelete, start, Some(end))
    }

    /// The number of entries.
    #[must_use]
    pub fn len(&self) -> u32 {
        self.count
    }

    /// Whether the batch holds no entry.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// The size of the encoding in bytes.
    #[must_use]
    pub fn size_bytes(&self) -> usize {
        self.rep.len()
    }

    /// Removes every entry, keeping the allocation for reuse.
    pub fn clear(&mut self) {
        self.rep.truncate(HEADER_BYTES);
        self.set_count(0);
    }

    /// The entries in the order they were added.
    #[must_use]
    pub fn iter(&self) -> BatchIter<'_> {
        BatchIter {
            rep: &self.rep,
            at: HEADER_BYTES,
        }
    }

    /// The encoding, as logged.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.rep
    }

    /// A batch from its encoding, validated completely.
    ///
    /// # Errors
    ///
    /// [`Error::UnsupportedVersion`] for an unknown version, [`Error::Truncated`] for an input
    /// that ends early, [`Error::TooLarge`] for a length beyond the engine's limits and
    /// [`Error::Malformed`] for an unknown entry kind or bytes after the last entry.
    pub fn from_bytes(rep: Vec<u8>) -> Result<Self> {
        check_size("write batch", rep.len(), MAX_BATCH_BYTES)?;
        let version = codec::read_u8(&rep, 0, "write batch")?;
        if version != FORMAT_VERSION && version != FORMAT_VERSION_1 {
            return Err(Error::UnsupportedVersion {
                what: "write batch",
                version,
            });
        }
        let count = codec::read_u32(&rep, 1, "write batch")?;
        let mut at = HEADER_BYTES;
        for _ in 0..count {
            (_, at) = codec::read_entry(&rep, at, version)?;
        }
        if at != rep.len() {
            return Err(Error::Malformed {
                what: "write batch",
                reason: "bytes follow the last entry",
            });
        }
        Ok(Self { rep, count })
    }

    fn append(
        &mut self,
        keyspace: KeyspaceId,
        kind: ValueKind,
        key: &[u8],
        value: Option<&[u8]>,
    ) -> Result<()> {
        check_size("key", key.len(), MAX_KEY_BYTES)?;
        if let Some(value) = value {
            check_size("value", value.len(), MAX_VALUE_BYTES)?;
        }
        let count = self.count.checked_add(1).ok_or(Error::TooLarge {
            what: "write batch entry count",
            size: u64::from(u32::MAX).saturating_add(1),
            limit: u64::from(u32::MAX),
        })?;
        let entry_bytes = codec::entry_bytes(key, value).ok_or(too_large_batch(usize::MAX))?;
        let total = self
            .rep
            .len()
            .checked_add(entry_bytes)
            .ok_or(too_large_batch(usize::MAX))?;
        check_size("write batch", total, MAX_BATCH_BYTES)?;
        codec::write_entry(&mut self.rep, keyspace, kind, key, value)?;
        self.set_count(count);
        Ok(())
    }

    fn set_count(&mut self, count: u32) {
        self.count = count;
        if let Some(field) = self.rep.get_mut(1..HEADER_BYTES) {
            field.copy_from_slice(&count.to_le_bytes());
        }
    }
}

/// The entries of a [`WriteBatch`], in order.
#[derive(Debug, Clone)]
pub struct BatchIter<'a> {
    rep: &'a [u8],
    at: usize,
}

impl<'a> Iterator for BatchIter<'a> {
    type Item = BatchEntry<'a>;

    fn next(&mut self) -> Option<BatchEntry<'a>> {
        if self.at >= self.rep.len() {
            return None;
        }
        // A batch's encoding is valid by construction, so decoding cannot fail here.
        let version = self.rep.first().copied().unwrap_or(FORMAT_VERSION);
        let (entry, next) = codec::read_entry(self.rep, self.at, version).ok()?;
        self.at = next;
        Some(entry)
    }
}

impl<'a> IntoIterator for &'a WriteBatch {
    type Item = BatchEntry<'a>;
    type IntoIter = BatchIter<'a>;

    fn into_iter(self) -> BatchIter<'a> {
        self.iter()
    }
}

/// Refuses `size` above `limit`.
fn check_size(what: &'static str, size: usize, limit: usize) -> Result<()> {
    if size > limit {
        return Err(Error::TooLarge {
            what,
            size: u64::try_from(size).unwrap_or(u64::MAX),
            limit: u64::try_from(limit).unwrap_or(u64::MAX),
        });
    }
    Ok(())
}

fn too_large_batch(size: usize) -> Error {
    Error::TooLarge {
        what: "write batch",
        size: u64::try_from(size).unwrap_or(u64::MAX),
        limit: u64::try_from(MAX_BATCH_BYTES).unwrap_or(u64::MAX),
    }
}

/// The refusal of a range that holds no key.
pub(super) fn empty_range() -> Error {
    Error::Malformed {
        what: "range delete",
        reason: "the start is not below the end",
    }
}
