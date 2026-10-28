//! The prefix filter: a second filter over the leading bytes of a table's keys, written when the
//! table's options name a prefix length.
//!
//! The prefix of a key is its first `length` bytes; a key shorter than that has none and is not
//! recorded. Each distinct prefix enters the filter once (keys arrive in order, so equal prefixes
//! arrive together). The filter's block follows the table's other metadata blocks, and the
//! properties block names it under [`PREFIX_FILTER_PROPERTY`] with the length it was built for:
//! a table without that entry has no prefix filter, which is how every table written before this
//! filter existed reads.
//!
//! The filter rules a table out for a read only when the read's own bounds prove that every key
//! it may return starts with one prefix, and the filter says that prefix is absent. A key shorter
//! than the prefix never lies inside such a range: it is either a proper prefix of the range's
//! prefix, and so below the range, or differs from it within its own length, and so lies below
//! or past it.

use tessarikv_constants::MAX_PREFIX_LENGTH;

use super::filter::{BloomBuilder, BloomFilter, Probing};
use super::handle::BlockHandle;
use crate::error::{Error, Result};

/// The properties block's key for the prefix filter's handle and prefix length.
pub(super) const PREFIX_FILTER_PROPERTY: &[u8] = b"prefix_filter";

/// Bytes of the property's value: a block handle, then the prefix length as a little-endian u32.
const ENTRY_BYTES: usize = 20;

/// Collects the distinct prefixes of the keys a table is built from.
#[derive(Debug)]
pub(super) struct PrefixFilterBuilder {
    length: u32,
    bloom: BloomBuilder,
    /// The last prefix added, so a run of keys sharing one adds it once.
    last: Vec<u8>,
    added: bool,
}

impl PrefixFilterBuilder {
    /// A builder for prefixes of `length` bytes with `bits_per_key` bits each; `None` when the
    /// table keeps no filters (zero bits per key).
    pub(super) fn new(length: u32, bits_per_key: u32) -> Option<Self> {
        (bits_per_key > 0).then(|| Self {
            length,
            bloom: BloomBuilder::new(bits_per_key),
            last: Vec::new(),
            added: false,
        })
    }

    /// Records the prefix of `key`, which follows every key recorded before in key order.
    pub(super) fn add(&mut self, key: &[u8]) {
        let Some(prefix) = usize::try_from(self.length)
            .ok()
            .and_then(|length| key.get(..length))
        else {
            return;
        };
        if self.added && prefix == self.last.as_slice() {
            return;
        }
        self.bloom.add(prefix);
        self.last.clear();
        self.last.extend_from_slice(prefix);
        self.added = true;
    }

    /// The filter block's payload, or `None` when no key had a prefix.
    pub(super) fn finish(&mut self) -> Option<Vec<u8>> {
        self.added.then(|| self.bloom.finish())
    }

    /// The property value naming the filter block at `handle`.
    pub(super) fn entry(&self, handle: BlockHandle) -> [u8; ENTRY_BYTES] {
        let mut entry = [0; ENTRY_BYTES];
        entry[..16].copy_from_slice(&handle.encode());
        entry[16..].copy_from_slice(&self.length.to_le_bytes());
        entry
    }
}

/// The filter block's handle and prefix length, from the property's value.
///
/// # Errors
///
/// [`Error::Malformed`] for a value of the wrong size or a length out of range.
pub(super) fn decode_entry(value: &[u8]) -> Result<(BlockHandle, u32)> {
    let malformed = |reason| Error::Malformed {
        what: "table properties",
        reason,
    };
    if value.len() != ENTRY_BYTES {
        return Err(malformed(
            "prefix filter entry is not a handle and a length",
        ));
    }
    let length = value
        .get(16..)
        .and_then(|bytes| bytes.try_into().ok())
        .map(u32::from_le_bytes)
        .ok_or(malformed(
            "prefix filter entry is not a handle and a length",
        ))?;
    if length == 0 || length > MAX_PREFIX_LENGTH {
        return Err(malformed("prefix length out of range"));
    }
    Ok((BlockHandle::decode(value)?, length))
}

/// An open table's prefix filter.
#[derive(Debug)]
pub(super) struct PrefixFilter {
    length: usize,
    bloom: BloomFilter,
}

impl PrefixFilter {
    /// Decodes the filter block `payload`, built for prefixes of `length` bytes and probed as
    /// `probing` says.
    ///
    /// # Errors
    ///
    /// As [`BloomFilter::new`] for a damaged block.
    pub(super) fn new(payload: Vec<u8>, length: u32, probing: Probing) -> Result<Self> {
        Ok(Self {
            length: usize::try_from(length).map_err(|_| Error::Malformed {
                what: "table properties",
                reason: "prefix length out of range",
            })?,
            bloom: BloomFilter::with_probing(payload, probing)?,
        })
    }

    /// Whether the table may hold a key in `[low, high)`: `false` only when every such key
    /// starts with one prefix the filter rules out.
    pub(super) fn may_hold(&self, low: &[u8], high: Option<&[u8]>) -> bool {
        let (Some(prefix), Some(high)) = (low.get(..self.length), high) else {
            return true;
        };
        // Past the prefix's successor (or with none), the range reaches other prefixes.
        if !at_most_successor(high, prefix) {
            return true;
        }
        self.bloom.may_contain(prefix)
    }
}

/// Whether `high` is at most the least key above every key that starts with `prefix` (the
/// prefix with its last byte below `0xff` incremented and the rest dropped); `false` when there
/// is no such key (the prefix is all `0xff`). Compares without building the successor.
fn at_most_successor(high: &[u8], prefix: &[u8]) -> bool {
    let Some(last) = prefix.iter().rposition(|&byte| byte != u8::MAX) else {
        return false;
    };
    let (Some(head), Some(&byte)) = (prefix.get(..last), prefix.get(last)) else {
        return false;
    };
    let Some(bumped) = byte.checked_add(1) else {
        return false;
    };
    let shared = high.len().min(last);
    match high.get(..shared).cmp(&head.get(..shared)) {
        std::cmp::Ordering::Less => return true,
        std::cmp::Ordering::Greater => return false,
        std::cmp::Ordering::Equal => {}
    }
    // `high` agrees with the successor so far; a shorter `high` is a prefix of it.
    match high.get(last) {
        None => true,
        Some(&next) if next < bumped => true,
        Some(&next) if next > bumped => false,
        // Equal through the successor's end: at most it only when it ends there too.
        Some(_) => high.len() == last.saturating_add(1),
    }
}

/// [`at_most_successor`], for the tests.
#[cfg(test)]
pub(super) fn at_most_successor_for_tests(high: &[u8], prefix: &[u8]) -> bool {
    at_most_successor(high, prefix)
}
