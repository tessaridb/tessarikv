//! A node's record in the byte arena and its tower in the link arena.
//!
//! Record layout, little-endian: `[link_index: u64][key_len: u32][value_len: u32][height: u32]
//! [reserved: u32][key][value]`. Written completely before the node is linked, never written again.

use super::{KeyComparator, SkipList};
use crate::error::ArenaError;
use crate::shim::AtomicU64;

/// Bytes before the key in a record.
pub(super) const HEADER_BYTES: usize = 24;

/// The fixed part of a published node, read from its record.
#[derive(Debug, Clone, Copy)]
pub(super) struct Node {
    /// The node's arena reference; never 0 (0 is the null link).
    pub(super) reference: u64,
    /// Index of the node's level-0 link slot; level `i` is at `link_index + i`.
    pub(super) link_index: u64,
    pub(super) key_len: u32,
    #[cfg_attr(
        all(loom, not(test)),
        expect(
            dead_code,
            reason = "under loom only the test-build accessors read the value"
        )
    )]
    pub(super) value_len: u32,
}

/// Reads a little-endian `u32` at `at` of `header`.
fn u32_at(header: &[u8; HEADER_BYTES], at: usize) -> Option<u32> {
    Some(u32::from_le_bytes(
        header.get(at..at.checked_add(4)?)?.try_into().ok()?,
    ))
}

impl<C: KeyComparator> SkipList<C> {
    /// Allocates and fills a record and a zeroed tower for an unpublished node.
    pub(super) fn write_node(
        &self,
        key: &[u8],
        value: &[u8],
        height: usize,
    ) -> Result<Node, ArenaError> {
        let key_len = u32::try_from(key.len()).map_err(|_| ArenaError::TooLarge)?;
        let value_len = u32::try_from(value.len()).map_err(|_| ArenaError::TooLarge)?;
        let height_u32 = u32::try_from(height).map_err(|_| ArenaError::TooLarge)?;
        let link_index = self.links.allocate(height_u32)?;
        let total = HEADER_BYTES
            .checked_add(key.len())
            .and_then(|len| len.checked_add(value.len()))
            .ok_or(ArenaError::TooLarge)?;
        let (reference, cells, offset) = self.bytes.allocate(total)?;
        let mut header = [0_u8; HEADER_BYTES];
        header[..8].copy_from_slice(&link_index.to_le_bytes());
        header[8..12].copy_from_slice(&key_len.to_le_bytes());
        header[12..16].copy_from_slice(&value_len.to_le_bytes());
        header[16..20].copy_from_slice(&height_u32.to_le_bytes());
        let key_at = offset
            .checked_add(HEADER_BYTES)
            .ok_or(ArenaError::TooLarge)?;
        let value_at = key_at.checked_add(key.len()).ok_or(ArenaError::TooLarge)?;
        // SAFETY: the allocation is this thread's alone until the linking CAS publishes it.
        unsafe {
            cells.write(offset, &header).ok_or(ArenaError::TooLarge)?;
            cells.write(key_at, key).ok_or(ArenaError::TooLarge)?;
            cells.write(value_at, value).ok_or(ArenaError::TooLarge)?;
        }
        Ok(Node {
            reference,
            link_index,
            key_len,
            value_len,
        })
    }

    /// The node behind a published `reference`.
    pub(super) fn node(&self, reference: u64) -> Option<Node> {
        let (cells, offset) = self.bytes.resolve(reference)?;
        let mut header = [0_u8; HEADER_BYTES];
        // SAFETY: `reference` was loaded with Acquire from a link whose Release CAS published the
        // complete record; records are never written again.
        unsafe { cells.read(offset, &mut header) }?;
        Some(Node {
            reference,
            link_index: u64::from_le_bytes(header.get(..8)?.try_into().ok()?),
            key_len: u32_at(&header, 8)?,
            value_len: u32_at(&header, 12)?,
        })
    }

    /// Runs `visit` over the node's key.
    pub(super) fn with_key<R>(&self, node: &Node, visit: impl FnOnce(&[u8]) -> R) -> Option<R> {
        let (cells, offset) = self.bytes.resolve(node.reference)?;
        let key_at = offset.checked_add(HEADER_BYTES)?;
        let len = usize::try_from(node.key_len).ok()?;
        // SAFETY: as in `node`: published, immutable bytes.
        unsafe { cells.with_slice(key_at, len, visit) }
    }

    /// Runs `visit` over the node's value.
    #[cfg(all(test, loom))]
    pub(super) fn with_value<R>(&self, node: &Node, visit: impl FnOnce(&[u8]) -> R) -> Option<R> {
        let (cells, offset) = self.bytes.resolve(node.reference)?;
        let key_len = usize::try_from(node.key_len).ok()?;
        let value_at = offset.checked_add(HEADER_BYTES)?.checked_add(key_len)?;
        let len = usize::try_from(node.value_len).ok()?;
        // SAFETY: as in `node`: published, immutable bytes.
        unsafe { cells.with_slice(value_at, len, visit) }
    }

    /// The link slot of `level` leaving `from` (0 = the head).
    pub(super) fn link(&self, from: u64, link_index: u64, level: usize) -> Option<&AtomicU64> {
        if from == 0 {
            return self.head.get(level);
        }
        self.links
            .slot(link_index.checked_add(u64::try_from(level).ok()?)?)
    }
}
