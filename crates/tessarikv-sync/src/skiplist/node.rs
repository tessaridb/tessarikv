//! A node's record in the byte arena: its header, its tower of links, its key and its value.
//!
//! Record layout, little-endian, 8-aligned: `[key_len: u32][value_len: u32][height: u32]
//! [reserved: u32][tower: height × u64][key][value]`. The header, key and value are written
//! before the node is linked and never written again; the tower words are links, read and
//! written only atomically, and share a cache line with the header and a short key.

use super::search::Position;
use super::{KeyComparator, SkipList};
use crate::error::ArenaError;
use crate::shim::AtomicU64;

/// Bytes before the tower in a record.
const HEADER_BYTES: usize = 16;

/// Bytes per tower link.
const LINK_BYTES: usize = 8;

/// The fixed part of a published node, read from its record.
#[derive(Debug, Clone, Copy)]
pub(super) struct Node {
    /// The node's arena reference; never 0 (0 is the null link).
    pub(super) reference: u64,
    /// Levels the node is linked at; its tower has this many links.
    pub(super) height: u32,
    pub(super) key_len: u32,
    pub(super) value_len: u32,
}

impl Node {
    /// Where the key starts, relative to the record.
    pub(super) fn key_offset(&self) -> Option<usize> {
        tower_bytes(self.height)?.checked_add(HEADER_BYTES)
    }

    /// Where the value starts, relative to the record.
    #[cfg_attr(
        all(loom, not(test)),
        expect(
            dead_code,
            reason = "under loom only the test-build accessors read the value"
        )
    )]
    pub(super) fn value_offset(&self) -> Option<usize> {
        self.key_offset()?
            .checked_add(usize::try_from(self.key_len).ok()?)
    }
}

/// Bytes of a tower of `height` links.
fn tower_bytes(height: u32) -> Option<usize> {
    usize::try_from(height).ok()?.checked_mul(LINK_BYTES)
}

/// Reads a little-endian `u32` at `at` of `header`.
fn u32_at(header: &[u8; HEADER_BYTES], at: usize) -> Option<u32> {
    Some(u32::from_le_bytes(
        header.get(at..at.checked_add(4)?)?.try_into().ok()?,
    ))
}

impl<C: KeyComparator> SkipList<C> {
    /// Allocates and fills the record of an unpublished node, its tower zeroed.
    pub(super) fn write_node(
        &self,
        key: &[u8],
        value: &[u8],
        height: usize,
    ) -> Result<Node, ArenaError> {
        let node = Node {
            reference: 0,
            height: u32::try_from(height).map_err(|_| ArenaError::TooLarge)?,
            key_len: u32::try_from(key.len()).map_err(|_| ArenaError::TooLarge)?,
            value_len: u32::try_from(value.len()).map_err(|_| ArenaError::TooLarge)?,
        };
        let key_at = node.key_offset().ok_or(ArenaError::TooLarge)?;
        let value_at = key_at.checked_add(key.len()).ok_or(ArenaError::TooLarge)?;
        let total = value_at
            .checked_add(value.len())
            .ok_or(ArenaError::TooLarge)?;
        let (reference, cells, offset) = self.bytes.allocate(total)?;
        let mut header = [0_u8; HEADER_BYTES];
        header[..4].copy_from_slice(&node.key_len.to_le_bytes());
        header[4..8].copy_from_slice(&node.value_len.to_le_bytes());
        header[8..12].copy_from_slice(&node.height.to_le_bytes());
        let key_at = offset.checked_add(key_at).ok_or(ArenaError::TooLarge)?;
        let value_at = offset.checked_add(value_at).ok_or(ArenaError::TooLarge)?;
        // SAFETY: the allocation is this thread's alone until the linking CAS publishes it, and
        // these ranges leave out the tower words, which are only ever accessed atomically.
        unsafe {
            cells.write(offset, &header).ok_or(ArenaError::TooLarge)?;
            cells.write(key_at, key).ok_or(ArenaError::TooLarge)?;
            cells.write(value_at, value).ok_or(ArenaError::TooLarge)?;
        }
        Ok(Node { reference, ..node })
    }

    /// The node behind a published `reference`.
    pub(super) fn node(&self, reference: u64) -> Option<Node> {
        let (cells, offset) = self.bytes.resolve(reference)?;
        let mut header = [0_u8; HEADER_BYTES];
        // SAFETY: `reference` was loaded with Acquire from a link whose Release CAS published the
        // complete record; the header is never written again.
        unsafe { cells.read(offset, &mut header) }?;
        Some(Node {
            reference,
            key_len: u32_at(&header, 0)?,
            value_len: u32_at(&header, 4)?,
            height: u32_at(&header, 8)?,
        })
    }

    /// Runs `visit` over the node's key.
    pub(super) fn with_key<R>(&self, node: &Node, visit: impl FnOnce(&[u8]) -> R) -> Option<R> {
        let (cells, offset) = self.bytes.resolve(node.reference)?;
        let key_at = offset.checked_add(node.key_offset()?)?;
        let len = usize::try_from(node.key_len).ok()?;
        // SAFETY: as in `node`: published bytes, never written again.
        unsafe { cells.with_slice(key_at, len, visit) }
    }

    /// Runs `visit` over the node's value.
    #[cfg(all(test, loom))]
    pub(super) fn with_value<R>(&self, node: &Node, visit: impl FnOnce(&[u8]) -> R) -> Option<R> {
        let (cells, offset) = self.bytes.resolve(node.reference)?;
        let value_at = offset.checked_add(node.value_offset()?)?;
        let len = usize::try_from(node.value_len).ok()?;
        // SAFETY: as in `node`: published bytes, never written again.
        unsafe { cells.with_slice(value_at, len, visit) }
    }

    /// The link of `level` leaving `from`: a head link, or a word of a node's tower; `None`
    /// above the node's height.
    pub(super) fn link(&self, from: Position, level: usize) -> Option<&AtomicU64> {
        if from.reference == 0 {
            return self.head.get(level);
        }
        if level >= usize::try_from(from.height).ok()? {
            return None;
        }
        let (cells, offset) = self.bytes.resolve(from.reference)?;
        let at = offset
            .checked_add(HEADER_BYTES)?
            .checked_add(level.checked_mul(LINK_BYTES)?)?;
        // SAFETY: `at` is a tower word of this record (below its height, 8-aligned because
        // records and the header are), and tower words are never read or written as bytes.
        unsafe { cells.atomic_word(at) }
    }
}
