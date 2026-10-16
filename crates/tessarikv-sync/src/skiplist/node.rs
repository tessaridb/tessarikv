//! A node's record in the byte arena: its header, its tower of links, its key and its value.
//!
//! Record layout, little-endian, 8-aligned: `[key_len: u32][value_len: u32][height: u32]
//! [reserved: u32][tower: height × u64][key][value]`. The header, key and value are written
//! before the node is linked and never written again; the tower words are links, read and
//! written only atomically, and share a cache line with the header and a short key.

use super::search::Position;
use super::{KeyComparator, SkipList};
use crate::error::ArenaError;
use crate::shim::{AtomicU64, ByteCells};

/// Bytes before the tower in a record.
const HEADER_BYTES: usize = 16;

/// Bytes per tower link.
const LINK_BYTES: usize = 8;

/// A node's record, located once: its reference, its storage and its fixed fields.
#[derive(Clone, Copy)]
pub(super) struct Node<'a> {
    /// The node's arena reference; never 0 (0 is the null link).
    pub(super) reference: u64,
    /// The chunk holding the record, and the record's offset in it.
    cells: &'a ByteCells,
    offset: usize,
    /// Levels the node is linked at; its tower has this many links.
    pub(super) height: u32,
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

impl<'a> Node<'a> {
    /// The record's bytes `relative..relative + len`, borrowed for as long as the arena lives.
    #[cfg(not(loom))]
    pub(super) fn slice(&self, relative: usize, len: u32) -> Option<&'a [u8]> {
        let at = self.offset.checked_add(relative)?;
        // SAFETY: the node was reached through an Acquire load of the link whose Release CAS
        // published the complete record; its header, key and value are never written again.
        unsafe { self.cells.slice(at, usize::try_from(len).ok()?) }
    }

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
    ) -> Result<Node<'_>, ArenaError> {
        let height = u32::try_from(height).map_err(|_| ArenaError::TooLarge)?;
        let key_len = u32::try_from(key.len()).map_err(|_| ArenaError::TooLarge)?;
        let value_len = u32::try_from(value.len()).map_err(|_| ArenaError::TooLarge)?;
        let key_at = tower_bytes(height)
            .and_then(|tower| tower.checked_add(HEADER_BYTES))
            .ok_or(ArenaError::TooLarge)?;
        let value_at = key_at.checked_add(key.len()).ok_or(ArenaError::TooLarge)?;
        let total = value_at
            .checked_add(value.len())
            .ok_or(ArenaError::TooLarge)?;
        let (reference, cells, offset) = self.bytes.allocate(total)?;
        let mut header = [0_u8; HEADER_BYTES];
        header[..4].copy_from_slice(&key_len.to_le_bytes());
        header[4..8].copy_from_slice(&value_len.to_le_bytes());
        header[8..12].copy_from_slice(&height.to_le_bytes());
        let key_at = offset.checked_add(key_at).ok_or(ArenaError::TooLarge)?;
        let value_at = offset.checked_add(value_at).ok_or(ArenaError::TooLarge)?;
        // SAFETY: the allocation is this thread's alone until the linking CAS publishes it, and
        // these ranges leave out the tower words, which are only ever accessed atomically.
        unsafe {
            cells.write(offset, &header).ok_or(ArenaError::TooLarge)?;
            cells.write(key_at, key).ok_or(ArenaError::TooLarge)?;
            cells.write(value_at, value).ok_or(ArenaError::TooLarge)?;
        }
        Ok(Node {
            reference,
            cells,
            offset,
            height,
            key_len,
            value_len,
        })
    }

    /// The node behind a published `reference`.
    pub(super) fn node(&self, reference: u64) -> Option<Node<'_>> {
        let (cells, offset) = self.bytes.resolve(reference)?;
        let mut header = [0_u8; HEADER_BYTES];
        // SAFETY: `reference` was loaded with Acquire from a link whose Release CAS published the
        // complete record; the header is never written again.
        unsafe { cells.read(offset, &mut header) }?;
        Some(Node {
            reference,
            cells,
            offset,
            key_len: u32_at(&header, 0)?,
            value_len: u32_at(&header, 4)?,
            height: u32_at(&header, 8)?,
        })
    }

    /// Runs `visit` over the node's key.
    pub(super) fn with_key<R>(&self, node: &Node<'_>, visit: impl FnOnce(&[u8]) -> R) -> Option<R> {
        let key_at = node.offset.checked_add(node.key_offset()?)?;
        let len = usize::try_from(node.key_len).ok()?;
        // SAFETY: as in `node`: published bytes, never written again.
        unsafe { node.cells.with_slice(key_at, len, visit) }
    }

    /// Runs `visit` over the node's value.
    #[cfg(all(test, loom))]
    pub(super) fn with_value<R>(
        &self,
        node: &Node<'_>,
        visit: impl FnOnce(&[u8]) -> R,
    ) -> Option<R> {
        let value_at = node.offset.checked_add(node.value_offset()?)?;
        let len = usize::try_from(node.value_len).ok()?;
        // SAFETY: as in `node`: published bytes, never written again.
        unsafe { node.cells.with_slice(value_at, len, visit) }
    }

    /// The link of `level` leaving `from`: a head link, or a word of a node's tower; `None`
    /// above the node's height.
    pub(super) fn link<'a>(&'a self, from: Position<'a>, level: usize) -> Option<&'a AtomicU64> {
        let Some(node) = from.node else {
            return self.head.get(level);
        };
        if level >= usize::try_from(node.height).ok()? {
            return None;
        }
        let at = node
            .offset
            .checked_add(HEADER_BYTES)?
            .checked_add(level.checked_mul(LINK_BYTES)?)?;
        // SAFETY: `at` is a tower word of this record (below its height, 8-aligned because
        // records and the header are), and tower words are never read or written as bytes.
        unsafe { node.cells.atomic_word(at) }
    }
}
