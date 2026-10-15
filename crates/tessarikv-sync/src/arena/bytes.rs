//! The byte arena: node records.

use crossbeam_utils::CachePadded;

use crate::error::ArenaError;
use crate::shim::{AtomicU64, ByteCells, Ordering};
use crate::slot_directory::SlotDirectory;

/// A reference to an oversize allocation carries this bit; the rest is its index.
const OVERSIZE_BIT: u64 = 1 << 63;

/// See the module documentation.
pub(crate) struct ByteArena {
    chunk_bytes: u64,
    /// `log2(chunk_bytes)`: a reference's chunk is `reference >> chunk_shift`.
    chunk_shift: u32,
    /// `chunk_bytes - 1`: a reference's offset in its chunk is `reference & chunk_mask`.
    chunk_mask: u64,
    /// Allocations larger than this (a quarter chunk) get their own bytes.
    oversize_threshold: u64,
    cursor: CachePadded<AtomicU64>,
    oversize_count: AtomicU64,
    oversize_bytes: AtomicU64,
    chunks: SlotDirectory<ByteCells>,
    oversize: SlotDirectory<ByteCells>,
}

/// `value` rounded up to a multiple of 8, so every small allocation starts 8-aligned.
fn round_up_8(value: u64) -> Option<u64> {
    value.checked_add(7).map(|padded| padded & !7)
}

impl ByteArena {
    /// An empty arena with chunks of `chunk_bytes`, a power of two of at least 64.
    pub(crate) fn new(chunk_bytes: usize) -> Result<Self, ArenaError> {
        if chunk_bytes < 64 || !chunk_bytes.is_power_of_two() {
            return Err(ArenaError::TooLarge);
        }
        let chunk_bytes_u64 = u64::try_from(chunk_bytes).map_err(|_| ArenaError::TooLarge)?;
        Ok(Self {
            chunk_bytes: chunk_bytes_u64,
            chunk_shift: chunk_bytes_u64.trailing_zeros(),
            chunk_mask: chunk_bytes_u64.wrapping_sub(1),
            oversize_threshold: chunk_bytes_u64 >> 2_u32,
            // Reference 0 is the null link, so the first byte handed out is byte 8.
            cursor: CachePadded::new(AtomicU64::new(8)),
            oversize_count: AtomicU64::new(0),
            oversize_bytes: AtomicU64::new(0),
            chunks: SlotDirectory::new(),
            oversize: SlotDirectory::new(),
        })
    }

    /// Reserves `len` bytes: the reference, the bytes, and the offset of the first one in them.
    ///
    /// The bytes are the caller's alone until it publishes the reference.
    pub(crate) fn allocate(&self, len: usize) -> Result<(u64, &ByteCells, usize), ArenaError> {
        let wanted = u64::try_from(len.max(1)).map_err(|_| ArenaError::TooLarge)?;
        let reserved = round_up_8(wanted).ok_or(ArenaError::TooLarge)?;
        if reserved > self.oversize_threshold {
            // Relaxed: a statistic.
            self.oversize_bytes.fetch_add(reserved, Ordering::Relaxed);
            return self.allocate_oversize(len);
        }
        loop {
            // Relaxed: hands out a disjoint range and publishes nothing.
            let start = self.cursor.fetch_add(reserved, Ordering::Relaxed);
            let last = reserved
                .checked_sub(1)
                .and_then(|tail| start.checked_add(tail))
                .ok_or(ArenaError::Exhausted)?;
            if last >= OVERSIZE_BIT {
                return Err(ArenaError::Exhausted);
            }
            let chunk_index = self.chunk_of(start);
            if chunk_index != self.chunk_of(last) {
                // The range crosses a chunk end: its bytes are wasted, reserve again.
                continue;
            }
            let chunk_bytes =
                usize::try_from(self.chunk_bytes).map_err(|_| ArenaError::TooLarge)?;
            let cells = self
                .chunks
                .get_or_install(chunk_index, || ByteCells::zeroed(chunk_bytes))
                .ok_or(ArenaError::Exhausted)?;
            let offset =
                usize::try_from(start & self.chunk_mask).map_err(|_| ArenaError::TooLarge)?;
            return Ok((start, cells, offset));
        }
    }

    /// An allocation too large to share a chunk: its own bytes, its own directory slot.
    fn allocate_oversize(&self, len: usize) -> Result<(u64, &ByteCells, usize), ArenaError> {
        // Relaxed: hands out a distinct index and publishes nothing.
        let index = self.oversize_count.fetch_add(1, Ordering::Relaxed);
        if index >= OVERSIZE_BIT {
            return Err(ArenaError::Exhausted);
        }
        let cells = self
            .oversize
            .get_or_install(index, || ByteCells::zeroed(len))
            .ok_or(ArenaError::Exhausted)?;
        Ok((OVERSIZE_BIT | index, cells, 0))
    }

    /// The bytes behind `reference` and the offset of its first byte.
    pub(crate) fn resolve(&self, reference: u64) -> Option<(&ByteCells, usize)> {
        if reference & OVERSIZE_BIT != 0 {
            return Some((self.oversize.get(reference & !OVERSIZE_BIT)?, 0));
        }
        let cells = self.chunks.get(self.chunk_of(reference))?;
        Some((cells, usize::try_from(reference & self.chunk_mask).ok()?))
    }

    /// The chunk holding byte `position`.
    fn chunk_of(&self, position: u64) -> u64 {
        // `chunk_shift` is below 64 because `chunk_bytes` is a nonzero `u64`.
        position.checked_shr(self.chunk_shift).unwrap_or(0)
    }

    /// Bytes reserved so far, including the bytes wasted at chunk ends; an estimate under
    /// concurrency (quiescently consistent).
    pub(crate) fn allocated_bytes(&self) -> u64 {
        // Relaxed: statistics.
        let small = self.cursor.load(Ordering::Relaxed);
        small.saturating_add(self.oversize_bytes.load(Ordering::Relaxed))
    }
}
