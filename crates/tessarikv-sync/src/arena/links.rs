//! The link arena: skiplist towers.

use crossbeam_utils::CachePadded;

use crate::error::ArenaError;
use crate::shim::{AtomicU64, Ordering};
use crate::slot_directory::SlotDirectory;

/// One chunk of link slots.
struct LinkChunk {
    slots: Box<[AtomicU64]>,
}

/// See the module documentation.
pub(crate) struct LinkArena {
    chunk_slots: u64,
    /// `log2(chunk_slots)`: a slot's chunk is `index >> chunk_shift`.
    chunk_shift: u32,
    /// `chunk_slots - 1`: a slot's position in its chunk is `index & chunk_mask`.
    chunk_mask: u64,
    cursor: CachePadded<AtomicU64>,
    chunks: SlotDirectory<LinkChunk>,
}

impl LinkArena {
    /// An empty arena with chunks of `chunk_slots` slots, a power of two of at least 16.
    pub(crate) fn new(chunk_slots: usize) -> Result<Self, ArenaError> {
        if chunk_slots < 16 || !chunk_slots.is_power_of_two() {
            return Err(ArenaError::TooLarge);
        }
        let chunk_slots_u64 = u64::try_from(chunk_slots).map_err(|_| ArenaError::TooLarge)?;
        Ok(Self {
            chunk_slots: chunk_slots_u64,
            chunk_shift: chunk_slots_u64.trailing_zeros(),
            chunk_mask: chunk_slots_u64.wrapping_sub(1),
            cursor: CachePadded::new(AtomicU64::new(0)),
            chunks: SlotDirectory::new(),
        })
    }

    /// Reserves `count` consecutive zeroed slots inside one chunk; returns the first slot's index.
    ///
    /// `count` is at most a quarter chunk.
    pub(crate) fn allocate(&self, count: u32) -> Result<u64, ArenaError> {
        let count = u64::from(count.max(1));
        if count > self.chunk_slots >> 2_u32 {
            return Err(ArenaError::TooLarge);
        }
        loop {
            // Relaxed: hands out disjoint slots and publishes nothing.
            let start = self.cursor.fetch_add(count, Ordering::Relaxed);
            let last = count
                .checked_sub(1)
                .and_then(|tail| start.checked_add(tail))
                .ok_or(ArenaError::Exhausted)?;
            let chunk_index = self.chunk_of(start);
            if chunk_index != self.chunk_of(last) {
                continue;
            }
            let chunk_slots =
                usize::try_from(self.chunk_slots).map_err(|_| ArenaError::TooLarge)?;
            self.chunks
                .get_or_install(chunk_index, || LinkChunk {
                    slots: (0..chunk_slots).map(|_| AtomicU64::new(0)).collect(),
                })
                .ok_or(ArenaError::Exhausted)?;
            return Ok(start);
        }
    }

    /// The slot at `index`, once its chunk exists.
    pub(crate) fn slot(&self, index: u64) -> Option<&AtomicU64> {
        let chunk = self.chunks.get(self.chunk_of(index))?;
        chunk
            .slots
            .get(usize::try_from(index & self.chunk_mask).ok()?)
    }

    /// The chunk holding slot `index`.
    fn chunk_of(&self, index: u64) -> u64 {
        // `chunk_shift` is below 64 because `chunk_slots` is a nonzero `u64`.
        index.checked_shr(self.chunk_shift).unwrap_or(0)
    }
}
