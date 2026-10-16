//! Where every chunk of a buffer lives, and the memory that outlives every handle to it.
//!
//! Chunk `c` lives in block `floor(log2(c + 1))` at slot `c + 1 - 2^block`; block `b` holds
//! `2^b` slots. Blocks and chunks are allocated once, by the single writer, published with a
//! Release store, and freed only when the shared state is dropped, so an address, once read,
//! stays valid and never names anything else (no reuse, hence no ABA).

use std::ptr::null_mut;

use crossbeam_utils::CachePadded;

use crate::shim::{AtomicPtr, AtomicU64, ByteCells, Ordering};

/// One block per bit of a 64-bit chunk index.
const BLOCKS: usize = 64;

/// A run of chunk slots.
struct Block {
    slots: Box<[AtomicPtr<Chunk>]>,
}

/// A fixed-size run of the buffer's bytes.
pub(super) struct Chunk {
    pub(super) bytes: ByteCells,
}

/// The state every handle of one buffer shares.
pub(super) struct Shared {
    pub(super) chunk_bytes: usize,
    blocks: [AtomicPtr<Block>; BLOCKS],
    /// How many bytes are readable. Written only by the writer; on its own cache line so the
    /// readers polling it do not share a line with the directory.
    pub(super) published: CachePadded<AtomicU64>,
}

/// The block and slot of chunk `index`.
fn locate(index: u64) -> Option<(usize, usize)> {
    let position = index.checked_add(1)?;
    let block = position.ilog2();
    let first = 1_u64.checked_shl(block)?;
    let slot = position.checked_sub(first)?;
    Some((usize::try_from(block).ok()?, usize::try_from(slot).ok()?))
}

impl Shared {
    pub(super) fn new(chunk_bytes: usize) -> Self {
        Self {
            chunk_bytes,
            blocks: std::array::from_fn(|_| AtomicPtr::new(null_mut())),
            published: CachePadded::new(AtomicU64::new(0)),
        }
    }

    /// The chunk at `index`, installing its block and itself when absent. WRITER ONLY.
    pub(super) fn chunk_for_write(&self, index: u64) -> Option<&Chunk> {
        let (block_index, slot_index) = locate(index)?;
        let block_slot = self.blocks.get(block_index)?;
        // Relaxed: the single writer reads back its own store.
        let mut block = block_slot.load(Ordering::Relaxed);
        if block.is_null() {
            let slots = 1_usize.checked_shl(u32::try_from(block_index).ok()?)?;
            let fresh = Box::into_raw(Box::new(Block {
                slots: (0..slots).map(|_| AtomicPtr::new(null_mut())).collect(),
            }));
            // Release: publishes the slot array to readers' Acquire load.
            block_slot.store(fresh, Ordering::Release);
            block = fresh;
        }
        // SAFETY: a non-null block pointer was installed by this writer, is never replaced, and
        // is freed only in `Drop`, which needs every handle gone.
        let block = unsafe { &*block };
        let slot = block.slots.get(slot_index)?;
        let mut chunk = slot.load(Ordering::Relaxed);
        if chunk.is_null() {
            let fresh = Box::into_raw(Box::new(Chunk {
                bytes: ByteCells::zeroed(self.chunk_bytes),
            }));
            // Release: publishes the chunk allocation to readers' Acquire load.
            slot.store(fresh, Ordering::Release);
            chunk = fresh;
        }
        // SAFETY: as for the block: installed once, never replaced, freed only in `Drop`.
        Some(unsafe { &*chunk })
    }

    /// The chunk at `index` if it has been installed. Any thread.
    pub(super) fn chunk(&self, index: u64) -> Option<&Chunk> {
        let (block_index, slot_index) = locate(index)?;
        // Acquire: pairs with the writer's Release install.
        let block = self.blocks.get(block_index)?.load(Ordering::Acquire);
        if block.is_null() {
            return None;
        }
        // SAFETY: installed once, never replaced, freed only in `Drop` (see `chunk_for_write`).
        let block = unsafe { &*block };
        // Acquire: pairs with the writer's Release install.
        let chunk = block.slots.get(slot_index)?.load(Ordering::Acquire);
        if chunk.is_null() {
            return None;
        }
        // SAFETY: as above.
        Some(unsafe { &*chunk })
    }
}

impl Drop for Shared {
    fn drop(&mut self) {
        for block_slot in &self.blocks {
            let block = block_slot.load(Ordering::Relaxed);
            if block.is_null() {
                continue;
            }
            // SAFETY: `drop` runs once, after every handle is gone; the pointer came from
            // `Box::into_raw` and is reclaimed exactly once here.
            let block = unsafe { Box::from_raw(block) };
            for slot in block.slots.iter() {
                let chunk = slot.load(Ordering::Relaxed);
                if !chunk.is_null() {
                    // SAFETY: as for the block.
                    drop(unsafe { Box::from_raw(chunk) });
                }
            }
        }
    }
}
