//! A growable array of lazily created values that any thread may install, lock-free.
//!
//! Sequential specification: `get(i)` returns the value at `i`, or none if none was installed;
//! `get_or_install(i, make)` returns the value at `i`, installing `make()` first if there was none.
//! Every caller of `get_or_install(i, ..)` sees the same value. Linearizable.
//!
//! | Operation | Linearization point | Progress |
//! |---|---|---|
//! | `get` | the Acquire load of the item slot | wait-free (two loads) |
//! | `get_or_install`, value present | the Acquire load of the item slot | wait-free |
//! | `get_or_install`, installs | its successful CAS on the item slot | lock-free: a failed CAS means another installer won |
//!
//! Index `i` lives in block `ilog2(i + 1)` at slot `i + 1 - 2^block`; block `b` holds `2^b` slots.
//! Indices are meant to be DENSE (0, 1, 2, ...): touching index `i` allocates its whole block, up to
//! `i + 1` slot pointers, so the slot overhead stays within twice the number of items.
//! Blocks and items are installed by CAS from null with Release and read with Acquire; a CAS loser
//! drops its own allocation, which no other thread ever saw, and uses the winner's.
//!
//! Reclamation: nothing is freed or replaced while the directory is shared; `Drop` (exclusive
//! access) frees every block and item once. Pointers are written once, so there is no ABA.

#[cfg(all(test, not(loom)))]
mod tests;

#[cfg(all(test, loom))]
mod loom_tests;

use std::ptr::null_mut;

use crate::shim::{AtomicPtr, Ordering};

/// One block per bit of a 64-bit index.
const BLOCKS: usize = 64;

/// A run of item slots.
struct Block<T> {
    slots: Box<[AtomicPtr<T>]>,
}

/// See the module documentation.
pub(crate) struct SlotDirectory<T> {
    blocks: [AtomicPtr<Block<T>>; BLOCKS],
}

// SAFETY: values are shared by reference across threads (`get` hands out `&T`), so `T: Sync` is
// required; they are created on one thread and dropped on another, so `T: Send` is required.
unsafe impl<T: Send + Sync> Sync for SlotDirectory<T> {}
// SAFETY: as above; owning the directory owns every value.
unsafe impl<T: Send + Sync> Send for SlotDirectory<T> {}

/// The block and slot of `index`.
fn locate(index: u64) -> Option<(usize, usize)> {
    let position = index.checked_add(1)?;
    let block = position.ilog2();
    let first = 1_u64.checked_shl(block)?;
    let slot = position.checked_sub(first)?;
    Some((usize::try_from(block).ok()?, usize::try_from(slot).ok()?))
}

/// Installs `fresh` into `slot` unless another thread already did, returning the installed value.
fn install<T>(slot: &AtomicPtr<T>, fresh: Box<T>) -> *mut T {
    let fresh = Box::into_raw(fresh);
    // Success Release: publishes the allocation.
    // Failure Acquire: the winner's pointer is dereferenced next.
    match slot.compare_exchange(null_mut(), fresh, Ordering::Release, Ordering::Acquire) {
        Ok(_) => fresh,
        Err(winner) => {
            // SAFETY: `fresh` came from `Box::into_raw` above and the failed CAS never published
            // it, so this thread still owns it exclusively.
            drop(unsafe { Box::from_raw(fresh) });
            winner
        }
    }
}

impl<T> SlotDirectory<T> {
    pub(crate) fn new() -> Self {
        Self {
            blocks: std::array::from_fn(|_| AtomicPtr::new(null_mut())),
        }
    }

    /// The value at `index`, if one was installed.
    pub(crate) fn get(&self, index: u64) -> Option<&T> {
        let (block_index, slot_index) = locate(index)?;
        // Acquire: pairs with the installer's Release.
        let block = self.blocks.get(block_index)?.load(Ordering::Acquire);
        if block.is_null() {
            return None;
        }
        // SAFETY: a non-null block was installed once, is never replaced, and is freed only in
        // `Drop`, which needs exclusive access; the Acquire load ordered its initialisation first.
        let block = unsafe { &*block };
        // Acquire: pairs with the installer's Release.
        let item = block.slots.get(slot_index)?.load(Ordering::Acquire);
        if item.is_null() {
            return None;
        }
        // SAFETY: as for the block.
        Some(unsafe { &*item })
    }

    /// The value at `index`, installing `make()` first when there is none; `None` only when
    /// `index` is beyond the directory (`u64::MAX`).
    pub(crate) fn get_or_install(&self, index: u64, make: impl FnOnce() -> T) -> Option<&T> {
        if let Some(value) = self.get(index) {
            return Some(value);
        }
        let (block_index, slot_index) = locate(index)?;
        let block_slot = self.blocks.get(block_index)?;
        // Acquire: as in `get`.
        let mut block = block_slot.load(Ordering::Acquire);
        if block.is_null() {
            let slots = 1_usize.checked_shl(u32::try_from(block_index).ok()?)?;
            let fresh = Box::new(Block {
                slots: (0..slots).map(|_| AtomicPtr::new(null_mut())).collect(),
            });
            block = install(block_slot, fresh);
        }
        // SAFETY: installed once, never replaced, freed only in `Drop` (see `get`).
        let block = unsafe { &*block };
        let item = install(block.slots.get(slot_index)?, Box::new(make()));
        // SAFETY: as for the block.
        Some(unsafe { &*item })
    }
}

impl<T> Drop for SlotDirectory<T> {
    fn drop(&mut self) {
        for block_slot in &self.blocks {
            let block = block_slot.load(Ordering::Relaxed);
            if block.is_null() {
                continue;
            }
            // SAFETY: `drop` has exclusive access; the pointer came from `Box::into_raw` and is
            // reclaimed exactly once here.
            let block = unsafe { Box::from_raw(block) };
            for slot in block.slots.iter() {
                let item = slot.load(Ordering::Relaxed);
                if !item.is_null() {
                    // SAFETY: as for the block.
                    drop(unsafe { Box::from_raw(item) });
                }
            }
        }
    }
}
