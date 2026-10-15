//! The concurrency primitives, from `std` in a normal build and from `loom` under `--cfg loom`,
//! so the same code is both shipped and model-checked.

#[cfg(loom)]
pub(crate) use loom::sync::Arc;
#[cfg(loom)]
pub(crate) use loom::sync::atomic::{AtomicPtr, AtomicU64, Ordering};
#[cfg(not(loom))]
pub(crate) use std::sync::Arc;
#[cfg(not(loom))]
pub(crate) use std::sync::atomic::{AtomicPtr, AtomicU64, Ordering};

/// A fixed run of bytes that one thread may write while others read OTHER bytes of it.
///
/// It is the caller's protocol that keeps the written and the read ranges apart; this type only
/// makes such disjoint access expressible without a data race on the whole allocation.
pub(crate) struct ByteCells {
    #[cfg(not(loom))]
    cells: Box<[std::cell::UnsafeCell<u8>]>,
    #[cfg(loom)]
    cells: Box<[loom::cell::UnsafeCell<u8>]>,
}

// SAFETY: the cells are only reached through `write` and `read`, whose contracts forbid two
// threads touching one byte without happens-before between them.
unsafe impl Sync for ByteCells {}

impl ByteCells {
    /// A zeroed run of `len` bytes.
    pub(crate) fn zeroed(len: usize) -> Self {
        #[cfg(not(loom))]
        let cells = (0..len).map(|_| std::cell::UnsafeCell::new(0)).collect();
        #[cfg(loom)]
        let cells = (0..len).map(|_| loom::cell::UnsafeCell::new(0)).collect();
        Self { cells }
    }

    /// Copies `src` into the bytes starting at `offset`; `None` if the range does not fit.
    ///
    /// # Safety
    ///
    /// No other thread may access `offset..offset + src.len()` until a happens-before edge
    /// orders this write before that access.
    pub(crate) unsafe fn write(&self, offset: usize, src: &[u8]) -> Option<()> {
        let end = offset.checked_add(src.len())?;
        let cells = self.cells.get(offset..end)?;
        #[cfg(not(loom))]
        {
            let dst = std::cell::UnsafeCell::raw_get(cells.as_ptr());
            // SAFETY: `cells` spans exactly `src.len()` bytes inside one allocation, the caller
            // guarantees exclusive access to them, and `src` cannot overlap this private range.
            unsafe { std::ptr::copy_nonoverlapping(src.as_ptr(), dst, src.len()) };
        }
        #[cfg(loom)]
        for (cell, byte) in cells.iter().zip(src) {
            // SAFETY: exclusive access to this byte per the caller's contract; loom checks it.
            cell.with_mut(|ptr| unsafe { *ptr = *byte });
        }
        Some(())
    }

    /// Copies the bytes starting at `offset` into `dst`; `None` if the range does not fit.
    ///
    /// # Safety
    ///
    /// Every write to `offset..offset + dst.len()` must happen-before this read, and no write
    /// to that range may run concurrently with it.
    pub(crate) unsafe fn read(&self, offset: usize, dst: &mut [u8]) -> Option<()> {
        let end = offset.checked_add(dst.len())?;
        let cells = self.cells.get(offset..end)?;
        #[cfg(not(loom))]
        {
            let src = std::cell::UnsafeCell::raw_get(cells.as_ptr());
            // SAFETY: the range lies inside one allocation and, per the caller's contract, is not
            // written concurrently; `dst` is a distinct exclusive borrow.
            unsafe { std::ptr::copy_nonoverlapping(src.cast_const(), dst.as_mut_ptr(), dst.len()) };
        }
        #[cfg(loom)]
        for (cell, byte) in cells.iter().zip(dst.iter_mut()) {
            // SAFETY: the write of this byte happens-before this read per the caller's contract.
            cell.with(|ptr| *byte = unsafe { *ptr });
        }
        Some(())
    }
}
