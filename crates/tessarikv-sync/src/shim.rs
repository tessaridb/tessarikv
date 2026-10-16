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

/// A fixed run of bytes, 8-aligned, that one thread may write while others read OTHER bytes of it.
///
/// It is the caller's protocol that keeps the written and the read ranges apart; this type only
/// makes such disjoint access expressible without a data race on the whole allocation. Any
/// 8-aligned word of it can instead be used as an [`AtomicU64`] through
/// [`ByteCells::atomic_word`], provided that word is then never read or written as bytes.
pub(crate) struct ByteCells {
    /// The storage, as words so that every 8-aligned offset is aligned for an atomic.
    #[cfg(not(loom))]
    words: Box<[std::cell::UnsafeCell<u64>]>,
    /// Under `loom` bytes and atomic words are separate objects, so loom tracks each access.
    #[cfg(loom)]
    cells: Box<[loom::cell::UnsafeCell<u8>]>,
    #[cfg(loom)]
    words: Box<[AtomicU64]>,
    /// Length in bytes.
    len: usize,
}

// SAFETY: the cells are only reached through `write`, `read`, `slice`, `with_slice` and
// `atomic_word`, whose contracts forbid two threads touching one byte without happens-before
// between them, and forbid byte access to a word used as an atomic.
unsafe impl Sync for ByteCells {}

impl ByteCells {
    /// A zeroed run of `len` bytes.
    pub(crate) fn zeroed(len: usize) -> Self {
        let word_count = len.div_ceil(8);
        #[cfg(not(loom))]
        let words = (0..word_count)
            .map(|_| std::cell::UnsafeCell::new(0))
            .collect();
        #[cfg(loom)]
        let words = (0..word_count).map(|_| AtomicU64::new(0)).collect();
        Self {
            words,
            #[cfg(loom)]
            cells: (0..len).map(|_| loom::cell::UnsafeCell::new(0)).collect(),
            len,
        }
    }

    /// The first byte of `offset..offset + len`; `None` if the range does not fit.
    #[cfg(not(loom))]
    fn start(&self, offset: usize, len: usize) -> Option<*mut u8> {
        if offset.checked_add(len)? > self.len {
            return None;
        }
        let base = std::cell::UnsafeCell::raw_get(self.words.as_ptr()).cast::<u8>();
        // SAFETY: `offset + len <= self.len`, which is at most the words' size in bytes, so the
        // result stays inside the allocation `base` points into.
        Some(unsafe { base.add(offset) })
    }

    /// Copies `src` into the bytes starting at `offset`; `None` if the range does not fit.
    ///
    /// # Safety
    ///
    /// No other thread may access `offset..offset + src.len()` until a happens-before edge
    /// orders this write before that access, and no word of the range is used as an atomic.
    pub(crate) unsafe fn write(&self, offset: usize, src: &[u8]) -> Option<()> {
        #[cfg(not(loom))]
        {
            let dst = self.start(offset, src.len())?;
            // SAFETY: the range lies inside the allocation, the caller guarantees exclusive
            // access to it, and `src` cannot overlap this private range.
            unsafe { std::ptr::copy_nonoverlapping(src.as_ptr(), dst, src.len()) };
        }
        #[cfg(loom)]
        {
            let cells = self.cells.get(offset..offset.checked_add(src.len())?)?;
            for (cell, byte) in cells.iter().zip(src) {
                // SAFETY: exclusive access to this byte per the caller's contract; loom checks it.
                cell.with_mut(|ptr| unsafe { *ptr = *byte });
            }
        }
        Some(())
    }

    /// Runs `visit` over the bytes `offset..offset + len`; `None` if the range does not fit.
    ///
    /// Under `loom` the bytes are copied first, so every byte read is checked; otherwise `visit`
    /// borrows them in place.
    ///
    /// # Safety
    ///
    /// As for [`ByteCells::read`]: every write to the range happens-before this call, and none
    /// runs concurrently with it or with `visit`.
    pub(crate) unsafe fn with_slice<R>(
        &self,
        offset: usize,
        len: usize,
        visit: impl FnOnce(&[u8]) -> R,
    ) -> Option<R> {
        #[cfg(not(loom))]
        {
            let start = self.start(offset, len)?.cast_const();
            // SAFETY: the range lies inside the allocation and, per the caller's contract, is not
            // written while the borrow lives.
            let bytes = unsafe { std::slice::from_raw_parts(start, len) };
            Some(visit(bytes))
        }
        #[cfg(loom)]
        {
            let mut copy = vec![0; len];
            // SAFETY: the caller's contract is this function's contract.
            unsafe { self.read(offset, &mut copy) }?;
            Some(visit(&copy))
        }
    }

    /// The bytes `offset..offset + len`, borrowed in place; `None` if the range does not fit.
    ///
    /// # Safety
    ///
    /// Every write to the range happens-before this call and no write to it ever follows.
    #[cfg(not(loom))]
    pub(crate) unsafe fn slice(&self, offset: usize, len: usize) -> Option<&[u8]> {
        let start = self.start(offset, len)?.cast_const();
        // SAFETY: inside the allocation and never written again per the caller's contract.
        Some(unsafe { std::slice::from_raw_parts(start, len) })
    }

    /// Copies the bytes starting at `offset` into `dst`; `None` if the range does not fit.
    ///
    /// # Safety
    ///
    /// Every write to `offset..offset + dst.len()` must happen-before this read, and no write
    /// to that range may run concurrently with it.
    pub(crate) unsafe fn read(&self, offset: usize, dst: &mut [u8]) -> Option<()> {
        #[cfg(not(loom))]
        {
            let src = self.start(offset, dst.len())?.cast_const();
            // SAFETY: the range lies inside the allocation and, per the caller's contract, is not
            // written concurrently; `dst` is a distinct exclusive borrow.
            unsafe { std::ptr::copy_nonoverlapping(src, dst.as_mut_ptr(), dst.len()) };
        }
        #[cfg(loom)]
        {
            let cells = self.cells.get(offset..offset.checked_add(dst.len())?)?;
            for (cell, byte) in cells.iter().zip(dst.iter_mut()) {
                // SAFETY: the write of this byte happens-before this read per the caller's
                // contract.
                cell.with(|ptr| *byte = unsafe { *ptr });
            }
        }
        Some(())
    }

    /// The 8 bytes at `offset` as an atomic word; `None` unless `offset` is 8-aligned and the
    /// word lies inside the run.
    ///
    /// # Safety
    ///
    /// For as long as the run lives, the word is accessed only through this method: never
    /// through `write`, `read`, `slice` or `with_slice`.
    pub(crate) unsafe fn atomic_word(&self, offset: usize) -> Option<&AtomicU64> {
        if offset & 7 != 0 || offset.checked_add(8)? > self.len {
            return None;
        }
        let word = self.words.get(offset >> 3_u32)?;
        #[cfg(not(loom))]
        {
            // SAFETY: the word is aligned for `u64` and lives as long as `&self`; per the
            // caller's contract every access to it from here on is atomic, and the zero it was
            // constructed with happens-before any thread holding `&self` reaches it.
            Some(unsafe { AtomicU64::from_ptr(word.get()) })
        }
        #[cfg(loom)]
        Some(word)
    }
}

/// A value that the caller's protocol hands from thread to thread, checked for data races under
/// `loom`.
pub(crate) struct Cell<T> {
    #[cfg(not(loom))]
    inner: std::cell::UnsafeCell<T>,
    #[cfg(loom)]
    inner: loom::cell::UnsafeCell<T>,
}

impl<T> Cell<T> {
    /// A cell holding `value`.
    pub(crate) fn new(value: T) -> Self {
        Self {
            #[cfg(not(loom))]
            inner: std::cell::UnsafeCell::new(value),
            #[cfg(loom)]
            inner: loom::cell::UnsafeCell::new(value),
        }
    }

    /// Runs `visit` over the value.
    ///
    /// # Safety
    ///
    /// Every write to the value happens-before this call, and none runs concurrently with it.
    pub(crate) unsafe fn with<R>(&self, visit: impl FnOnce(&T) -> R) -> R {
        #[cfg(not(loom))]
        {
            // SAFETY: no write runs concurrently, per the caller's contract.
            visit(unsafe { &*self.inner.get() })
        }
        #[cfg(loom)]
        // SAFETY: as above; loom checks the claim.
        self.inner.with(|value| visit(unsafe { &*value }))
    }

    /// Runs `change` over the value, exclusively.
    ///
    /// # Safety
    ///
    /// Every other access to the value happens-before this call or after it; none runs
    /// concurrently with it.
    pub(crate) unsafe fn with_mut<R>(&self, change: impl FnOnce(&mut T) -> R) -> R {
        #[cfg(not(loom))]
        {
            // SAFETY: this access is exclusive, per the caller's contract.
            change(unsafe { &mut *self.inner.get() })
        }
        #[cfg(loom)]
        // SAFETY: as above; loom checks the claim.
        self.inner.with_mut(|value| change(unsafe { &mut *value }))
    }
}
