//! A test storage over memory that counts how files are read, and that can refuse every read
//! promised not to block, as a file system does for blocks not in its page cache.
//!
//! Ordering: the counters are statistics read after the reads are done; nothing is published
//! through them, so every access is Relaxed.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use super::{MemoryStorage, ReadableFile, Storage, StorageLock, WritableFile};
use crate::Result;

/// Reads made so far through files this storage opened.
#[derive(Debug, Default)]
pub(crate) struct Reads {
    /// Reads made without blocking.
    pub(crate) inline: AtomicU64,
    /// Reads made where blocking is allowed.
    pub(crate) blocking: AtomicU64,
    /// Files opened and not yet dropped.
    pub(crate) open: AtomicU64,
    /// Reads made on a thread of a runtime's pools rather than on the caller's own thread.
    pub(crate) on_pool_threads: AtomicU64,
}

/// Memory storage whose files count their reads; `inline` decides whether a read promised not
/// to block is made or refused.
#[derive(Debug, Clone)]
pub(crate) struct CountedStorage {
    inner: MemoryStorage,
    inline: bool,
    pub(crate) reads: Arc<Reads>,
}

impl CountedStorage {
    pub(crate) fn new(inline: bool) -> Self {
        Self {
            inner: MemoryStorage::new(),
            inline,
            reads: Arc::default(),
        }
    }
}

struct CountedFile {
    inner: Arc<dyn ReadableFile>,
    inline: bool,
    reads: Arc<Reads>,
}

impl ReadableFile for CountedFile {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<()> {
        // Relaxed: see the module documentation.
        self.reads.blocking.fetch_add(1, Ordering::Relaxed);
        let pooled = std::thread::current()
            .name()
            .is_some_and(|name| name.starts_with("tokio-"));
        if pooled {
            self.reads.on_pool_threads.fetch_add(1, Ordering::Relaxed);
        }
        self.inner.read_at(offset, buf)
    }

    fn try_read_at(&self, offset: u64, buf: &mut [u8]) -> Result<bool> {
        if !self.inline {
            return Ok(false);
        }
        let made = self.inner.try_read_at(offset, buf)?;
        if made {
            // Relaxed: see the module documentation.
            self.reads.inline.fetch_add(1, Ordering::Relaxed);
        }
        Ok(made)
    }

    fn len(&self) -> Result<u64> {
        self.inner.len()
    }
}

impl Drop for CountedFile {
    fn drop(&mut self) {
        // Relaxed: see the module documentation.
        self.reads.open.fetch_sub(1, Ordering::Relaxed);
    }
}

impl Storage for CountedStorage {
    fn create(&self, path: &Path) -> Result<Box<dyn WritableFile>> {
        self.inner.create(path)
    }
    fn open(&self, path: &Path) -> Result<Arc<dyn ReadableFile>> {
        let inner = self.inner.open(path)?;
        // Relaxed: see the module documentation.
        self.reads.open.fetch_add(1, Ordering::Relaxed);
        Ok(Arc::new(CountedFile {
            inner,
            inline: self.inline,
            reads: Arc::clone(&self.reads),
        }))
    }
    fn exists(&self, path: &Path) -> Result<bool> {
        self.inner.exists(path)
    }
    fn remove(&self, path: &Path) -> Result<()> {
        self.inner.remove(path)
    }
    fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        self.inner.rename(from, to)
    }
    fn list(&self, dir: &Path) -> Result<Vec<String>> {
        self.inner.list(dir)
    }
    fn create_dir_all(&self, dir: &Path) -> Result<()> {
        self.inner.create_dir_all(dir)
    }
    fn sync_dir(&self, dir: &Path) -> Result<()> {
        self.inner.sync_dir(dir)
    }
    fn lock(&self, path: &Path) -> Result<Box<dyn StorageLock>> {
        self.inner.lock(path)
    }
}
