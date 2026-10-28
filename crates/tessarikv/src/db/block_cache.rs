//! The block cache: decoded data blocks of table files, kept for the next read that needs them.
//!
//! One cache serves every table file of a database, bounded by
//! [`crate::DbOptions::block_cache_bytes`]. It is keyed by `(table file id, block offset)`: file
//! ids are never reused and a table file never changes, so an entry never goes stale and needs
//! no invalidation; the blocks of a removed file are simply never asked for again and leave as
//! newer blocks arrive. A block is shared, not copied, so a reader keeps the block it got even
//! after the cache lets it go.
//!
//! Lookups and inserts never wait for one another: the cache is a CLOCK cache
//! ([`tessarikv_sync::ClockCache`]) that evicts blocks not read since its hand last passed them.
//! A block larger than the cache is not kept, and an insert that finds every candidate being
//! read leaves the block uncached; the read that loaded it still gets it.

use std::path::Path;
use std::sync::Arc;

use tessarikv_format::Block;
use tessarikv_sync::ClockCache;

use crate::Result;
use crate::sealing::Sealing;
use crate::statistics::{Statistics, Ticker};
use crate::storage::{ReadableFile, Storage};

#[cfg(test)]
mod tests;

/// Where a block is: its table file and its offset in it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct BlockKey {
    pub(crate) file: u64,
    pub(crate) offset: u64,
}

/// Decoded blocks of table files, bounded in bytes, and, when open files are bounded, the
/// handles of table files.
#[derive(Debug)]
pub(crate) struct BlockCache {
    blocks: ClockCache<BlockKey, Block>,
    files: Option<FileHandles>,
    /// How table files are encrypted, when they are.
    sealing: Option<Arc<Sealing>>,
    /// Where block reads are counted, when the database keeps statistics.
    statistics: Option<Arc<Statistics>>,
    /// Whether a block read that may block is made on the reading task's own thread.
    reads_on_worker: bool,
}

/// Open table files by file id, at most [`crate::DbOptions::max_open_files`] of them: a read
/// takes a file's handle from here, opening the file again when it was let go. The same CLOCK
/// cache as the blocks', so a lookup never waits; a handle read since the clock last passed is
/// spared once. Reads in flight hold their own handle, so the files open at once are at most the
/// bound plus the reads in flight.
#[derive(Debug)]
struct FileHandles {
    storage: Arc<dyn Storage>,
    handles: ClockCache<u64, Arc<dyn ReadableFile>>,
}

impl BlockCache {
    /// A cache holding at most `capacity` bytes of blocks of about `block_bytes` each; zero
    /// keeps nothing.
    pub(crate) fn new(capacity: u64, block_bytes: u64) -> Self {
        Self {
            blocks: ClockCache::new(capacity, block_bytes),
            files: None,
            sealing: None,
            statistics: None,
            reads_on_worker: false,
        }
    }

    /// Reads blocks that may block on the reading task's thread when `on_worker` is set (see
    /// [`crate::DbOptions::block_reads_on_worker`]).
    pub(crate) fn with_reads_on_worker(mut self, on_worker: bool) -> Self {
        self.reads_on_worker = on_worker;
        self
    }

    /// Whether a block read that may block is made on the reading task's own thread.
    pub(crate) fn reads_on_worker(&self) -> bool {
        self.reads_on_worker
    }

    /// Keeps at most `max_open_files` table files open, opened through `storage`; zero keeps
    /// each table file open for as long as the table is read.
    pub(crate) fn with_open_files(
        mut self,
        max_open_files: u32,
        storage: Arc<dyn Storage>,
    ) -> Self {
        self.files = (max_open_files != 0).then(|| FileHandles {
            storage,
            handles: ClockCache::new(u64::from(max_open_files), 1),
        });
        self
    }

    /// Encrypts the table files written and opens the ones read with `sealing`.
    pub(crate) fn with_sealing(mut self, sealing: Option<Arc<Sealing>>) -> Self {
        self.sealing = sealing;
        self
    }

    /// Counts block reads in `statistics`.
    pub(crate) fn with_statistics(mut self, statistics: Option<Arc<Statistics>>) -> Self {
        self.statistics = statistics;
        self
    }

    /// Where reads of table files are counted; `None` when the database keeps no statistics.
    pub(crate) fn statistics(&self) -> Option<&Statistics> {
        self.statistics.as_deref()
    }

    /// Counts a data block not found here, of `bytes` bytes on storage, read from its file.
    pub(crate) fn count_read(&self, bytes: u64) {
        if let Some(statistics) = self.statistics() {
            statistics.record(Ticker::BlockCacheMiss, 1);
            statistics.record(Ticker::BlockBytesRead, bytes);
        }
    }

    /// How table files are encrypted; `None` when they are not.
    pub(crate) fn sealing(&self) -> Option<&Sealing> {
        self.sealing.as_deref()
    }

    /// The sealing, shared with another cache that reads the same files.
    pub(crate) fn shared_sealing(&self) -> Option<Arc<Sealing>> {
        self.sealing.clone()
    }

    /// Whether table files are opened through this cache rather than each kept open.
    pub(crate) fn bounds_open_files(&self) -> bool {
        self.files.is_some()
    }

    /// Keeps `file`, the handle of table file `id`, for the next read of it.
    pub(crate) fn offer_file(&self, id: u64, file: Arc<dyn ReadableFile>) {
        if let Some(files) = &self.files {
            // Whether it was kept changes nothing: a later read opens the file again.
            let _ = files.handles.insert(id, file, 1);
        }
    }

    /// The handle of table file `id`, when one is kept.
    pub(crate) fn cached_file(&self, id: u64) -> Option<Arc<dyn ReadableFile>> {
        self.files.as_ref()?.handles.get(&id)
    }

    /// The handle of table file `id` at `path`: the one kept, or the file opened again and
    /// kept. Opens, so it runs where blocking is allowed.
    ///
    /// # Errors
    ///
    /// The storage's error when the file cannot be opened.
    pub(crate) fn file(&self, id: u64, path: &Path) -> Result<Arc<dyn ReadableFile>> {
        if let Some(file) = self.cached_file(id) {
            return Ok(file);
        }
        let Some(files) = &self.files else {
            return Err(crate::Error::Unsupported {
                what: "a table file without a handle",
            });
        };
        let file = files.storage.open(path)?;
        let _ = files.handles.insert(id, Arc::clone(&file), 1);
        Ok(file)
    }

    /// Lets go of the handle of table file `id`, which is about to be removed; returns whether
    /// none is kept any more (a handle being read stays until the next call).
    pub(crate) fn forget_file(&self, id: u64) -> bool {
        let Some(files) = &self.files else {
            return true;
        };
        files.handles.remove(&id);
        files.handles.get(&id).is_none()
    }

    /// The block at `key`, when the cache holds it.
    pub(crate) fn get(&self, key: BlockKey) -> Option<Block> {
        let block = self.blocks.get(&key);
        if let (Some(statistics), Some(_)) = (self.statistics(), &block) {
            statistics.record(Ticker::BlockCacheHit, 1);
        }
        block
    }

    /// Keeps `block` as the block at `key`, making room by evicting blocks not read recently. A
    /// block larger than the cache, or one the cache already holds, is not added.
    pub(crate) fn insert(&self, key: BlockKey, block: Block) {
        let charge = u64::try_from(block.size_bytes()).unwrap_or(u64::MAX);
        // Whether the block was kept changes nothing for the reader, which already has it.
        let _ = self.blocks.insert(key, block, charge);
    }

    /// Bytes of blocks held, counting inserts in flight: an estimate while others insert.
    #[cfg(test)]
    pub(crate) fn used_bytes(&self) -> u64 {
        self.blocks.used()
    }
}
