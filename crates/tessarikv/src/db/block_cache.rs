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

use tessarikv_format::Block;
use tessarikv_sync::ClockCache;

#[cfg(test)]
mod tests;

/// Where a block is: its table file and its offset in it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct BlockKey {
    pub(crate) file: u64,
    pub(crate) offset: u64,
}

/// Decoded blocks of table files, bounded in bytes.
#[derive(Debug)]
pub(crate) struct BlockCache {
    blocks: ClockCache<BlockKey, Block>,
}

impl BlockCache {
    /// A cache holding at most `capacity` bytes of blocks of about `block_bytes` each; zero
    /// keeps nothing.
    pub(crate) fn new(capacity: u64, block_bytes: u64) -> Self {
        Self {
            blocks: ClockCache::new(capacity, block_bytes),
        }
    }

    /// The block at `key`, when the cache holds it.
    pub(crate) fn get(&self, key: BlockKey) -> Option<Block> {
        self.blocks.get(&key)
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
