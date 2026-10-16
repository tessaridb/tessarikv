//! Compacting a key range on request.

use super::Db;
use super::compaction::manual;
use crate::Result;

impl Db {
    /// Compacts the user keys `[start, end)` (unbounded where `None`) and returns once done:
    /// everything committed before the call is flushed, every table holding keys in the range is
    /// merged down to the bottom level, and the bottom-level tables the range touches are
    /// rewritten, so overwritten versions and deletes leave the disk unless a snapshot still
    /// reads them. Writes, reads and automatic compactions go on meanwhile.
    ///
    /// It rewrites every byte the range holds, once per level it passes, so bound the range: the
    /// whole keyspace needs free space on the order of its size. A universal keyspace merges all
    /// its runs into the bottom level when any of them touches the range; a first-in-first-out
    /// keyspace never merges, so it only flushes. In memory there are no tables and it returns at
    /// once.
    ///
    /// # Errors
    ///
    /// [`crate::Error::FlushFailed`] or [`crate::Error::CompactionFailed`] when the background
    /// work stopped.
    pub async fn compact_range(&self, start: Option<&[u8]>, end: Option<&[u8]>) -> Result<()> {
        let Some(installer) = &self.installer else {
            return Ok(());
        };
        self.flush().await?;
        manual::compact(&installer.requests, &installer.signals, start, end).await
    }
}
