//! Compacting a key range on request.

use tessarikv_types::KeyspaceId;

use super::Db;
use super::catalog::Command;
use super::compaction::manual::Request;
use crate::{Error, Result};

impl Db {
    /// Compacts the user keys `[start, end)` of `keyspace` (unbounded above when `end` is
    /// `None`) and returns once done: everything committed before the call is flushed, every
    /// table holding keys in the range is merged down to the bottom level, and the bottom-level
    /// tables the range touches are rewritten, so overwritten versions and deletes leave the disk
    /// unless a snapshot still reads them. Writes, reads and automatic compactions go on
    /// meanwhile.
    ///
    /// It rewrites every byte the range holds, once per level it passes, so bound the range: the
    /// whole keyspace needs free space on the order of its size. A universal keyspace merges all
    /// its runs into the bottom level when any of them touches the range; a first-in-first-out
    /// keyspace never merges, so it only flushes. In memory there are no tables and it returns at
    /// once. After a compaction failed, the next one starts only once the wait that failure
    /// imposed is over (at most 30 s), so the call may take that long first.
    ///
    /// # Errors
    ///
    /// [`crate::Error::UnknownKeyspace`] for a keyspace this database does not hold; the
    /// storage's error (such as [`crate::Error::Io`]) when one of its compactions failed, which
    /// leaves the database as it was and writable; and [`crate::Error::FlushFailed`] or
    /// [`crate::Error::CompactionFailed`] when the background work stopped.
    pub async fn compact_range(
        &self,
        keyspace: KeyspaceId,
        start: &[u8],
        end: Option<&[u8]>,
    ) -> Result<()> {
        self.check_keyspace(keyspace)?;
        let Some(installer) = &self.installer else {
            return Ok(());
        };
        self.flush().await?;
        // The installer serves requests over a bounded channel and answers on the request's own
        // reply channel; a closed channel means it stopped, for the reason it published.
        let (request, outcome) = Request::new(keyspace.get(), start, end);
        let stopped = || {
            installer
                .signals
                .failure()
                .unwrap_or(Error::CompactionFailed)
        };
        installer
            .requests
            .send(Command::Compact(request))
            .await
            .map_err(|_| stopped())?;
        outcome.await.map_err(|_| stopped())?
    }

    /// Waits until the database has no flush or compaction running, queued or due: every
    /// compaction its levels call for has run, and the last of them left nothing more to do.
    /// Writes and reads go on meanwhile, and a write that fills a table may start more work,
    /// which the wait then covers too. A database in memory returns at once.
    ///
    /// A compaction that keeps failing keeps the wait going, as it runs again after its back-off;
    /// bound the wait with a timeout where that matters.
    ///
    /// # Errors
    ///
    /// The error that stopped the background work, when the database stopped while waiting, and
    /// [`Error::Closed`] when it was closed.
    pub async fn wait_for_compact(&self) -> Result<()> {
        let Some(installer) = &self.installer else {
            return Ok(());
        };
        let (reply, idle) = tokio::sync::oneshot::channel();
        let stopped = || installer.signals.failure().unwrap_or(Error::Closed);
        installer
            .requests
            .send(Command::Idle(reply))
            .await
            .map_err(|_| stopped())?;
        idle.await.map_err(|_| stopped())
    }
}
