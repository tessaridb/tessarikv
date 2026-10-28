//! The database: atomic batches in, consistent point reads and ordered scans out.
//!
//! A write claims one sequence number per entry, hands its batch to the write-ahead log, waits
//! until the log holds it as durably as the write asks, applies it to the in-memory table and
//! completes the claim; a read takes the highest sequence covering only completed batches as its
//! snapshot. Writes from many tasks run concurrently, the log syncs once for every write waiting
//! on it, and reads never wait for writes. Opening a database on storage replays its log.
//!
//! Writes land in the in-memory table of the epoch their claim carries, and reads consult every
//! table of the current read view, newest first (see `view`).
//!
//! This version serves the default keyspace. On storage, a full in-memory table is flushed to a
//! table file in the background (see `installer`); in memory, every batch stays in memory.

mod approximate_size;
mod block_cache;
mod catalog;
mod checkpoint;
mod commits;
mod compact_range;
mod compaction;
mod destroy;
mod flush_job;
mod flush_signals;
mod flushes;
mod installer;
mod iterator;
mod key_check;
mod keyspaces;
mod level_versions;
mod lookup_key;
mod merge;
mod multi_get;
mod multi_scan;
mod open;
mod open_memory;
mod options_file;
mod prefixes;
mod pressure;
mod rate_limiter;
mod read;
mod recover;
mod snapshot;
mod table_blocks;
mod table_versions;
mod tables;
mod verify;
pub(crate) mod view;
mod visibility;
mod write;
mod write_statistics;

#[cfg(test)]
mod approximate_size_tests;
#[cfg(test)]
mod auto_compaction_tests;
#[cfg(test)]
mod background_error_tests;
#[cfg(test)]
mod cache_tests;
#[cfg(test)]
mod checkpoint_tests;
#[cfg(test)]
mod compaction_error_tests;
#[cfg(test)]
mod compaction_model_tests;
#[cfg(test)]
mod compaction_tests;
#[cfg(test)]
mod compression_tests;
#[cfg(test)]
mod crash_step_tests;
#[cfg(test)]
mod destroy_tests;
#[cfg(test)]
mod drop_tests;
#[cfg(test)]
mod encrypted_files_tests;
#[cfg(test)]
mod encrypted_log_tests;
#[cfg(test)]
mod encryption_tests;
#[cfg(test)]
mod fifo_tests;
#[cfg(test)]
mod flush_tests;
#[cfg(test)]
mod iterator_tests;
#[cfg(test)]
mod key_rotation_tests;
#[cfg(test)]
mod keyspace_admission_tests;
#[cfg(test)]
mod keyspace_compaction_tests;
#[cfg(test)]
mod keyspace_create_tests;
#[cfg(test)]
mod keyspace_drop_tests;
#[cfg(test)]
mod keyspace_open_tests;
#[cfg(test)]
mod keyspace_tests;
#[cfg(test)]
mod kill_tests;
#[cfg(test)]
mod level0_stall_tests;
#[cfg(test)]
mod level_read_tests;
#[cfg(test)]
mod level_tests;
#[cfg(test)]
mod log_bound_tests;
#[cfg(test)]
mod manual_compaction_tests;
#[cfg(test)]
mod memtable_ceiling_tests;
#[cfg(test)]
mod missing_segment_tests;
#[cfg(test)]
mod multi_get_tests;
#[cfg(test)]
mod multi_scan_tests;
#[cfg(test)]
mod open_files_tests;
#[cfg(test)]
mod open_tests;
#[cfg(test)]
mod options_file_tests;
#[cfg(test)]
mod pressure_tests;
#[cfg(test)]
mod range_delete_tests;
#[cfg(test)]
mod range_reclaim_tests;
#[cfg(test)]
mod rate_limiter_tests;
#[cfg(test)]
mod read_tests;
#[cfg(test)]
mod recovery_tests;
#[cfg(test)]
mod stall_tests;
#[cfg(test)]
mod statistics_tests;
#[cfg(test)]
mod statistics_write_tests;
#[cfg(test)]
mod switch_tests;
#[cfg(test)]
mod sync_wal_tests;
#[cfg(test)]
mod table_tests;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod verify_tests;
#[cfg(test)]
mod write_visibility_tests;

use std::path::Path;
use std::sync::Arc;

use arc_swap::ArcSwap;

use tessarikv_sync::{Sequencer, SnapshotRegistry};
use tessarikv_types::SequenceNumber;

use crate::log::LogWriter;
use crate::statistics::{Statistics, StatisticsSnapshot};
use crate::storage::{OsStorage, Storage};
use crate::{DbOptions, Error, Result, StorageMode};
use view::View;

pub(crate) use commits::{Check, Reads};
pub use iterator::DbIterator;
pub use read::{ScanPage, ScanRange};
pub use snapshot::Snapshot;

/// An open database.
///
/// # Example
///
/// ```
/// use tessarikv::{Db, DbOptions, KeyspaceId, ReadOptions, WriteBatch, WriteOptions};
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let db = Db::open("example", DbOptions::in_memory()).await?;
/// let mut batch = WriteBatch::new();
/// batch.put(KeyspaceId::DEFAULT, b"apple", b"red")?;
/// db.write(&batch, &WriteOptions::default()).await?;
/// let value = db.get(KeyspaceId::DEFAULT, b"apple", &ReadOptions::default()).await?;
/// assert_eq!(value.as_deref(), Some(&b"red"[..]));
/// # Ok(())
/// # }
/// ```
pub struct Db {
    sequencer: Arc<Sequencer>,
    /// Wakes writers waiting for their batch to become visible (see `visibility`).
    visibility: Arc<tokio::sync::Notify>,
    /// What each commit wrote, for transactions' conflict checks.
    commits: commits::Commits,
    /// The tables reads consult and writes land in.
    view: Arc<ArcSwap<View>>,
    /// The snapshots readers hold, which compaction keeps versions for.
    snapshots: Arc<SnapshotRegistry>,
    /// Set while a switch runs, so switches never overlap.
    #[cfg(test)]
    switching: std::sync::atomic::AtomicBool,
    /// The compaction pacer the installer was given, for tests to read.
    #[cfg(test)]
    limiter: Option<Arc<rate_limiter::RateLimiter>>,
    /// The write-ahead log; none for a database in memory.
    log: Option<LogWriter>,
    /// The flush installer; none for a database in memory.
    installer: Option<installer::Installer>,
    /// What the engine counts, when [`DbOptions::statistics`] is set.
    statistics: Option<Arc<Statistics>>,
    /// How this database holds its files: not at all, under the directory lock, or read-only
    /// (see `open`).
    access: open::Access,
}

impl Db {
    /// Opens the database at `path`.
    ///
    /// With [`StorageMode::Os`] the database lives in the directory `path` on the operating
    /// system's file system: it is created there when missing (unless
    /// [`DbOptions::create_if_missing`] is off), its log is replayed, and its lock keeps any
    /// other process from opening it. With [`StorageMode::Memory`] nothing touches storage, no
    /// write outlives the process, and `path` only names the database.
    ///
    /// Must be called on a tokio runtime, whose tasks then run the log.
    ///
    /// # Errors
    ///
    /// [`Error::NotFound`] or [`Error::AlreadyExists`] as the options require,
    /// [`Error::Locked`] when the database is open elsewhere, [`Error::Corruption`] when its
    /// log cannot be replayed under [`DbOptions::wal_recovery`], [`Error::Io`] when storage
    /// fails, and [`Error::Unsupported`] outside a tokio runtime.
    pub async fn open(path: impl AsRef<Path>, options: DbOptions) -> Result<Self> {
        match options.storage() {
            StorageMode::Memory => Ok(Self::open_in_memory(&options)),
            StorageMode::Os => Self::open_with_storage(path, options, Arc::new(OsStorage)).await,
        }
    }

    /// Opens the database at `path` on `storage`, as [`Db::open`] does on the operating
    /// system's file system, whatever [`DbOptions::storage`] says.
    ///
    /// # Errors
    ///
    /// As [`Db::open`].
    pub async fn open_with_storage(
        path: impl AsRef<Path>,
        options: DbOptions,
        storage: Arc<dyn Storage>,
    ) -> Result<Self> {
        Self::open_on(path.as_ref().to_path_buf(), options, storage).await
    }

    /// Closes the database: a running flush finishes, every write already acknowledged is
    /// written and synced, and the lock is released. Dropping a database without closing it
    /// leaves the log writer to finish in the background, unobserved.
    ///
    /// # Errors
    ///
    /// [`Error::LogFailed`] when the log had stopped or its final sync failed, and the error of
    /// a failed flush.
    pub async fn close(self) -> Result<()> {
        let flushed = match self.installer {
            Some(installer) => installer.close().await,
            None => Ok(()),
        };
        let logged = match self.log {
            Some(log) => log.close().await,
            None => Ok(()),
        };
        flushed.and(logged)
    }

    /// The snapshot a read takes: the highest sequence covering only completed batches.
    fn visible_sequence(&self) -> Result<SequenceNumber> {
        SequenceNumber::new(self.sequencer.visible()).map_err(|_| Error::SequenceExhausted)
    }

    /// What the engine counted: tickers since the database opened, and histograms since the
    /// previous call, which this call starts a new interval for. `None` unless
    /// [`DbOptions::statistics`] is set.
    #[must_use]
    pub fn statistics(&self) -> Option<StatisticsSnapshot> {
        self.statistics.as_deref().map(Statistics::snapshot)
    }

    /// Whether serializable transactions can run on this database.
    pub(crate) fn checks_reads(&self) -> bool {
        self.commits.serializable()
    }
}
