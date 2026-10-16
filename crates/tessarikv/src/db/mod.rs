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

mod block_cache;
mod commits;
mod compact_range;
mod compaction;
mod flush_job;
mod flush_signals;
mod installer;
mod iterator;
mod merge;
mod multi_get;
mod open;
mod prefixes;
mod rate_limiter;
mod read;
mod recover;
mod snapshot;
mod table_blocks;
mod table_versions;
mod tables;
mod view;
mod write;

#[cfg(test)]
mod cache_tests;
#[cfg(test)]
mod compaction_model_tests;
#[cfg(test)]
mod compaction_tests;
#[cfg(test)]
mod compression_tests;
#[cfg(test)]
mod crash_step_tests;
#[cfg(test)]
mod fifo_tests;
#[cfg(test)]
mod flush_tests;
#[cfg(test)]
mod iterator_tests;
#[cfg(test)]
mod kill_tests;
#[cfg(test)]
mod level0_stall_tests;
#[cfg(test)]
mod level_tests;
#[cfg(test)]
mod manual_compaction_tests;
#[cfg(test)]
mod multi_get_tests;
#[cfg(test)]
mod open_tests;
#[cfg(test)]
mod range_delete_tests;
#[cfg(test)]
mod rate_limiter_tests;
#[cfg(test)]
mod read_tests;
#[cfg(test)]
mod recovery_tests;
#[cfg(test)]
mod stall_tests;
#[cfg(test)]
mod switch_tests;
#[cfg(test)]
mod table_tests;
#[cfg(test)]
mod tests;

use std::path::Path;
use std::sync::Arc;

use arc_swap::ArcSwap;

use tessarikv_constants::COMMIT_RING_SLOTS;
#[cfg(test)]
use tessarikv_sync::Switch;
use tessarikv_sync::{Sequencer, SnapshotRegistry};
use tessarikv_types::{KeyspaceId, SequenceNumber};

use crate::log::LogWriter;
use crate::memtable::Memtable;
use crate::storage::{OsStorage, Storage, StorageLock};
use crate::{DbOptions, Error, Result, StorageMode};
use view::View;

pub(crate) use commits::{Check, Reads};
pub use iterator::DbIterator;
pub use read::ScanPage;
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
    /// Held for as long as the database is open.
    _lock: Option<Box<dyn StorageLock>>,
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
            StorageMode::Memory => Ok(Self {
                sequencer: Arc::new(Sequencer::new(0, COMMIT_RING_SLOTS)),
                commits: commits::Commits::new(options.serializable_transactions()),
                view: Arc::new(ArcSwap::from_pointee(View::new(
                    0,
                    Memtable::sized(options.default_keyspace().memtable_bytes()),
                ))),
                snapshots: Arc::new(SnapshotRegistry::new()),
                #[cfg(test)]
                switching: std::sync::atomic::AtomicBool::new(false),
                #[cfg(test)]
                limiter: None,
                log: None,
                installer: None,
                _lock: None,
            }),
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

    /// Refuses a keyspace this database does not hold.
    pub(crate) fn check_keyspace(keyspace: KeyspaceId) -> Result<()> {
        if keyspace == KeyspaceId::DEFAULT {
            Ok(())
        } else {
            Err(Error::UnknownKeyspace {
                keyspace: keyspace.get(),
            })
        }
    }

    /// Starts a new in-memory table: later claims carry the next epoch and land in it, while
    /// the table it replaces stays readable. Returns `None` when another switch is running.
    ///
    /// The view naming the new table is published BEFORE the sequencer starts its epoch, so a
    /// claim carrying that epoch always finds it (see [`Sequencer::switch`]).
    #[cfg(test)]
    pub(crate) fn switch_memtable(&self) -> Option<Switch> {
        use std::sync::atomic::Ordering;
        // Acquire: orders this switch after the previous one, whose Release clears the flag.
        if self
            .switching
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            return None;
        }
        let next = self.view.load().with_next_table();
        let epoch = next.newest_epoch();
        self.view.store(Arc::new(next));
        let switch = self.sequencer.switch();
        debug_assert_eq!(
            switch.epoch(),
            epoch,
            "the view and the sequencer agree on epochs"
        );
        // Release: publishes this switch's view to the next switcher.
        self.switching.store(false, Ordering::Release);
        Some(switch)
    }

    /// The snapshot a read takes: the highest sequence covering only completed batches.
    fn visible_sequence(&self) -> Result<SequenceNumber> {
        SequenceNumber::new(self.sequencer.visible()).map_err(|_| Error::SequenceExhausted)
    }

    /// Writes every batch committed before the call to table files, and returns once they are
    /// there: the in-memory table holding them is frozen even if not full, then flushed like a
    /// full one. Later writes go on meanwhile. In memory there are no table files and it returns
    /// at once.
    ///
    /// # Errors
    ///
    /// [`Error::FlushFailed`] or [`Error::CompactionFailed`] when the background work stopped.
    pub async fn flush(&self) -> Result<()> {
        let Some(installer) = &self.installer else {
            return Ok(());
        };
        installer
            .signals
            .flush_through(self.sequencer.visible())
            .await
    }

    /// Whether serializable transactions can run on this database.
    pub(crate) fn checks_reads(&self) -> bool {
        self.commits.serializable()
    }
}
