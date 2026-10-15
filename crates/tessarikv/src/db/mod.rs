//! The database: atomic batches in, consistent point reads and ordered scans out.
//!
//! A write claims one sequence number per entry, hands its batch to the write-ahead log, waits
//! until the log holds it as durably as the write asks, applies it to the in-memory table and
//! completes the claim; a read takes the highest sequence covering only completed batches as its
//! snapshot. Writes from many tasks run concurrently, the log syncs once for every write waiting
//! on it, and reads never wait for writes. Opening a database on storage replays its log.
//!
//! This version serves the default keyspace and keeps every batch in memory: tables are not yet
//! flushed to storage, so the log grows until the database is reopened.

mod open;
mod read;
mod recover;
mod write;

#[cfg(test)]
mod recovery_tests;
#[cfg(test)]
mod tests;

use std::path::Path;
use std::sync::Arc;

use tessarikv_constants::COMMIT_RING_SLOTS;
use tessarikv_sync::Sequencer;
use tessarikv_types::{KeyspaceId, SequenceNumber};

use crate::log::LogWriter;
use crate::memtable::Memtable;
use crate::storage::{OsStorage, Storage, StorageLock};
use crate::{DbOptions, Error, Result, StorageMode};

pub use read::ScanPage;

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
    sequencer: Sequencer,
    table: Memtable,
    /// The write-ahead log; none for a database in memory.
    log: Option<LogWriter>,
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
                sequencer: Sequencer::new(0, COMMIT_RING_SLOTS),
                table: Memtable::new(),
                log: None,
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

    /// Closes the database: every write already acknowledged is written and synced, and the
    /// lock is released. Dropping a database without closing it leaves the log writer to
    /// finish in the background, unobserved.
    ///
    /// # Errors
    ///
    /// [`Error::LogFailed`] when the log had stopped or its final sync failed.
    pub async fn close(self) -> Result<()> {
        match self.log {
            Some(log) => log.close().await,
            None => Ok(()),
        }
    }

    /// Refuses a keyspace this database does not hold.
    fn check_keyspace(keyspace: KeyspaceId) -> Result<()> {
        if keyspace == KeyspaceId::DEFAULT {
            Ok(())
        } else {
            Err(Error::UnknownKeyspace {
                keyspace: keyspace.get(),
            })
        }
    }

    /// The snapshot a read takes: the highest sequence covering only completed batches.
    fn snapshot(&self) -> Result<SequenceNumber> {
        SequenceNumber::new(self.sequencer.visible()).map_err(|_| Error::SequenceExhausted)
    }
}
