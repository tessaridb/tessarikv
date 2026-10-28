//! Opening a database read-only: its tables and its log read as they are, nothing written.
//!
//! A read-only open takes no lock and changes no file. It reads the manifest and opens the
//! tables it names, then replays the log above them into in-memory tables, which reads consult
//! like any others; nothing flushes them, and no background work runs. What a read-write open
//! repairs is left in place: a damaged log tail ends the replay where it starts (and fails the
//! open under [`WalRecovery::Absolute`]), and files a crash left unnamed stay where they are.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use tessarikv_constants::COMMIT_RING_SLOTS;
use tessarikv_sync::{Sequencer, SnapshotRegistry};

use super::super::Db;
use super::super::block_cache::BlockCache;
use super::super::commits::Commits;
use super::super::key_check::check_key;
use super::super::keyspaces::{missing_keyspaces, refused, tables_for};
use super::super::recover::recover;
use super::super::view::{View, restore_tables};
use super::{IDENTITY, block_cache};
#[cfg(doc)]
use crate::WalRecovery;
use crate::sealing::Sealing;
use crate::statistics::Statistics;
use crate::storage::{OsStorage, Storage, StorageLock};
use crate::version::Version;
use crate::version::manifest::ManifestStore;
use crate::{DbOptions, Error, Result, StorageMode};

/// How a database holds its files.
pub(in crate::db) enum Access {
    /// In memory: there are none.
    Memory,
    /// Under the directory lock, shared with the background tasks: it is released when the last
    /// of them ends, so no other database opens the directory while this one still writes there.
    Exclusive {
        /// Held, never read.
        _lock: Arc<dyn StorageLock>,
        /// The options that differ from the previous open's record.
        changed: Vec<crate::OptionChange>,
    },
    /// Read-only: read, never written.
    ReadOnly(ReadOnlyFiles),
}

/// Where a read-only database reads its files.
pub(in crate::db) struct ReadOnlyFiles {
    /// The storage holding them.
    pub(in crate::db) storage: Arc<dyn Storage>,
    /// Their directory.
    pub(in crate::db) dir: PathBuf,
    /// The cache their blocks are read through.
    pub(in crate::db) cache: Arc<BlockCache>,
}

impl Db {
    /// Opens the database at `path` read-only, on the operating system's file system: every
    /// read sees the database as its files held it at the open, batches in its log included,
    /// and nothing it does changes a file. No lock is taken, so a read-only open runs beside
    /// other read-only opens and beside a database that is closed; a database writing the
    /// directory meanwhile may remove files this one reads, which it reports as the storage's
    /// error.
    ///
    /// Every change is refused with [`Error::ReadOnly`]: writes, transactions' commits,
    /// [`Db::flush`], [`Db::sync_wal`], [`Db::compact_range`], creating and dropping keyspaces,
    /// and [`Db::checkpoint`]. [`Db::wait_for_compact`] returns at once, as nothing runs in the
    /// background, and [`Db::verify_checksums`] reads the table files as on any database.
    ///
    /// The options name the database's keyspaces as for [`Db::open`]; one the database does
    /// not hold is refused, whatever [`DbOptions::create_missing_keyspaces`] says.
    /// [`DbOptions::create_if_missing`] and [`DbOptions::error_if_exists`] are not consulted.
    /// A damaged log tail ends what is read under [`WalRecovery::PointInTime`], and is left in
    /// the file for a read-write open to repair.
    ///
    /// # Errors
    ///
    /// [`Error::NotFound`] when no database is at `path`, [`Error::InvalidArgument`] when the
    /// options and the database's keyspaces differ, [`Error::Corruption`] when its manifest or
    /// tables are damaged or its log is under [`WalRecovery::Absolute`], [`Error::Unsupported`]
    /// for [`StorageMode::Memory`] (whose database has no files to read) or outside a tokio
    /// runtime, the errors of a wrong or missing encryption key, and the storage's error for a
    /// failed read.
    pub async fn open_read_only(path: impl AsRef<Path>, options: DbOptions) -> Result<Self> {
        match options.storage() {
            StorageMode::Memory => Err(Error::Unsupported {
                what: "a read-only open of a database in memory",
            }),
            StorageMode::Os => {
                Self::open_read_only_with_storage(path, options, Arc::new(OsStorage)).await
            }
        }
    }

    /// Opens the database at `path` on `storage` read-only, as [`Db::open_read_only`] does on
    /// the operating system's file system, whatever [`DbOptions::storage`] says.
    ///
    /// # Errors
    ///
    /// As [`Db::open_read_only`].
    pub async fn open_read_only_with_storage(
        path: impl AsRef<Path>,
        options: DbOptions,
        storage: Arc<dyn Storage>,
    ) -> Result<Self> {
        let dir = path.as_ref().to_path_buf();
        let serializable = options.serializable_transactions();
        let sealing = options
            .encryption()
            .map(|encryption| Arc::new(Sealing::new(encryption, options.encryption_cipher())));
        let statistics = options.statistics().then(|| Arc::new(Statistics::new()));
        let cache = block_cache(&options, &storage, sealing, statistics.clone());
        let files = ReadOnlyFiles {
            storage,
            dir,
            cache,
        };
        let (view, last_sequence) = {
            let storage = Arc::clone(&files.storage);
            let dir = files.dir.clone();
            let cache = Arc::clone(&files.cache);
            tokio::task::spawn_blocking(move || read(storage.as_ref(), &dir, &options, &cache))
                .await
                .map_err(|_| Error::Unsupported {
                    what: "opening a database after its opening task panicked",
                })??
        };
        Ok(Self {
            sequencer: Arc::new(Sequencer::new(last_sequence, COMMIT_RING_SLOTS)),
            visibility: Arc::new(tokio::sync::Notify::new()),
            commits: Commits::new(serializable),
            view: Arc::new(arc_swap::ArcSwap::from_pointee(view)),
            snapshots: Arc::new(SnapshotRegistry::new()),
            #[cfg(test)]
            switching: std::sync::atomic::AtomicBool::new(false),
            #[cfg(test)]
            limiter: None,
            log: None,
            installer: None,
            statistics,
            access: Access::ReadOnly(files),
        })
    }

    /// Refuses `what` on a database opened read-only.
    pub(in crate::db) fn refuse_read_only(&self, what: &'static str) -> Result<()> {
        match self.access {
            Access::ReadOnly(_) => Err(Error::ReadOnly { what }),
            Access::Memory | Access::Exclusive { .. } => Ok(()),
        }
    }

    /// The files of a database opened read-only; `None` for any other.
    pub(in crate::db) fn read_only_files(&self) -> Option<&ReadOnlyFiles> {
        match &self.access {
            Access::ReadOnly(files) => Some(files),
            Access::Memory | Access::Exclusive { .. } => None,
        }
    }
}

/// Reads the database in `dir` without changing a file: its key check, its manifest, the tables
/// that names and its log above them. Returns the read view and the last sequence it holds. Runs
/// on a blocking thread.
fn read(
    storage: &dyn Storage,
    dir: &Path,
    options: &DbOptions,
    cache: &Arc<BlockCache>,
) -> Result<(View, u64)> {
    if !storage.exists(&dir.join(IDENTITY))? {
        return Err(Error::NotFound {
            path: dir.to_path_buf(),
        });
    }
    check_key(storage, dir, cache.sealing(), true, false)?;
    let version = ManifestStore::recover(storage, dir, cache.sealing())?
        .map_or_else(Version::empty, |(version, _)| version);
    if !missing_keyspaces(&version, options)?.is_empty() {
        return Err(refused("the options name a keyspace it does not hold"));
    }
    let files = restore_tables(storage, dir, &version, cache, false)?;
    let (tables, names) = tables_for(options, version.keyspaces());
    let flushed = version.flushed_sequence();
    let recovered = recover(
        storage,
        dir,
        &tables,
        options.wal_recovery(),
        flushed,
        cache.sealing(),
        false,
    )?;
    let last = recovered.last_sequence.max(flushed);
    Ok((View::recovered(tables, names, files), last))
}

#[cfg(test)]
mod tests;
