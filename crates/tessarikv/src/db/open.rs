//! Opening a database on storage: its directory, its lock, and the replay of its log.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use tessarikv_constants::COMMIT_RING_SLOTS;
use tessarikv_sync::Sequencer;

use super::Db;
use super::recover::recover;
use crate::log::LogWriter;
use crate::memtable::Memtable;
use crate::storage::Storage;
use crate::{DbOptions, Error, Result};

/// The file whose presence marks a directory as holding a database.
const IDENTITY: &str = "IDENTITY";

/// The file whose lock keeps a second process from opening the same database.
const LOCK: &str = "LOCK";

impl Db {
    /// Opens the database at `path` on `storage`, creating it when `options` allow, and replays
    /// its log.
    pub(super) async fn open_on(
        path: PathBuf,
        options: DbOptions,
        storage: Arc<dyn Storage>,
    ) -> Result<Self> {
        let recovery = options.wal_recovery();
        let segment_bytes = options.wal_segment_bytes();
        let opened = {
            let storage = Arc::clone(&storage);
            let path = path.clone();
            tokio::task::spawn_blocking(move || {
                let lock = prepare(storage.as_ref(), &path, &options)?;
                let table = Memtable::new();
                let recovered = recover(storage.as_ref(), &path, &table, recovery)?;
                Ok::<_, Error>((lock, table, recovered))
            })
            .await
            .map_err(|_| Error::Unsupported {
                what: "opening a database after its opening task panicked",
            })??
        };
        let (lock, table, recovered) = opened;
        let next_sequence = recovered
            .last_sequence
            .checked_add(1)
            .ok_or(Error::SequenceExhausted)?;
        let log = LogWriter::start(
            storage,
            path,
            recovered.next_log_number,
            next_sequence,
            segment_bytes,
            recovered.segments,
        )?;
        Ok(Self {
            sequencer: Sequencer::new(recovered.last_sequence, COMMIT_RING_SLOTS),
            view: arc_swap::ArcSwap::from_pointee(super::view::View::new(0, table)),
            snapshots: tessarikv_sync::SnapshotRegistry::new(),
            #[cfg(test)]
            switching: std::sync::atomic::AtomicBool::new(false),
            log: Some(log),
            _lock: Some(lock),
        })
    }
}

/// Checks whether a database exists at `path` against `options`, creates one when asked, and
/// takes its lock. Runs on a blocking thread.
fn prepare(
    storage: &dyn Storage,
    path: &Path,
    options: &DbOptions,
) -> Result<Box<dyn crate::StorageLock>> {
    let identity = path.join(IDENTITY);
    let exists = storage.exists(&identity)?;
    if !exists && !options.create_if_missing() {
        return Err(Error::NotFound {
            path: path.to_path_buf(),
        });
    }
    if exists && options.error_if_exists() {
        return Err(Error::AlreadyExists {
            path: path.to_path_buf(),
        });
    }
    storage.create_dir_all(path)?;
    let lock = storage.lock(&path.join(LOCK))?;
    if !exists {
        let mut file = storage.create(&identity)?;
        file.sync()?;
        drop(file);
        storage.sync_dir(path)?;
    }
    Ok(lock)
}
