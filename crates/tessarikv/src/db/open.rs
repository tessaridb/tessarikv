//! Opening a database on storage: its directory, its lock, its manifest and table files, and
//! the replay of its log above what the tables hold.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use tessarikv_constants::{COMMIT_RING_SLOTS, LEVELS};
use tessarikv_sync::Sequencer;

use super::Db;
use super::block_cache::BlockCache;
use super::flush_job::{FileNumbers, table_path};
use super::installer::{Flusher, Installer};
use super::rate_limiter::RateLimiter;
use super::recover::recover;
use super::table_blocks::open_table;
use super::view::{TableFile, View};
use crate::log::LogWriter;
use crate::memtable::Memtable;
use crate::storage::Storage;
use crate::version::Version;
use crate::version::manifest::ManifestStore;
use crate::{DbOptions, Error, Result};

/// The file whose presence marks a directory as holding a database.
const IDENTITY: &str = "IDENTITY";

/// The file whose lock keeps a second process from opening the same database.
const LOCK: &str = "LOCK";

/// How far ahead of their rate compactions may write: a second's worth.
const COMPACTION_BURST: std::time::Duration = std::time::Duration::from_secs(1);

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
        let limiter =
            RateLimiter::new(options.compaction_bytes_per_second(), COMPACTION_BURST).map(Arc::new);
        let delayed_writes = options.delayed_write_bytes_per_second();
        let keyspace = options.default_keyspace().clone();
        let cache = Arc::new(BlockCache::new(
            options.block_cache_bytes(),
            keyspace.block_bytes(),
        ));
        let memtable_bytes = keyspace.memtable_bytes();
        let opened = {
            let storage = Arc::clone(&storage);
            let path = path.clone();
            let cache = Arc::clone(&cache);
            tokio::task::spawn_blocking(move || {
                let lock = prepare(storage.as_ref(), &path, &options)?;
                let (version, number) = ManifestStore::recover(storage.as_ref(), &path)?
                    .unwrap_or_else(|| (Version::empty(), 0));
                let files = restore_tables(storage.as_ref(), &path, &version, &cache)?;
                let table = Memtable::sized(memtable_bytes);
                let flushed = version.flushed_sequence();
                let recovered = recover(storage.as_ref(), &path, &table, recovery, flushed)?;
                let number = number.checked_add(1).ok_or(Error::SequenceExhausted)?;
                let manifest = ManifestStore::create(storage, &path, &version, number)?;
                let last = recovered.last_sequence.max(flushed);
                Ok::<_, Error>((
                    lock,
                    View::recovered(table, files),
                    recovered,
                    manifest,
                    last,
                ))
            })
            .await
            .map_err(|_| Error::Unsupported {
                what: "opening a database after its opening task panicked",
            })??
        };
        let (lock, view, recovered, manifest, last_sequence) = opened;
        let next_sequence = last_sequence
            .checked_add(1)
            .ok_or(Error::SequenceExhausted)?;
        let log = LogWriter::start(
            Arc::clone(&storage),
            path.clone(),
            recovered.next_log_number,
            next_sequence,
            segment_bytes,
            recovered.segments,
        )?;
        let sequencer = Arc::new(Sequencer::new(last_sequence, COMMIT_RING_SLOTS));
        let view = Arc::new(arc_swap::ArcSwap::from_pointee(view));
        let snapshots = Arc::new(tessarikv_sync::SnapshotRegistry::new());
        let next_file = manifest.version().next_file();
        let installer = Installer::start(Flusher {
            storage,
            dir: path,
            sequencer: Arc::clone(&sequencer),
            view: Arc::clone(&view),
            manifest: Some(manifest),
            log: log.releaser(),
            keyspace,
            cache,
            snapshots: Arc::clone(&snapshots),
            numbers: Arc::new(FileNumbers::new(next_file)),
            limiter: limiter.clone(),
            delayed_writes,
        });
        Ok(Self {
            sequencer,
            view,
            snapshots,
            #[cfg(test)]
            switching: std::sync::atomic::AtomicBool::new(false),
            #[cfg(test)]
            limiter,
            log: Some(log),
            installer: Some(installer),
            _lock: Some(lock),
        })
    }
}

/// Opens every table file `version` names, newest first, reading their data blocks through
/// `cache`, and removes the table files it does not name: those a crash left between writing a
/// table and naming it in the manifest. Runs on a blocking thread.
fn restore_tables(
    storage: &dyn Storage,
    dir: &Path,
    version: &Version,
    cache: &Arc<BlockCache>,
) -> Result<Vec<TableFile>> {
    let mut files = Vec::new();
    for level in 0..LEVELS {
        for meta in version.tables(0, level) {
            let path = table_path(dir, meta.id);
            let (table, blocks) = open_table(storage, &path, meta.id, cache, None)?;
            files.push(TableFile {
                meta: Arc::clone(meta),
                table,
                blocks,
            });
        }
    }
    for name in storage.list(dir)? {
        let named = name
            .strip_suffix(".sst")
            .and_then(|stem| stem.parse::<u64>().ok())
            .map(|id| files.iter().any(|file| file.meta.id == id));
        if named == Some(false) {
            storage.remove(&dir.join(&name))?;
        }
    }
    Ok(files)
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
