//! Opening a database on storage: its directory, its lock, its manifest and table files, and
//! the replay of its log above what the tables hold.
//!
//! What the replay recovers is flushed to a level-0 table before the database opens, and the
//! segments it came from are then removed, so a reopen leaves no log behind it and the next one
//! replays nothing twice. The order is a flush's: the table synced, the manifest naming it, then
//! the segments removed; a crash before the manifest edit leaves an unnamed table, removed at the
//! next open, and the log whole.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use tessarikv_constants::COMMIT_RING_SLOTS;
use tessarikv_format::FileKind;
use tessarikv_sync::Sequencer;

use super::Db;
use super::block_cache::BlockCache;
use super::commits::Commits;
use super::flush_job::{FileNumbers, Job, table_options};
use super::installer::{Flusher, Installer};
use super::key_check::check_key;
use super::keyspaces::options_of;
use super::rate_limiter::RateLimiter;
use super::recover::recover;
use super::view::View;
use crate::log::{LogExtras, LogWriter};
use crate::options::Reporter;
use crate::sealing::Sealing;
use crate::statistics::Statistics;
use crate::storage::{Storage, StorageLock};
use crate::version::Version;
use crate::version::manifest::{CURRENT, ManifestStore};
use crate::{DbOptions, Error, Result};

mod read_only;

pub(super) use read_only::Access;

/// The file whose presence marks a directory as holding a database.
pub(super) const IDENTITY: &str = "IDENTITY";

/// The file whose lock keeps a second process from opening the same database.
pub(super) const LOCK: &str = "LOCK";

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
        let total_memtable_bytes = options.total_memtable_bytes();
        let serializable = options.serializable_transactions();
        let keyspace = options.default_keyspace().clone();
        // Kept for the installer: `options` goes to the opening task.
        let named = options.clone();
        let sealing = options
            .encryption()
            .map(|encryption| Arc::new(Sealing::new(encryption, options.encryption_cipher())));
        let statistics = options.statistics().then(|| Arc::new(Statistics::new()));
        let events = Reporter::of(&options);
        let cache = block_cache(&options, &storage, sealing.clone(), statistics.clone());
        let opened = {
            let storage = Arc::clone(&storage);
            let max_manifest_bytes = options.max_manifest_bytes();
            let path = path.clone();
            let cache = Arc::clone(&cache);
            tokio::task::spawn_blocking(move || {
                let lock = prepare(storage.as_ref(), &path, &options)?;
                // The key is checked before any encrypted file is read.
                let existing = storage.exists(&path.join(CURRENT))?;
                check_key(storage.as_ref(), &path, cache.sealing(), existing, true)?;
                let found = ManifestStore::recover(storage.as_ref(), &path, cache.sealing())?;
                // One key for every log segment this open writes; a new open, a new salt.
                let log_seal = cache
                    .sealing()
                    .map(|sealing| sealing.new_file(FileKind::Log))
                    .transpose()?;
                let (version, number) = found.unwrap_or_else(|| (Version::empty(), 0));
                let mut files =
                    super::view::restore_tables(storage.as_ref(), &path, &version, &cache, true)?;
                let (replay, _) = super::keyspaces::tables_for(&options, version.keyspaces());
                let flushed = version.flushed_sequence();
                let recovered = recover(
                    storage.as_ref(),
                    &path,
                    &replay,
                    recovery,
                    flushed,
                    cache.sealing(),
                    true,
                )?;
                let number = number.checked_add(1).ok_or(Error::SequenceExhausted)?;
                let mut manifest = ManifestStore::create(
                    Arc::clone(&storage),
                    &path,
                    &version,
                    number,
                    cache.sealing(),
                )?
                .rolling_at(max_manifest_bytes, sealing);
                super::keyspaces::reconcile_keyspaces(&mut manifest, &options)?;
                super::compaction::refuse_deep_tables(&manifest.version(), &options)?;
                super::options_file::record(
                    storage.as_ref(),
                    &path,
                    &options,
                    number,
                    cache.sealing(),
                )?;
                let last = recovered.last_sequence.max(flushed);
                let replayed = recovered.last_sequence > flushed;
                if replayed {
                    let named = super::keyspaces::options_by_id(&options, version.keyspaces());
                    let tables = replay
                        .into_iter()
                        .map(|(id, table)| {
                            let keyspace = options_of(&named, options.default_keyspace(), id);
                            let threshold = keyspace.blob_threshold();
                            Ok((id, table_options(keyspace, 0)?, threshold, Arc::new(table)))
                        })
                        .collect::<Result<Vec<_>>>()?;
                    let job = Job {
                        storage,
                        dir: path,
                        tables,
                        flushed: last,
                        numbers: Arc::new(FileNumbers::new(manifest.version().next_file())),
                        cache,
                    };
                    // The newest level-0 tables: first among the files of their keyspaces.
                    for file in job.run(&mut manifest)? {
                        files.insert(0, file);
                    }
                }
                let (tables, names) =
                    super::keyspaces::tables_for(&options, manifest.version().keyspaces());
                Ok::<_, Error>((
                    lock,
                    View::recovered(tables, names, files),
                    recovered,
                    manifest,
                    last,
                    replayed,
                    log_seal,
                ))
            })
            .await
            .map_err(|_| Error::Unsupported {
                what: "opening a database after its opening task panicked",
            })??
        };
        let (lock, view, recovered, manifest, last_sequence, replayed, log_seal) = opened;
        let lock: Arc<dyn StorageLock> = Arc::from(lock);
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
            LogExtras {
                seal: log_seal,
                statistics: statistics.clone(),
                lock: Some(Arc::clone(&lock)),
                events: events.clone(),
            },
        )?;
        if replayed {
            // The replayed records are in a table the manifest names: their segments may go.
            log.releaser().release_through(last_sequence);
        }
        let sequencer = Arc::new(Sequencer::new(last_sequence, COMMIT_RING_SLOTS));
        let view = Arc::new(arc_swap::ArcSwap::from_pointee(view));
        let snapshots = Arc::new(tessarikv_sync::SnapshotRegistry::new());
        let next_file = manifest.version().next_file();
        let keyspaces = super::keyspaces::options_by_id(&named, manifest.version().keyspaces());
        let installer = Installer::start(Flusher {
            storage,
            dir: path,
            sequencer: Arc::clone(&sequencer),
            view: Arc::clone(&view),
            manifest: Some(manifest),
            log: log.releaser(),
            keyspace,
            keyspaces,
            cache,
            snapshots: Arc::clone(&snapshots),
            numbers: Arc::new(FileNumbers::new(next_file)),
            limiter: limiter.clone(),
            delayed_writes,
            total_memtable_bytes,
            switched: last_sequence,
            events,
            _lock: Arc::clone(&lock),
        });
        Ok(Self {
            sequencer,
            visibility: Arc::new(tokio::sync::Notify::new()),
            commits: Commits::new(serializable),
            view,
            snapshots,
            #[cfg(test)]
            switching: std::sync::atomic::AtomicBool::new(false),
            #[cfg(test)]
            limiter,
            log: Some(log),
            installer: Some(installer),
            statistics,
            access: Access::Exclusive { _lock: lock },
        })
    }
}

/// The block cache of a database opened on `storage` with `options`.
fn block_cache(
    options: &DbOptions,
    storage: &Arc<dyn Storage>,
    sealing: Option<Arc<Sealing>>,
    statistics: Option<Arc<Statistics>>,
) -> Arc<BlockCache> {
    Arc::new(
        BlockCache::new(
            options.block_cache_bytes(),
            options.default_keyspace().block_bytes(),
        )
        .with_open_files(options.max_open_files(), Arc::clone(storage))
        .with_sealing(sealing)
        .with_statistics(statistics)
        .with_reads_on_worker(options.block_reads_on_worker()),
    )
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
