//! Freezing full in-memory tables and flushing them to table files in the background.
//!
//! One installer task per database on storage owns the manifest and is the only writer of the
//! read view. A writer whose batch leaves the newest in-memory table at or past its size asks for
//! a switch: the installer publishes a view with a new table in front, then starts its epoch, so
//! later batches land in the new table and every earlier one in the frozen table. Frozen tables
//! wait in order; one flush at a time takes the oldest and, each step durable before the next:
//!
//! 1. waits until every batch of the frozen table's epoch has completed;
//! 2. writes its versions to a table file, syncs it and its directory;
//! 3. appends a manifest edit naming the file and the flushed sequence, synced;
//!
//! then the installer publishes a view in which the file replaces the frozen table, and lets the
//! log remove the segments the flushed sequence covers.
//!
//! A crash before step 3 leaves an unnamed table file, removed when the database opens, and the
//! batches still in the log; after it, opening reads the file and skips those batches in the
//! log. A failed table or manifest write stops writes ([`Error::FlushFailed`]).
//!
//! Writes stall while the newest table is full and as many tables are frozen as the keyspace
//! allows: memory stays bounded by that many tables plus one.
//!
//! Between flushes the installer also runs compactions, one at a time (see
//! [`super::compaction`]); a failed compaction stops writes like a failed flush.

use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::Arc;

use arc_swap::ArcSwap;
use tessarikv_sync::{Sequencer, SnapshotRegistry};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

use super::block_cache::BlockCache;
use super::catalog::{Catalog, Command};
use super::compaction::manual::QUEUE;
use super::compaction::{self, Compactions};
use super::flush_job::FileNumbers;
use super::flush_signals::FlushSignals;
use super::flushes::{Flushed, wait};
use super::rate_limiter::RateLimiter;
use super::view::View;
use crate::log::LogRelease;
use crate::storage::Storage;
use crate::version::manifest::ManifestStore;
use crate::{Error, KeyspaceOptions, Result};

/// What the installer works with.
pub(super) struct Flusher {
    pub(super) storage: Arc<dyn Storage>,
    pub(super) dir: PathBuf,
    pub(super) sequencer: Arc<Sequencer>,
    pub(super) view: Arc<ArcSwap<View>>,
    /// Lent to the running flush.
    pub(super) manifest: Option<ManifestStore>,
    pub(super) log: LogRelease,
    pub(super) keyspace: KeyspaceOptions,
    /// The options of the keyspaces beside the default one, by id.
    pub(super) keyspaces: Vec<(u32, KeyspaceOptions)>,
    /// The cache flushed table files read their data blocks through.
    pub(super) cache: Arc<BlockCache>,
    /// The snapshots readers hold, which compactions keep versions for.
    pub(super) snapshots: Arc<SnapshotRegistry>,
    /// Table file numbers, shared by flushes and compactions.
    pub(super) numbers: Arc<FileNumbers>,
    /// Paces compaction output, when the options ask for it.
    pub(super) limiter: Option<Arc<RateLimiter>>,
    /// The rate writes go at while level 0 is past its slowdown trigger, in bytes a second.
    pub(super) delayed_writes: u64,
    /// The ceiling on the newest tables together, in bytes; zero sets none.
    pub(super) total_memtable_bytes: u64,
    /// Every sequence up to this one is in a frozen table or on table files.
    pub(super) switched: u64,
}

/// The installer task and the handles that steer it.
pub(super) struct Installer {
    pub(super) signals: Arc<FlushSignals>,
    /// Where the table files are, for `Db::verify_checksums`.
    pub(super) storage: Arc<dyn Storage>,
    pub(super) dir: PathBuf,
    /// The engine's block cache, whose sealing opens encrypted table files.
    pub(super) cache: Arc<BlockCache>,
    /// Range compactions and keyspace changes (see `Db::compact_range`, `Db::create_keyspace`).
    pub(super) requests: mpsc::Sender<Command>,
    shutdown: watch::Sender<bool>,
    task: JoinHandle<Result<()>>,
}

impl Installer {
    /// Starts the installer task on the current tokio runtime.
    pub(super) fn start(flusher: Flusher) -> Self {
        let max_frozen = flusher
            .all_keyspaces()
            .map(|(_, options)| options.max_immutable_memtables())
            .min()
            .unwrap_or(1);
        let signals = Arc::new(FlushSignals::new(
            max_frozen,
            flusher.delayed_writes,
            flusher.total_memtable_bytes,
        ));
        // Everything at or below the last switch is on table files when the database opens.
        signals.set_flushed(flusher.switched);
        let (shutdown, stop) = watch::channel(false);
        let (requests, requested) = mpsc::channel(QUEUE);
        let (storage, dir, cache) = (
            Arc::clone(&flusher.storage),
            flusher.dir.clone(),
            Arc::clone(&flusher.cache),
        );
        let task = tokio::spawn(run(flusher, Arc::clone(&signals), stop, requested));
        Self {
            signals,
            storage,
            dir,
            cache,
            requests,
            shutdown,
            task,
        }
    }

    /// Lets a running flush finish, then stops the task; frozen tables not yet flushed stay in
    /// the log.
    ///
    /// # Errors
    ///
    /// The error that stopped a flush.
    pub(super) async fn close(self) -> Result<()> {
        // A send fails only when the task has already ended, which the join reports.
        let _ = self.shutdown.send(true);
        self.task.await.map_err(|_| Error::FlushFailed)?
    }
}

/// Serves switch requests and runs flushes until shutdown; a failure is published, and stalled
/// writers woken, before it is returned.
async fn run(
    mut flusher: Flusher,
    signals: Arc<FlushSignals>,
    mut stop: watch::Receiver<bool>,
    mut requests: mpsc::Receiver<Command>,
) -> Result<()> {
    let mut frozen = VecDeque::new();
    let mut running: Option<JoinHandle<Result<Flushed>>> = None;
    let mut compactions = Compactions::default();
    let mut catalog = Catalog::default();
    let mut stopping = false;
    // Levels recovered past a trigger compact now: no write may come to wake the installer
    // while the levels hold writes.
    settle(&mut flusher, &mut compactions, &mut catalog, &signals, true).await?;
    loop {
        tokio::select! {
            // Shutdown first: no new flush starts after it, the running one finishes.
            biased;
            _ = stop.changed(), if !stopping => stopping = true,
            finished = wait(&mut running), if running.is_some() => {
                running = None;
                match finished.and_then(|flushed| flusher.install(&mut frozen, flushed)) {
                    Ok(through) => signals.set_flushed(through),
                    Err(error) => {
                        signals.fail(&error);
                        return Err(error);
                    }
                }
            }
            compacted = compactions.finished(), if compactions.is_running() => {
                if let Err(error) = compacted {
                    signals.fail(&error);
                    return Err(error);
                }
            }
            Some(command) = requests.recv(), if !stopping => match command {
                Command::Compact(request) => compactions.request(request),
                Command::Create(create) => catalog.create(create),
                Command::Drop(drop) => catalog.drop_keyspace(drop),
            },
            () = signals.requested(), if !stopping => {}
            () = tokio::time::sleep(compaction::SWEEP_INTERVAL), if compactions.has_obsolete() => {}
        }
        // Finished work is recorded even while stopping; no new job starts then.
        settle(
            &mut flusher,
            &mut compactions,
            &mut catalog,
            &signals,
            !stopping,
        )
        .await?;
        if stopping {
            if running.is_none() && !compactions.is_running() {
                compactions.step(&mut flusher, false).await?;
                return Ok(());
            }
            continue;
        }
        signals.take_request();
        flusher.switch_if_full(&signals, &mut frozen);
        if running.is_none()
            && let Some(oldest) = frozen.front()
        {
            running = Some(flusher.start_flush(oldest, &signals)?);
        }
    }
}

/// Makes the waiting keyspace changes, records finished compactions and, with `start`, starts
/// those due, then publishes what the levels ask of writers, who look again; a failure is
/// published before it is returned.
async fn settle(
    flusher: &mut Flusher,
    compactions: &mut Compactions,
    catalog: &mut Catalog,
    signals: &FlushSignals,
    start: bool,
) -> Result<()> {
    let stepped = async {
        let created = catalog.step(flusher, compactions).await?;
        Ok::<_, Error>(compactions.step(flusher, start).await? || created)
    };
    match stepped.await {
        Ok(published) => {
            signals.set_load(flusher.load());
            if published {
                signals.installed();
            }
            Ok(())
        }
        Err(error) => {
            signals.fail(&error);
            Err(error)
        }
    }
}
