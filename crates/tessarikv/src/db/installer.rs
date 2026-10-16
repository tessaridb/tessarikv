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
use super::compaction::manual::{QUEUE, Request};
use super::compaction::{self, Compactions};
use super::flush_job::{FileNumbers, Job, table_options};
use super::flush_signals::FlushSignals;
use super::keyspaces::options_of;
use super::rate_limiter::RateLimiter;
use super::view::{TableFile, View};
use crate::log::LogRelease;
use crate::memtable::Memtable;
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
    /// Every sequence up to this one is in a frozen table or on table files.
    pub(super) switched: u64,
}

/// The frozen tables of an epoch, one per keyspace, waiting for their flush.
struct Frozen {
    epoch: u8,
    tables: Vec<(u32, Arc<Memtable>)>,
    /// The last sequence of its epoch.
    flushed: u64,
}

/// What a finished flush hands back: the manifest, and the table files or why there are none.
type Flushed = (ManifestStore, Result<Vec<TableFile>>);

/// The installer task and the handles that steer it.
pub(super) struct Installer {
    pub(super) signals: Arc<FlushSignals>,
    /// Range compaction requests (see `Db::compact_range`).
    pub(super) requests: mpsc::Sender<Request>,
    shutdown: watch::Sender<bool>,
    task: JoinHandle<Result<()>>,
}

impl Installer {
    /// Starts the installer task on the current tokio runtime.
    pub(super) fn start(flusher: Flusher) -> Self {
        let signals = Arc::new(FlushSignals::new(&flusher.keyspace, flusher.delayed_writes));
        // Everything at or below the last switch is on table files when the database opens.
        signals.set_flushed(flusher.switched);
        let (shutdown, stop) = watch::channel(false);
        let (requests, requested) = mpsc::channel(QUEUE);
        let task = tokio::spawn(run(flusher, Arc::clone(&signals), stop, requested));
        Self {
            signals,
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
    mut requests: mpsc::Receiver<Request>,
) -> Result<()> {
    let mut frozen = VecDeque::new();
    let mut running: Option<JoinHandle<Result<Flushed>>> = None;
    let mut compactions = Compactions::default();
    let mut stopping = false;
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
            Some(request) = requests.recv(), if !stopping => compactions.request(request),
            () = signals.requested(), if !stopping => {}
            () = tokio::time::sleep(compaction::SWEEP_INTERVAL), if compactions.has_obsolete() => {}
        }
        // Finished work is recorded even while stopping; no new job starts then.
        match compactions.step(&mut flusher, !stopping).await {
            // Writers held by level 0 look again.
            Ok(published) => {
                if let Some(owed) = compaction::pending_bytes(&flusher) {
                    signals.set_owed(owed);
                }
                if published {
                    signals.installed();
                }
            }
            Err(error) => {
                signals.fail(&error);
                return Err(error);
            }
        }
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

/// The running flush's result; a flush whose task or blocking thread died stops flushing.
async fn wait(running: &mut Option<JoinHandle<Result<Flushed>>>) -> Result<Flushed> {
    match running.as_mut() {
        Some(flush) => flush.await.map_err(|_| Error::FlushFailed)?,
        None => std::future::pending().await,
    }
}

impl Flusher {
    /// Freezes the newest tables when one is full and another epoch may be frozen.
    fn switch_if_full(&mut self, signals: &FlushSignals, frozen: &mut VecDeque<Frozen>) {
        let current = self.view.load_full();
        if !signals.should_switch(&current, frozen.len(), self.switched) {
            return;
        }
        let epoch = current.newest_epoch();
        let Ok(tables) = current.shared_epoch(epoch) else {
            return;
        };
        // The view naming the next table is published before its epoch starts.
        self.view.store(Arc::new(current.with_next_table()));
        let flushed = self.sequencer.switch().boundary().saturating_sub(1);
        self.switched = flushed;
        frozen.push_back(Frozen {
            epoch,
            tables,
            flushed,
        });
    }

    /// Starts flushing `oldest` (steps 1 to 3 of the module documentation).
    fn start_flush(
        &mut self,
        oldest: &Frozen,
        signals: &Arc<FlushSignals>,
    ) -> Result<JoinHandle<Result<Flushed>>> {
        let tables = oldest
            .tables
            .iter()
            .map(|(keyspace, table)| {
                let keyspace_options = options_of(&self.keyspaces, &self.keyspace, *keyspace);
                let options = table_options(keyspace_options, 0)?;
                Ok((*keyspace, options, Arc::clone(table)))
            })
            .collect::<Result<Vec<_>>>()?;
        let mut manifest = self.manifest.take().ok_or(Error::FlushFailed)?;
        let job = Job {
            storage: Arc::clone(&self.storage),
            dir: self.dir.clone(),
            tables,
            flushed: oldest.flushed,
            numbers: Arc::clone(&self.numbers),
            cache: Arc::clone(&self.cache),
        };
        let (sequencer, signals) = (Arc::clone(&self.sequencer), Arc::clone(signals));
        Ok(tokio::spawn(async move {
            // Step 1: every claim of the frozen epoch has completed.
            loop {
                let completed = signals.visible.notified();
                if sequencer.watch(job.flushed) {
                    break;
                }
                completed.await;
            }
            sequencer.unwatch();
            // Steps 2 and 3, on a blocking thread; the manifest goes down with a panicked one.
            tokio::task::spawn_blocking(move || {
                let written = job.run(&mut manifest);
                (manifest, written)
            })
            .await
            .map_err(|_| Error::FlushFailed)
        }))
    }

    /// Publishes the view in which the flushed file replaces the oldest frozen table, and
    /// releases the log it covers; returns the sequence now on table files.
    fn install(&mut self, frozen: &mut VecDeque<Frozen>, flushed: Flushed) -> Result<u64> {
        let (manifest, written) = flushed;
        self.manifest = Some(manifest);
        let files = written?;
        let oldest = frozen.pop_front().ok_or(Error::FlushFailed)?;
        let current = self.view.load_full();
        self.view
            .store(Arc::new(current.after_flush(oldest.epoch, files)?));
        self.log.release_through(oldest.flushed);
        Ok(oldest.flushed)
    }
}
