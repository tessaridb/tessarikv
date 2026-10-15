//! Flushing in-memory tables to table files in the background.
//!
//! One installer task per database on storage owns the manifest and is the only writer of the
//! read view. A writer whose batch leaves the newest in-memory table at or past its size asks
//! for a flush; the installer then, in this order, each step durable before the next:
//!
//! 1. switches: publishes a view with a new table in front, then starts its epoch, so later
//!    batches land in the new table and every earlier one in the old;
//! 2. waits until every batch of the old epoch has completed (`visible` reaches the boundary);
//! 3. writes the old table's versions to a table file, syncs it and its directory;
//! 4. appends a manifest edit naming the file and the flushed sequence, synced;
//! 5. publishes a view in which the file replaces the old table;
//! 6. lets the log remove the segments the flushed sequence covers.
//!
//! A crash before step 4 leaves an unnamed table file, removed when the database opens, and the
//! batches still in the log; after it, opening reads the file and skips those batches in the
//! log. A failed table or manifest write stops writes ([`Error::FlushFailed`]).

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use arc_swap::ArcSwap;
use tessarikv_format::{Compression, ManifestEdit, TableMeta, TableOptions};
use tessarikv_sync::Sequencer;
use tokio::sync::{Notify, watch};
use tokio::task::JoinHandle;

use super::tables::{build, open};
use super::view::{TableFile, View};
use crate::log::LogRelease;
use crate::memtable::Memtable;
use crate::storage::Storage;
use crate::version::manifest::ManifestStore;
use crate::{Error, KeyspaceOptions, Result};

/// What writers and the installer tell each other.
pub(super) struct FlushSignals {
    /// A flush is asked for or running; later requests fold into it.
    pending: AtomicBool,
    /// Wakes the installer for a flush.
    wake: Notify,
    /// Wakes the installer when the batches it waits for have completed.
    pub(super) visible: Notify,
    /// A flush failed: writes are refused until the database is reopened.
    failed: AtomicBool,
    /// Bytes the newest in-memory table holds before a flush is asked for.
    memtable_bytes: u64,
}

impl FlushSignals {
    /// Asks for a flush when `newest` has reached its size.
    pub(super) fn check(&self, newest: &Memtable) {
        if newest.approximate_bytes() >= self.memtable_bytes
            // AcqRel: one requester wins; pairs with the installer clearing the flag.
            && !self.pending.swap(true, Ordering::AcqRel)
        {
            self.wake.notify_one();
        }
    }

    /// Whether a flush failed, so writes must stop.
    pub(super) fn failed(&self) -> bool {
        // Acquire: pairs with the Release store when the installer gives up.
        self.failed.load(Ordering::Acquire)
    }
}

/// What a flush works with.
pub(super) struct Flusher {
    pub(super) storage: Arc<dyn Storage>,
    pub(super) dir: PathBuf,
    pub(super) sequencer: Arc<Sequencer>,
    pub(super) view: Arc<ArcSwap<View>>,
    /// Lent to a blocking thread while a flush writes it.
    pub(super) manifest: Option<ManifestStore>,
    pub(super) log: LogRelease,
    pub(super) keyspace: KeyspaceOptions,
}

/// The installer task and the handles that steer it.
pub(super) struct Installer {
    pub(super) signals: Arc<FlushSignals>,
    shutdown: watch::Sender<bool>,
    task: JoinHandle<Result<()>>,
}

impl Installer {
    /// Starts the installer task on the current tokio runtime.
    pub(super) fn start(flusher: Flusher) -> Self {
        let signals = Arc::new(FlushSignals {
            pending: AtomicBool::new(false),
            wake: Notify::new(),
            visible: Notify::new(),
            failed: AtomicBool::new(false),
            memtable_bytes: flusher.keyspace.memtable_bytes(),
        });
        let (shutdown, stop) = watch::channel(false);
        let task = tokio::spawn(run(flusher, Arc::clone(&signals), stop));
        Self {
            signals,
            shutdown,
            task,
        }
    }

    /// Lets a running flush finish, then stops the task.
    ///
    /// # Errors
    ///
    /// The error that stopped an earlier flush.
    pub(super) async fn close(self) -> Result<()> {
        // A send fails only when the task has already ended, which the join reports.
        let _ = self.shutdown.send(true);
        self.task.await.map_err(|_| Error::FlushFailed)?
    }
}

/// Serves flush requests until shutdown; a failure is published before it is returned.
async fn run(
    mut flusher: Flusher,
    signals: Arc<FlushSignals>,
    mut stop: watch::Receiver<bool>,
) -> Result<()> {
    loop {
        tokio::select! {
            // Shutdown first: a close never waits behind another flush.
            biased;
            _ = stop.changed() => return Ok(()),
            () = signals.wake.notified() => {}
        }
        if let Err(error) = flusher.flush(&signals).await {
            // Release: writers that see the flag see the failure's effects.
            signals.failed.store(true, Ordering::Release);
            return Err(error);
        }
        // Release: pairs with a requester's swap. A writer that found the flag set while this
        // flush ran asked for nothing, so the size is checked again here.
        signals.pending.store(false, Ordering::Release);
        if let Some(newest) = flusher.view.load().memtables().next() {
            signals.check(newest);
        }
    }
}

impl Flusher {
    /// Flushes the newest in-memory table (see the module documentation).
    async fn flush(&mut self, signals: &FlushSignals) -> Result<()> {
        let current = self.view.load_full();
        let epoch = current.newest_epoch();
        let table = current.shared_table(epoch)?;
        // Step 1: the view naming the next table is published before its epoch starts.
        self.view.store(Arc::new(current.with_next_table()));
        let flushed = self.sequencer.switch().boundary().saturating_sub(1);
        // Step 2: every claim below the boundary has completed.
        loop {
            let completed = signals.visible.notified();
            if self.sequencer.watch(flushed) {
                break;
            }
            completed.await;
        }
        self.sequencer.unwatch();
        // Steps 3 and 4, on a blocking thread.
        let job = Job {
            storage: Arc::clone(&self.storage),
            dir: self.dir.clone(),
            options: table_options(&self.keyspace)?,
            table,
            flushed,
        };
        let mut manifest = self.manifest.take().ok_or(Error::FlushFailed)?;
        let (manifest, written) = tokio::task::spawn_blocking(move || {
            let written = job.run(&mut manifest);
            (manifest, written)
        })
        .await
        .map_err(|_| Error::FlushFailed)?;
        self.manifest = Some(manifest);
        let file = written?;
        // Step 5: the file replaces the table in one view.
        let current = self.view.load_full();
        self.view.store(Arc::new(current.after_flush(epoch, file)?));
        // Step 6.
        self.log.release_through(flushed);
        Ok(())
    }
}

/// A flush's blocking part: the table file and the manifest edit.
struct Job {
    storage: Arc<dyn Storage>,
    dir: PathBuf,
    options: TableOptions,
    table: Arc<Memtable>,
    flushed: u64,
}

impl Job {
    /// Writes the table file and its manifest edit; returns the file, or none when the table
    /// held no version (its sequences were claimed by batches that were never applied).
    fn run(self, manifest: &mut ManifestStore) -> Result<Option<TableFile>> {
        let mut edit = ManifestEdit {
            flushed_sequence: Some(self.flushed),
            ..ManifestEdit::default()
        };
        let mut versions = self.table.iter();
        versions.seek_to_first();
        if versions.entry().is_none() {
            manifest.append(&edit)?;
            return Ok(None);
        }
        let built = build(&self.table, self.options)?;
        let id = manifest.version().next_file();
        let path = table_path(&self.dir, id);
        let mut file = self.storage.create(&path)?;
        file.append(&built.bytes)?;
        file.sync()?;
        drop(file);
        self.storage.sync_dir(&self.dir)?;
        let meta = TableMeta {
            keyspace: 0,
            level: 0,
            id,
            size: u64::try_from(built.bytes.len()).map_err(|_| Error::FlushFailed)?,
            smallest: built.smallest,
            largest: built.largest,
            min_sequence: built.min_sequence,
            max_sequence: built.max_sequence,
        };
        edit.next_file = Some(id.checked_add(1).ok_or(Error::SequenceExhausted)?);
        edit.added.push(meta.clone());
        manifest.append(&edit)?;
        Ok(Some(TableFile {
            meta: Arc::new(meta),
            table: open(built.bytes, Some(&path))?,
        }))
    }
}

/// The file of table `id` in `dir`.
pub(super) fn table_path(dir: &std::path::Path, id: u64) -> PathBuf {
    dir.join(format!("{id:020}.sst"))
}

/// How flushed tables of a keyspace are laid out.
fn table_options(keyspace: &KeyspaceOptions) -> Result<TableOptions> {
    let invalid = |option| Error::InvalidOptions {
        option,
        reason: "does not fit this platform's address size".to_owned(),
    };
    Ok(TableOptions {
        block_bytes: usize::try_from(keyspace.block_bytes())
            .map_err(|_| invalid("keyspace.block_bytes"))?,
        restart_interval: usize::try_from(keyspace.block_restart_interval())
            .map_err(|_| invalid("keyspace.block_restart_interval"))?,
        bloom_bits_per_key: keyspace.bloom_bits_per_key(),
        compression: Compression::Lz4,
    })
}
