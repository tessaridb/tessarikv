//! The flush side of the installer: freezing the newest epoch when a table of it is full or a
//! flush asks for it, running the flush of the oldest frozen epoch, and installing its files.

use std::collections::VecDeque;
use std::sync::Arc;

use tokio::task::JoinHandle;

use super::Db;
use super::flush_job::{Job, table_options};
use super::flush_signals::FlushSignals;
use super::installer::Flusher;
use super::view::TableFile;
use crate::memtable::Memtable;
use crate::version::manifest::ManifestStore;
use crate::{Error, Result};

/// The frozen tables of an epoch, one per keyspace, waiting for their flush.
pub(super) struct Frozen {
    epoch: u8,
    tables: Vec<(u32, Arc<Memtable>)>,
    /// The last sequence of its epoch.
    flushed: u64,
}

/// What a finished flush hands back: the manifest, and the table files or why there are none.
pub(super) type Flushed = (ManifestStore, Result<Vec<TableFile>>);

/// The running flush's result; a flush whose task or blocking thread died stops flushing.
pub(super) async fn wait(running: &mut Option<JoinHandle<Result<Flushed>>>) -> Result<Flushed> {
    match running.as_mut() {
        Some(flush) => flush.await.map_err(|_| Error::FlushFailed)?,
        None => std::future::pending().await,
    }
}

impl Flusher {
    /// Freezes the newest tables when one is full and another epoch may be frozen.
    pub(super) fn switch_if_full(&mut self, signals: &FlushSignals, frozen: &mut VecDeque<Frozen>) {
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
    pub(super) fn start_flush(
        &mut self,
        oldest: &Frozen,
        signals: &Arc<FlushSignals>,
    ) -> Result<JoinHandle<Result<Flushed>>> {
        // A keyspace dropped since the epoch froze has nothing to flush: its writes go with it.
        let tables = oldest
            .tables
            .iter()
            .filter(|(keyspace, _)| self.holds(*keyspace))
            .map(|(keyspace, table)| {
                let options = table_options(self.keyspace_options(*keyspace), 0)?;
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
    pub(super) fn install(
        &mut self,
        frozen: &mut VecDeque<Frozen>,
        flushed: Flushed,
    ) -> Result<u64> {
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

impl Db {
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

    /// Makes every batch written before the call durable, those written with
    /// [`crate::Durability::Buffered`] included: the log syncs once for all of them. Returns
    /// at once in memory, where nothing outlives the process.
    ///
    /// # Errors
    ///
    /// [`Error::LogFailed`] when the log stopped or the sync failed.
    pub async fn sync_wal(&self) -> Result<()> {
        match &self.log {
            Some(log) => log.sync().await,
            None => Ok(()),
        }
    }
}
