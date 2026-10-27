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
use crate::log::LogWriter;
use crate::memtable::Memtable;
use crate::version::manifest::ManifestStore;
use crate::{Error, FlushCompleted, Result};

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
    /// releases the log it covers; returns what it installed.
    pub(super) fn install(
        &mut self,
        frozen: &mut VecDeque<Frozen>,
        flushed: Flushed,
    ) -> Result<FlushCompleted> {
        let (manifest, written) = flushed;
        self.manifest = Some(manifest);
        let files = written?;
        let completed = FlushCompleted {
            tables: u32::try_from(files.len()).unwrap_or(u32::MAX),
            bytes: files
                .iter()
                .fold(0, |bytes: u64, file| bytes.saturating_add(file.meta.size)),
            flushed_sequence: 0,
        };
        let oldest = frozen.pop_front().ok_or(Error::FlushFailed)?;
        let current = self.view.load_full();
        self.view
            .store(Arc::new(current.after_flush(oldest.epoch, files)?));
        self.log.release_through(oldest.flushed);
        Ok(FlushCompleted {
            flushed_sequence: oldest.flushed,
            ..completed
        })
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

    /// The failure that stopped this database's background work, if one did: a failed log
    /// write or sync ([`Error::LogFailed`]), else a failed flush or a failed manifest edit of a
    /// compaction ([`Error::FlushFailed`], [`Error::CompactionFailed`]). A compaction that fails
    /// before its edit changes nothing and runs again later, so it is not reported here, only
    /// counted ([`crate::Ticker::CompactionFailures`]). Writes are refused from then on
    /// until the database is reopened; this reads the state without attempting one. `None`
    /// while healthy, and always in memory.
    pub fn background_error(&self) -> Option<Error> {
        if self.log.as_ref().is_some_and(LogWriter::failed) {
            return Some(Error::LogFailed);
        }
        self.installer
            .as_ref()
            .and_then(|installer| installer.signals.failure())
    }
}

#[cfg(test)]
impl Db {
    /// Starts a new in-memory table: later claims carry the next epoch and land in it, while
    /// the table it replaces stays readable. Returns `None` when another switch is running.
    ///
    /// The view naming the new table is published BEFORE the sequencer starts its epoch, so a
    /// claim carrying that epoch always finds it (see [`tessarikv_sync::Sequencer::switch`]).
    pub(crate) fn switch_memtable(&self) -> Option<tessarikv_sync::Switch> {
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
}
