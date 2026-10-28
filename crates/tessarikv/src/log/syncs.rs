//! Syncs of the log that run beside its writer task, taken back in the order they started.
//!
//! A sync covers everything appended before it starts, and the task appends in sequence order,
//! so the sync started after the frames through sequence `n` were written makes them durable.
//! Results are taken oldest first and nothing later is reported before an earlier one: the
//! writer's appends and every sync share one open file, and a failed write-back is reported to
//! only one of the syncs that look at it, so a later sync may succeed over pages an earlier one
//! found lost.

use std::collections::VecDeque;
use std::sync::Arc;

use tessarikv_constants::LOG_SYNCS_IN_FLIGHT;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use super::progress::Progress;
use super::segments::Segments;
use crate::statistics::{Statistics, Ticker};
use crate::storage::FileSyncer;
use crate::{Error, Result};

/// The running syncs, oldest first, each with the last sequence it makes durable.
#[derive(Default)]
pub(super) struct Syncs {
    running: VecDeque<(u64, JoinHandle<Result<()>>)>,
}

impl Syncs {
    /// Whether no sync is running.
    pub(super) fn is_empty(&self) -> bool {
        self.running.is_empty()
    }

    /// How many syncs are running.
    pub(super) fn len(&self) -> usize {
        self.running.len()
    }

    /// Starts a sync through `syncer`, which makes the frames through `through` durable.
    pub(super) fn start(
        &mut self,
        through: u64,
        syncer: Arc<dyn FileSyncer>,
        statistics: Option<Arc<Statistics>>,
    ) {
        let sync = tokio::task::spawn_blocking(move || {
            syncer.sync()?;
            if let Some(statistics) = statistics {
                statistics.record(Ticker::WalSyncs, 1);
            }
            Ok(())
        });
        self.running.push_back((through, sync));
    }

    /// Waits for the oldest running sync and returns the last sequence it made durable. Dropped
    /// before it returns, it leaves the sync running and first in line.
    ///
    /// # Errors
    ///
    /// The sync's error, or [`Error::LogFailed`] when its thread was lost; either stops the log.
    pub(super) async fn oldest(&mut self) -> Result<u64> {
        let (through, sync) = self.running.front_mut().ok_or(Error::LogFailed)?;
        let through = *through;
        let result = sync.await.map_err(|_| Error::LogFailed);
        self.running.pop_front();
        result?.map(|()| through)
    }
}

/// Starts a sync of everything written so far beside the log task, after taking the oldest
/// result when as many syncs as allowed are running. A writer asking for durability then does
/// not wait for a sync that started before its frame was written. Returns whether the sync was
/// made here instead, one at a time, for a storage without a [`FileSyncer`].
///
/// # Errors
///
/// The error of a sync taken here, or of the sync made here for a storage without a
/// [`FileSyncer`]; either stops the log.
pub(super) async fn sync_beside(
    slot: &mut Option<Segments>,
    progress: &watch::Sender<Progress>,
    syncs: &mut Syncs,
) -> Result<bool> {
    if syncs.len() >= LOG_SYNCS_IN_FLIGHT {
        durable(progress, syncs.oldest().await?);
    }
    let through = progress.borrow().written();
    let segments = slot.as_ref().ok_or(Error::LogFailed)?;
    if let Some(syncer) = segments.syncer() {
        syncs.start(through, syncer, segments.statistics.clone());
        return Ok(false);
    }
    // A storage that cannot sync beside the writes, or no segment open: every running sync is
    // taken first, so nothing is reported out of order, then this one is made on a blocking
    // thread.
    while !syncs.is_empty() {
        durable(progress, syncs.oldest().await?);
    }
    let mut segments = slot.take().ok_or(Error::LogFailed)?;
    let (segments, synced) = tokio::task::spawn_blocking(move || {
        let synced = segments.sync_current();
        (segments, synced)
    })
    .await
    .map_err(|_| Error::LogFailed)?;
    *slot = Some(segments);
    synced?;
    durable(progress, through);
    Ok(true)
}

/// Publishes that every frame through `through` is on stable storage.
pub(super) fn durable(progress: &watch::Sender<Progress>, through: u64) {
    progress.send_modify(|progress| progress.durable_through(through));
}
