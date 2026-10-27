//! How a job ends — its outputs kept for the manifest, or its failure — and when compactions may
//! start again after failed ones.
//!
//! A job that fails — an output that cannot be written or synced, an input that cannot be read —
//! removes the files it created and changes nothing else: its inputs are still the ones the
//! manifest names. Writes go on; no compaction starts for [`RETRY_FIRST`], doubled after each
//! further failure up to [`RETRY_MOST`], and the first job that succeeds clears the wait. A range
//! request waiting for the job is answered with its error. A job thread that died is a defect
//! rather than a fault of the storage, and stops writes like a failed manifest edit.

use std::time::Duration;

use tokio::time::Instant;

use super::Compactions;
use crate::statistics::{Statistics, Ticker};
use crate::{Error, Result};

/// How long no compaction starts after one failed: long enough not to spin on a fault that
/// lasts, short enough that a passing one costs little.
pub(super) const RETRY_FIRST: Duration = Duration::from_millis(100);

/// The longest wait after failures in a row: a full disk freed by hand is used within it.
pub(super) const RETRY_MOST: Duration = Duration::from_secs(30);

/// Failed compactions in a row and the wait they impose: [`RETRY_FIRST`], doubled after each
/// further failure up to [`RETRY_MOST`].
#[derive(Debug, Default)]
pub(super) struct Backoff {
    failures: u32,
    until: Option<Instant>,
}

impl Backoff {
    /// When compactions may start again, while they wait.
    pub(super) fn until(&self) -> Option<Instant> {
        self.until
    }

    /// A compaction failed: the wait starts now and is longer than the last.
    pub(super) fn failed(&mut self) {
        self.failures = self.failures.saturating_add(1);
        let doublings = self.failures.saturating_sub(1).min(16);
        let wait = RETRY_FIRST
            .saturating_mul(1_u32 << doublings)
            .min(RETRY_MOST);
        let now = Instant::now();
        self.until = Some(now.checked_add(wait).unwrap_or(now));
    }

    /// A compaction succeeded: the next failure waits [`RETRY_FIRST`] again.
    pub(super) fn succeeded(&mut self) {
        self.failures = 0;
    }

    /// Whether compactions may start now; ends a wait that is over.
    pub(super) fn ready(&mut self) -> bool {
        let ready = self.until.is_none_or(|until| until <= Instant::now());
        if ready {
            self.until = None;
        }
        ready
    }
}

impl Compactions {
    /// Waits for a running job and keeps its outputs for [`Compactions::step`]; never returns
    /// while none runs. A failed job is counted in `statistics` and delays the next start.
    ///
    /// # Errors
    ///
    /// [`Error::CompactionFailed`] when the job's thread died.
    pub(in crate::db) async fn finished(&mut self, statistics: Option<&Statistics>) -> Result<()> {
        let Some(joined) = self.jobs.join_next().await else {
            return std::future::pending().await;
        };
        let (number, outputs) = joined.map_err(|_| Error::CompactionFailed)?;
        let at = self
            .running
            .iter()
            .position(|(running, _, _)| *running == number)
            .ok_or(Error::CompactionFailed)?;
        let (_, keyspace, pick) = self.running.swap_remove(at);
        match outputs {
            Ok(outputs) => {
                self.backoff.succeeded();
                self.ready.push((number, keyspace, pick, outputs));
            }
            Err(Error::CompactionFailed) => return Err(Error::CompactionFailed),
            Err(error) => self.failed(number, error, statistics),
        }
        Ok(())
    }

    /// When compactions may start again after a failed one, while they wait.
    pub(in crate::db) fn retry_at(&self) -> Option<Instant> {
        self.backoff.until()
    }

    /// Job `number` failed with `error`: counted, the next start waits, and a range request
    /// waiting for it is answered with the error.
    pub(super) fn failed(&mut self, number: u64, error: Error, statistics: Option<&Statistics>) {
        if let Some(statistics) = statistics {
            statistics.record(Ticker::CompactionFailures, 1);
        }
        self.backoff.failed();
        if let Some(active) = self.active.take_if(|active| active.waiting == Some(number)) {
            active.fail(error);
        }
    }
}
