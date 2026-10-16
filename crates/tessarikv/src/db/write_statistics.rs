//! What writes count when the database keeps statistics.

use std::time::Instant;

use tessarikv_format::WriteBatch;
use tessarikv_types::SequenceNumber;

use super::Db;
use super::commits::Check;
use super::installer::Installer;
use crate::statistics::{Histogram, Ticker};
use crate::{Result, WriteOptions};

impl Db {
    /// [`Db::write`], or a transaction's commit: the batch is committed (see `commit_uncounted`)
    /// and, when the database keeps statistics, its entries, bytes and time are counted once it
    /// is applied.
    pub(crate) async fn commit_batch(
        &self,
        batch: &WriteBatch,
        options: &WriteOptions,
        check: Option<Check<'_>>,
    ) -> Result<SequenceNumber> {
        let Some(statistics) = self.statistics.as_deref() else {
            return self.commit_uncounted(batch, options, check).await;
        };
        let started = Instant::now();
        let committed = self.commit_uncounted(batch, options, check).await;
        if committed.is_ok() {
            statistics.record(Ticker::KeysWritten, u64::from(batch.len()));
            let bytes = u64::try_from(batch.as_bytes().len()).unwrap_or(u64::MAX);
            statistics.record(Ticker::BytesWritten, bytes);
        }
        let micros = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX);
        statistics.observe(Histogram::WriteMicros, micros);
        committed
    }

    /// Waits until a write of `bytes` may go ahead (see `FlushSignals::admit`), counting the wait.
    pub(super) async fn admit(
        &self,
        installer: &Installer,
        no_slowdown: bool,
        bytes: u64,
    ) -> Result<()> {
        let started = self.statistics.as_ref().map(|_| Instant::now());
        let admitted = installer
            .signals
            .admit(&self.view, no_slowdown, bytes)
            .await;
        if let (Some(statistics), Some(started)) = (self.statistics.as_deref(), started) {
            let micros = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX);
            statistics.record(Ticker::StallMicros, micros);
        }
        admitted
    }
}
