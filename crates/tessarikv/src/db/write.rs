//! Applying a batch.

use tessarikv_format::WriteBatch;
use tessarikv_types::SequenceNumber;

use super::Db;
use crate::{Error, Result, WriteOptions};

impl Db {
    /// Applies `batch` atomically: a read sees all of its entries or none. Returns the sequence
    /// number of the batch's last entry (for an empty batch, the latest visible sequence).
    ///
    /// Each entry takes the next sequence number in the order it was added, so a later entry
    /// for the same key wins. In memory every durability level is the same: nothing outlives the
    /// process.
    ///
    /// # Errors
    ///
    /// [`Error::UnknownKeyspace`] when an entry names a keyspace this database does not hold
    /// (nothing is applied), [`Error::SequenceExhausted`] when the sequence space is used up, and
    /// [`Error::Memtable`] when the table cannot take an entry, which means an engine invariant is
    /// broken.
    pub async fn write(
        &self,
        batch: &WriteBatch,
        _options: &WriteOptions,
    ) -> Result<SequenceNumber> {
        for entry in batch {
            Self::check_keyspace(entry.keyspace)?;
        }
        if batch.is_empty() {
            return self.snapshot();
        }
        let claim = self
            .sequencer
            .claim(u64::from(batch.len()))
            .ok_or(Error::SequenceExhausted)?;
        let mut applied = Ok(());
        for (sequence, entry) in (claim.first()..=claim.last()).zip(batch) {
            applied = SequenceNumber::new(sequence)
                .map_err(|_| Error::SequenceExhausted)
                .and_then(|sequence| {
                    self.table.insert(
                        sequence,
                        entry.kind,
                        entry.key,
                        entry.value.unwrap_or_default(),
                    )
                });
            if applied.is_err() {
                break;
            }
        }
        let last = claim.last();
        // Completed even after a failed insert, so later batches are not held back forever.
        self.sequencer.complete(claim);
        applied?;
        SequenceNumber::new(last).map_err(|_| Error::SequenceExhausted)
    }
}
