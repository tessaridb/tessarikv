//! Applying a batch.

use tessarikv_constants::DIRECT_CHECKSUM_BYTES;
use tessarikv_format::{FRAME_HEADER_BYTES, WriteBatch, concat_checksum, payload_checksum};
use tessarikv_sync::{Claim, Sequencer};
use tessarikv_types::{KeyspaceId, SequenceNumber};
use tokio::sync::Notify;

use super::recover::SEQUENCE_BYTES;
use super::{Check, Db};
use crate::log::{Frame, Record, Wait};
use crate::{Durability, Error, Result, WriteOptions};

impl Db {
    /// Applies `batch` atomically: a read sees all of its entries or none. Returns the sequence
    /// number of the batch's last entry (for an empty batch, the latest visible sequence).
    ///
    /// Each entry takes the next sequence number in the order it was added, so a later entry
    /// for the same key wins. On storage the write returns once the log holds the batch as
    /// [`WriteOptions::durability`] asks, and only then becomes visible to reads; in memory every
    /// level is the same, and nothing outlives the process.
    ///
    /// Dropping the returned future before it completes leaves the outcome open: the batch is
    /// not applied in this process, but it may already be in the log and reappear when the
    /// database is reopened.
    ///
    /// # Errors
    ///
    /// [`Error::UnknownKeyspace`] when an entry names a keyspace this database does not hold
    /// (nothing is applied), [`Error::LogFailed`] when the log stopped before the batch was as
    /// durable as asked (nothing is applied, and no later write succeeds),
    /// [`Error::FlushFailed`] after a flush failed (nothing is applied),
    /// [`Error::WriteStalled`] when [`WriteOptions::no_slowdown`] is set and the write would
    /// wait for a flush (nothing is applied),
    /// [`Error::SequenceExhausted`] when the sequence space is used up, and [`Error::Memtable`]
    /// when the table cannot take an entry, which means an engine invariant is broken.
    pub async fn write(
        &self,
        batch: &WriteBatch,
        options: &WriteOptions,
    ) -> Result<SequenceNumber> {
        self.commit_batch(batch, options, None).await
    }

    /// As [`Db::write`]; with a `check`, the batch is a transaction's and is refused with
    /// [`Error::Conflict`] when a write committed after its snapshot touched one of its keys or,
    /// for a serializable one, something it read (see `commits`). A refused batch is not applied
    /// and reaches the log only as a skip.
    pub(crate) async fn commit_batch(
        &self,
        batch: &WriteBatch,
        options: &WriteOptions,
        check: Option<Check<'_>>,
    ) -> Result<SequenceNumber> {
        // The default keyspace is always held, so only a batch naming another loads the view.
        let mut view = None;
        for entry in batch {
            let keyspace = entry.keyspace;
            if keyspace == KeyspaceId::DEFAULT {
                continue;
            }
            if !view
                .get_or_insert_with(|| self.view.load())
                .holds(keyspace.get())
            {
                return Err(Error::UnknownKeyspace {
                    keyspace: keyspace.get(),
                });
            }
        }
        if batch.is_empty() {
            return self.visible_sequence();
        }
        let Some(log) = &self.log else {
            let claim = self.claim(batch)?;
            let accepted = self
                .commits
                .publish(&self.sequencer, &claim, batch, check.as_ref());
            let claim = Completion::new(&self.sequencer, None, claim);
            if !accepted {
                return claim.complete(Err(Error::Conflict));
            }
            let applied = self.apply(claim.epoch(), claim.first(), batch);
            return claim.complete(applied);
        };
        let logged = match options.durability {
            Durability::Sync => Some((prepare(batch), Wait::Durable)),
            Durability::Buffered => Some((prepare(batch), Wait::Written)),
            Durability::NoWal => None,
        };
        // A place in the log's queue is taken before the claim, so nothing between claiming
        // and handing the range over can wait or be cancelled.
        let installer = self.installer.as_ref();
        if let Some(installer) = installer {
            installer
                .signals
                .admit(
                    &self.view,
                    options.no_slowdown,
                    u64::try_from(batch.as_bytes().len()).unwrap_or(u64::MAX),
                )
                .await?;
        }
        let slot = log.reserve().await?;
        let completed = installer.map(|installer| &installer.signals.visible);
        let claim = self.claim(batch)?;
        // Published before anything awaits, so a later transaction never waits on this one for
        // longer than its synchronous publication.
        let accepted = self
            .commits
            .publish(&self.sequencer, &claim, batch, check.as_ref());
        let claim = Completion::new(&self.sequencer, completed, claim);
        let (first, last) = (claim.first(), claim.last());
        let logged = logged.filter(|_| accepted);
        let (record, built) = match logged.map(|(prepared, wait)| (prepared.finish(first), wait)) {
            Some((Ok(frame), wait)) => (Record::Frame { frame, wait }, Ok(())),
            // The range still reaches the log, as a skip, so later frames are not held back.
            Some((Err(error), _)) => (Record::Skip, Err(error)),
            None => (Record::Skip, Ok(())),
        };
        let wait = slot.send(first, last, record);
        built?;
        if !accepted {
            return claim.complete(Err(Error::Conflict));
        }
        if let Some(wait) = wait {
            log.wait(last, wait).await?;
        }
        let applied = self.apply(claim.epoch(), first, batch);
        if let Some(installer) = installer {
            installer.signals.check(&self.view.load());
        }
        claim.complete(applied)
    }

    /// Inserts `batch` from sequence `first` into the in-memory tables of `epoch`, each entry
    /// into its keyspace's. An entry of a keyspace dropped after the batch was checked, whose
    /// table this epoch no longer has, goes with the keyspace, as replaying the log would drop it.
    pub(super) fn apply(&self, epoch: u8, first: u64, batch: &WriteBatch) -> Result<()> {
        let view = self.view.load();
        let mut sequence = first;
        for entry in batch {
            let number = SequenceNumber::new(sequence).map_err(|_| Error::SequenceExhausted)?;
            let keyspace = entry.keyspace.get();
            match view.table(keyspace, epoch) {
                Ok(table) => table.insert(
                    number,
                    entry.kind,
                    entry.key,
                    entry.value.unwrap_or_default(),
                )?,
                Err(_) if !view.holds(keyspace) => {}
                Err(missing) => return Err(missing),
            }
            sequence = sequence.checked_add(1).ok_or(Error::SequenceExhausted)?;
        }
        Ok(())
    }

    /// Claims one sequence number per entry of `batch`.
    fn claim(&self, batch: &WriteBatch) -> Result<Claim> {
        self.sequencer
            .claim(u64::from(batch.len()))
            .ok_or(Error::SequenceExhausted)
    }
}

/// A log frame built before its sequence number is known: the batch is copied and checksummed
/// without holding a claim, so the claim is held only while the first sequence is filled in.
struct Prepared {
    bytes: Vec<u8>,
    batch_checksum: u32,
    batch_len: usize,
}

fn prepare(batch: &WriteBatch) -> Prepared {
    let encoded = batch.as_bytes();
    let header = FRAME_HEADER_BYTES.saturating_add(SEQUENCE_BYTES);
    let mut bytes = Vec::with_capacity(header.saturating_add(encoded.len()));
    bytes.resize(header, 0);
    bytes.extend_from_slice(encoded);
    Prepared {
        bytes,
        // Only a batch whose checksum is combined later needs it now.
        batch_checksum: if encoded.len() < DIRECT_CHECKSUM_BYTES {
            0
        } else {
            payload_checksum(encoded)
        },
        batch_len: encoded.len(),
    }
}

impl Prepared {
    /// The frame of a batch committed at sequence `first`.
    fn finish(mut self, first: u64) -> Result<Frame> {
        let sequence = first.to_le_bytes();
        self.bytes
            .get_mut(FRAME_HEADER_BYTES..FRAME_HEADER_BYTES.saturating_add(SEQUENCE_BYTES))
            .ok_or(Error::Unsupported {
                what: "a log frame without room for its sequence number",
            })?
            .copy_from_slice(&sequence);
        let checksum = if self.batch_len < DIRECT_CHECKSUM_BYTES {
            payload_checksum(self.bytes.get(FRAME_HEADER_BYTES..).unwrap_or_default())
        } else {
            concat_checksum(
                payload_checksum(&sequence),
                self.batch_checksum,
                self.batch_len,
            )
        };
        Frame::with_checksum(self.bytes, checksum)
    }
}

/// A claim that is completed when dropped, so a write abandoned while it waits on the log does
/// not hold back every later batch.
struct Completion<'a> {
    sequencer: &'a Sequencer,
    /// Woken when completing reaches the sequence a flush waits for.
    completed: Option<&'a Notify>,
    claim: Option<Claim>,
}

impl<'a> Completion<'a> {
    fn new(sequencer: &'a Sequencer, completed: Option<&'a Notify>, claim: Claim) -> Self {
        Self {
            sequencer,
            completed,
            claim: Some(claim),
        }
    }

    /// Completes the claim, once, waking a waiting flush when it reached its sequence.
    fn finish(&mut self) {
        if let Some(claim) = self.claim.take()
            && self.sequencer.complete(claim)
            && let Some(completed) = self.completed
        {
            completed.notify_one();
        }
    }

    fn epoch(&self) -> u8 {
        self.claim.as_ref().map_or(0, Claim::epoch)
    }

    fn first(&self) -> u64 {
        self.claim.as_ref().map_or(0, Claim::first)
    }

    fn last(&self) -> u64 {
        self.claim.as_ref().map_or(0, Claim::last)
    }

    /// Completes the claim whatever `applied` says, so later batches are not held back, and
    /// returns the batch's last sequence number.
    fn complete(mut self, applied: Result<()>) -> Result<SequenceNumber> {
        let last = self.last();
        self.finish();
        applied?;
        SequenceNumber::new(last).map_err(|_| Error::SequenceExhausted)
    }
}

impl Drop for Completion<'_> {
    fn drop(&mut self) {
        self.finish();
    }
}
