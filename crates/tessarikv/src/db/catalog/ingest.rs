//! Adding tables built outside the database.
//!
//! An ingest takes one new sequence number S for all its entries. The installer freezes the
//! newest in-memory tables and claims S in the same step that starts the next epoch, so every
//! earlier write has a lower sequence and sits in the frozen tables or below, and every later one
//! a higher sequence in the new tables. The ingest then waits in the flush queue behind the
//! frozen epoch: once that is on table files, its entries are written as level-0 tables at S
//! (synced, then ONE manifest edit naming them, `flushed_sequence = S`), the view naming them is
//! published on top of level 0, and only then is S completed, so no reader sees S before the
//! tables do. Writes made meanwhile are logged and applied, and return once S is completed.

use std::collections::VecDeque;
use std::sync::Arc;

use tessarikv_sync::{Claim, Sequencer};
use tessarikv_types::{KeyspaceId, SequenceNumber};
use tokio::sync::{Notify, oneshot};
use tokio::task::JoinHandle;

use super::Command;
use crate::db::Db;
use crate::db::flush_job::{Job, table_options};
use crate::db::flush_signals::FlushSignals;
use crate::db::flushes::{Flushed, Frozen};
use crate::db::installer::Flusher;
use crate::ingest::{Entry, decode};
use crate::log::OwnedSlot;
use crate::memtable::Memtable;
use crate::{Error, Result};

/// A request to ingest decoded tables into `keyspace`, and where its sequence goes.
pub(crate) struct Ingest {
    keyspace: u32,
    tables: Vec<Vec<Entry>>,
    slot: OwnedSlot,
    visibility: Arc<Notify>,
    done: oneshot::Sender<Result<SequenceNumber>>,
}

/// An ingest in the flush queue: its tables until their job takes them, and its sequence,
/// completed when this is dropped, so a failed ingest never holds later writes back.
pub(crate) struct Ingesting {
    keyspace: u32,
    tables: Option<Vec<Vec<Entry>>>,
    sequencer: Arc<Sequencer>,
    signals: Arc<FlushSignals>,
    visibility: Arc<Notify>,
    claim: Option<Claim>,
    done: Option<oneshot::Sender<Result<SequenceNumber>>>,
}

impl Ingesting {
    /// The tables are in the published view: completes the sequence and answers the caller.
    pub(crate) fn installed(mut self) {
        let sequence = self.claim.as_ref().map_or(0, Claim::last);
        self.complete();
        if let Some(done) = self.done.take() {
            // The caller may have stopped waiting; the ingest holds either way.
            let _ = done.send(SequenceNumber::new(sequence).map_err(|_| Error::SequenceExhausted));
        }
    }

    fn complete(&mut self) {
        let Some(claim) = self.claim.take() else {
            return;
        };
        if self.sequencer.complete(claim) {
            self.signals.visible.notify_one();
        }
        if self.sequencer.has_waiters() {
            self.visibility.notify_waiters();
        }
    }
}

impl Drop for Ingesting {
    fn drop(&mut self) {
        self.complete();
    }
}

impl Flusher {
    /// Freezes the newest tables, claims the ingest's sequence as the next epoch starts, and
    /// queues the ingest behind them.
    pub(in crate::db) fn ingest(
        &mut self,
        ingest: Ingest,
        signals: &Arc<FlushSignals>,
        frozen: &mut VecDeque<Frozen>,
    ) {
        let Ingest {
            keyspace,
            tables,
            slot,
            visibility,
            done,
        } = ingest;
        if !self.holds(keyspace) {
            let _ = done.send(Err(Error::UnknownKeyspace { keyspace }));
            return;
        }
        let current = self.view.load_full();
        let epoch = current.newest_epoch();
        let shared = match current.shared_epoch(epoch) {
            Ok(shared) => shared,
            Err(error) => {
                let _ = done.send(Err(error));
                return;
            }
        };
        // The view naming the next table is published before its epoch starts.
        self.view.store(Arc::new(current.with_next_table()));
        let Some((switch, claim)) = self.sequencer.switch_claiming() else {
            let _ = done.send(Err(Error::SequenceExhausted));
            return;
        };
        // Announced and handed to the log at once: transactions and later log frames wait for
        // neither.
        self.sequencer.publish(&claim);
        let sequence = switch.boundary();
        slot.skip(sequence);
        self.switched = sequence;
        frozen.push_back(Frozen::epoch(epoch, shared, sequence.saturating_sub(1)));
        let ingesting = Ingesting {
            keyspace,
            tables: Some(tables),
            sequencer: Arc::clone(&self.sequencer),
            signals: Arc::clone(signals),
            visibility,
            claim: Some(claim),
            done: Some(done),
        };
        frozen.push_back(Frozen::ingest(sequence, ingesting));
    }

    /// Writes the ingest's tables at `sequence` and the manifest edit naming them.
    pub(in crate::db) fn start_ingest(
        &mut self,
        ingesting: &mut Ingesting,
        sequence: u64,
    ) -> Result<JoinHandle<Result<Flushed>>> {
        let keyspace = ingesting.keyspace;
        let tables = ingesting.tables.take().ok_or(Error::FlushFailed)?;
        let keyspace_options = self.keyspace_options(keyspace);
        let options = table_options(keyspace_options, 0)?;
        let threshold = keyspace_options.blob_threshold();
        let mut manifest = self.manifest.take().ok_or(Error::FlushFailed)?;
        let mut job = Job {
            storage: Arc::clone(&self.storage),
            dir: self.dir.clone(),
            tables: Vec::new(),
            flushed: sequence,
            numbers: Arc::clone(&self.numbers),
            cache: Arc::clone(&self.cache),
        };
        Ok(tokio::spawn(async move {
            tokio::task::spawn_blocking(move || {
                let written = in_memory(&tables, sequence).and_then(|memtables| {
                    job.tables = memtables
                        .into_iter()
                        .map(|table| (keyspace, options, threshold, table))
                        .collect();
                    job.run(&mut manifest)
                });
                (manifest, written)
            })
            .await
            .map_err(|_| Error::FlushFailed)
        }))
    }
}

/// Each table's entries at `sequence`, in an in-memory table of its own.
fn in_memory(tables: &[Vec<Entry>], sequence: u64) -> Result<Vec<Arc<Memtable>>> {
    let sequence = SequenceNumber::new(sequence).map_err(|_| Error::SequenceExhausted)?;
    tables
        .iter()
        .map(|entries| {
            let bytes = entries.iter().fold(0_u64, |bytes, (key, _, value)| {
                let entry = key.len().saturating_add(value.len());
                bytes.saturating_add(u64::try_from(entry).unwrap_or(u64::MAX))
            });
            let table = Memtable::sized(bytes.max(1));
            for (key, kind, value) in entries {
                table.insert(sequence, *kind, key, value)?;
            }
            Ok(Arc::new(table))
        })
        .collect()
}

impl Db {
    /// Adds the entries of `tables`, each written by [`crate::TableWriter`], to `keyspace` at
    /// one new sequence number, which it returns: newer than every write that returned before
    /// the call, older than every write that starts after it returns, and all of them or none
    /// of them after a crash. The tables are checked whole first, then written again under the
    /// keyspace's options as level-0 table files; each is held in memory meanwhile, so split a
    /// large load into several tables. The in-memory tables are flushed first, and writes made
    /// while an ingest runs return once it is done.
    ///
    /// A transaction that began before the ingest and writes one of its keys commits as if the
    /// ingest had not happened.
    ///
    /// # Errors
    ///
    /// [`Error::InvalidArgument`] when no table is named, a table does not decode, holds anything
    /// but puts and deletes [`crate::TableWriter`] wrote, or two tables overlap;
    /// [`Error::UnknownKeyspace`]; [`Error::ReadOnly`] on a database opened read-only;
    /// [`Error::Unsupported`] in memory; [`Error::FlushFailed`] or [`Error::LogFailed`] when
    /// the background work or the log stopped.
    pub async fn ingest(
        &self,
        keyspace: KeyspaceId,
        tables: Vec<Vec<u8>>,
    ) -> Result<SequenceNumber> {
        self.refuse_read_only("an ingest")?;
        let (Some(installer), Some(log)) = (&self.installer, &self.log) else {
            return Err(Error::Unsupported {
                what: "an ingest into a database in memory",
            });
        };
        let refused = |reason| Error::InvalidArgument {
            what: "an ingest",
            reason,
        };
        if tables.is_empty() {
            return Err(refused("an ingest names no table"));
        }
        self.check_keyspace(keyspace)?;
        let mut decoded = tokio::task::spawn_blocking(move || {
            tables.into_iter().map(decode).collect::<Result<Vec<_>>>()
        })
        .await
        .map_err(|_| Error::ReadFailed)??;
        decoded.sort_by(|left, right| first_key(left).cmp(first_key(right)));
        let overlap = decoded.windows(2).any(|pair| match pair {
            [earlier, later] => last_key(earlier) >= first_key(later),
            _ => false,
        });
        if overlap {
            return Err(refused("ingested tables overlap"));
        }
        let slot = log.reserve_owned().await?;
        let (done, ingested) = oneshot::channel();
        let stopped = || installer.signals.failure().unwrap_or(Error::FlushFailed);
        installer
            .requests
            .send(Command::Ingest(Ingest {
                keyspace: keyspace.get(),
                tables: decoded,
                slot,
                visibility: Arc::clone(&self.visibility),
                done,
            }))
            .await
            .map_err(|_| stopped())?;
        let sequence = ingested.await.map_err(|_| stopped())??;
        self.readable(Ok(sequence)).await
    }
}

fn first_key(entries: &[Entry]) -> &[u8] {
    entries.first().map_or(&[], |(key, _, _)| key.as_slice())
}

fn last_key(entries: &[Entry]) -> &[u8] {
    entries.last().map_or(&[], |(key, _, _)| key.as_slice())
}

#[cfg(test)]
mod tests;
