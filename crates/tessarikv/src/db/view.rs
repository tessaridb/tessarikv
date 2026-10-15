//! The read view: the in-memory tables reads consult and writes land in, published as one value.
//!
//! Each table belongs to an epoch of the sequencer, and a write puts its batch into the table of
//! the epoch its claim carries, never into "the newest table". A switch publishes a view whose
//! newest table carries the next epoch BEFORE the sequencer starts that epoch, so a claim in the
//! new epoch always finds its table; the table it replaces stays in the view until every claim of
//! its own epoch has completed. Reads consult the tables newest first.
//!
//! Views are immutable and swapped whole. Loading the current view never blocks: the swap cell
//! documents its loads as lock-free, and wait-free except at most once in `usize::MAX / 4` loads;
//! publishing a view is lock-free. A view, and the tables only it still names, are freed when the
//! last reader holding it lets go.

use std::sync::Arc;

use crate::memtable::Memtable;
use crate::{Error, Result};

/// The tables of one read view, newest first.
pub(crate) struct View {
    tables: Vec<EpochTable>,
}

/// A table and the epoch whose writes it holds.
struct EpochTable {
    epoch: u8,
    table: Arc<Memtable>,
}

impl View {
    /// A view of one table holding epoch `epoch`.
    pub(crate) fn new(epoch: u8, table: Memtable) -> Self {
        Self {
            tables: vec![EpochTable {
                epoch,
                table: Arc::new(table),
            }],
        }
    }

    /// The epoch of the newest table.
    #[cfg(test)]
    pub(crate) fn newest_epoch(&self) -> u8 {
        self.tables.first().map_or(0, |newest| newest.epoch)
    }

    /// This view with a new, empty table in front for the epoch after the newest one.
    #[cfg(test)]
    pub(crate) fn with_next_table(&self) -> Self {
        use tessarikv_sync::next_epoch;

        let mut tables = Vec::with_capacity(self.tables.len().saturating_add(1));
        tables.push(EpochTable {
            epoch: next_epoch(self.newest_epoch()),
            table: Arc::new(Memtable::new()),
        });
        tables.extend(self.tables.iter().map(|older| EpochTable {
            epoch: older.epoch,
            table: Arc::clone(&older.table),
        }));
        Self { tables }
    }

    /// The table holding the writes of `epoch`.
    ///
    /// # Errors
    ///
    /// [`Error::Memtable`] when no table of this view holds that epoch, which means a write
    /// outlived its table: an engine invariant is broken.
    pub(crate) fn table(&self, epoch: u8) -> Result<&Memtable> {
        self.tables
            .iter()
            .find(|candidate| candidate.epoch == epoch)
            .map(|found| found.table.as_ref())
            .ok_or(Error::Memtable {
                reason: "no in-memory table holds the epoch of a claimed batch",
            })
    }

    /// The tables, newest first.
    pub(crate) fn tables(&self) -> impl Iterator<Item = &Memtable> {
        self.tables.iter().map(|entry| entry.table.as_ref())
    }

    /// The tables with their epochs, newest first.
    #[cfg(test)]
    pub(crate) fn epoch_tables(&self) -> impl Iterator<Item = (u8, &Memtable)> {
        self.tables
            .iter()
            .map(|entry| (entry.epoch, entry.table.as_ref()))
    }
}
