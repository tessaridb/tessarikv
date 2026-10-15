//! The read view: the in-memory tables reads consult and writes land in, and the table files
//! flushed from earlier ones, published as one value.
//!
//! Each table belongs to an epoch of the sequencer, and a write puts its batch into the table of
//! the epoch its claim carries, never into "the newest table". A switch publishes a view whose
//! newest table carries the next epoch BEFORE the sequencer starts that epoch, so a claim in the
//! new epoch always finds its table; the table it replaces stays in the view until every claim of
//! its own epoch has completed. A flush replaces a frozen table with the table file built from
//! it in one new view, so every version stays in exactly one place a reader looks. Reads consult
//! the in-memory tables newest first, then the table files newest first.
//!
//! Views are immutable and swapped whole. Loading the current view never blocks: the swap cell
//! documents its loads as lock-free, and wait-free except at most once in `usize::MAX / 4` loads;
//! publishing a view is lock-free. A view, and the tables only it still names, are freed when the
//! last reader holding it lets go.

use std::sync::Arc;

use tessarikv_format::{Table, TableMeta};

use crate::memtable::Memtable;
use crate::{Error, Result};

/// The tables of one read view.
pub(crate) struct View {
    /// In-memory tables, newest first.
    tables: Vec<EpochTable>,
    /// Table files, newest first.
    files: Vec<TableFile>,
}

/// A table file a view reads, and what the manifest records about it.
#[derive(Clone)]
pub(crate) struct TableFile {
    pub(crate) meta: Arc<TableMeta>,
    pub(crate) table: Table,
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
            files: Vec::new(),
        }
    }

    /// A view of `table`, holding epoch 0, over `files`, newest first: the view of a database
    /// just opened.
    pub(crate) fn recovered(table: Memtable, files: Vec<TableFile>) -> Self {
        Self {
            files,
            ..Self::new(0, table)
        }
    }

    /// The epoch of the newest table.
    pub(crate) fn newest_epoch(&self) -> u8 {
        self.tables.first().map_or(0, |newest| newest.epoch)
    }

    /// This view with a new, empty table in front for the epoch after the newest one.
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
        Self {
            tables,
            files: self.files.clone(),
        }
    }

    /// This view with the table of `epoch` replaced by `file`, the table file built from it,
    /// which becomes the newest file; without a file, the table (which held no version) is
    /// simply dropped.
    ///
    /// # Errors
    ///
    /// [`Error::Memtable`] when no table of this view holds that epoch.
    pub(crate) fn after_flush(&self, epoch: u8, file: Option<TableFile>) -> Result<Self> {
        if !self.tables.iter().any(|candidate| candidate.epoch == epoch) {
            return Err(Error::Memtable {
                reason: "a flushed in-memory table is not in the view it leaves",
            });
        }
        let tables = self
            .tables
            .iter()
            .filter(|candidate| candidate.epoch != epoch)
            .map(|kept| EpochTable {
                epoch: kept.epoch,
                table: Arc::clone(&kept.table),
            })
            .collect();
        let mut files = Vec::with_capacity(self.files.len().saturating_add(1));
        files.extend(file);
        files.extend(self.files.iter().cloned());
        Ok(Self { tables, files })
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

    /// The table holding the writes of `epoch`, shared, for a flush that outlives this view.
    ///
    /// # Errors
    ///
    /// As [`View::table`].
    pub(crate) fn shared_table(&self, epoch: u8) -> Result<Arc<Memtable>> {
        self.tables
            .iter()
            .find(|candidate| candidate.epoch == epoch)
            .map(|found| Arc::clone(&found.table))
            .ok_or(Error::Memtable {
                reason: "no in-memory table holds the epoch being flushed",
            })
    }

    /// The in-memory tables, newest first.
    pub(crate) fn memtables(&self) -> impl Iterator<Item = &Memtable> {
        self.tables.iter().map(|entry| entry.table.as_ref())
    }

    /// The table files of `keyspace`, newest first.
    pub(crate) fn files(&self, keyspace: u32) -> impl Iterator<Item = &TableFile> {
        self.files
            .iter()
            .filter(move |file| file.meta.keyspace == keyspace)
    }

    /// The tables with their epochs, newest first.
    #[cfg(test)]
    pub(crate) fn epoch_tables(&self) -> impl Iterator<Item = (u8, &Memtable)> {
        self.tables
            .iter()
            .map(|entry| (entry.epoch, entry.table.as_ref()))
    }
}
