//! The in-memory side of a view: one table per keyspace in every epoch, newest epoch first.

use std::sync::Arc;

use tessarikv_sync::next_epoch;

use super::super::tables;
use super::{EpochTables, TableFile, View};
use crate::memtable::Memtable;
use crate::tombstones::RangeTombstone;
use crate::{Error, Result};

impl View {
    /// The epoch of the newest tables.
    pub(crate) fn newest_epoch(&self) -> u8 {
        self.tables.first().map_or(0, |newest| newest.epoch)
    }

    /// This view with new, empty tables in front, one per keyspace, for the epoch after the
    /// newest one.
    pub(crate) fn with_next_table(&self) -> Self {
        let keyspaces = match self.tables.first() {
            Some(newest) => newest
                .keyspaces
                .iter()
                .map(|(keyspace, table)| (*keyspace, Arc::new(table.successor())))
                .collect(),
            None => vec![(
                0,
                Arc::new(Memtable::sized(tessarikv_constants::DEFAULT_MEMTABLE_BYTES)),
            )],
        };
        let mut tables = Vec::with_capacity(self.tables.len().saturating_add(1));
        tables.push(EpochTables {
            epoch: next_epoch(self.newest_epoch()),
            keyspaces,
        });
        tables.extend(self.shared_tables());
        Self {
            tables,
            files: self.files.clone(),
        }
    }

    /// This view with the tables of `epoch` replaced by `file`, the table file built from them,
    /// which becomes the newest level-0 file of its keyspace; without a file, the tables (which
    /// held no version) are simply dropped.
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
            .shared_tables()
            .into_iter()
            .filter(|kept| kept.epoch != epoch)
            .collect();
        let mut files = self.files.clone();
        if let Some(file) = file {
            let keyspace = file.meta.keyspace;
            let front = files.partition_point(|older| older.meta.keyspace < keyspace);
            files.insert(front, file);
        }
        Ok(Self { tables, files })
    }

    /// The table of `keyspace` holding the writes of `epoch`.
    ///
    /// # Errors
    ///
    /// [`Error::Memtable`] when no table of this view holds that epoch and keyspace, which means
    /// a write outlived its table: an engine invariant is broken.
    pub(crate) fn table(&self, keyspace: u32, epoch: u8) -> Result<&Memtable> {
        self.find(keyspace, epoch)
            .map(AsRef::as_ref)
            .ok_or(Error::Memtable {
                reason: "no in-memory table holds the epoch of a claimed batch",
            })
    }

    /// The table of `keyspace` holding the writes of `epoch`, shared, for a flush that outlives
    /// this view.
    ///
    /// # Errors
    ///
    /// As [`View::table`].
    pub(crate) fn shared_table(&self, keyspace: u32, epoch: u8) -> Result<Arc<Memtable>> {
        self.find(keyspace, epoch)
            .map(Arc::clone)
            .ok_or(Error::Memtable {
                reason: "no in-memory table holds the epoch being flushed",
            })
    }

    /// The in-memory tables of `keyspace`, newest first.
    pub(crate) fn memtables(&self, keyspace: u32) -> impl Iterator<Item = &Memtable> {
        self.shared_memtables(keyspace).map(AsRef::as_ref)
    }

    /// The in-memory tables of `keyspace`, newest first, shared: a holder keeps one alive after
    /// the view is replaced.
    pub(crate) fn shared_memtables(&self, keyspace: u32) -> impl Iterator<Item = &Arc<Memtable>> {
        self.tables
            .iter()
            .filter_map(move |epoch| Self::of(epoch, keyspace))
    }

    /// The tables of the newest epoch, one per keyspace.
    pub(crate) fn newest_tables(&self) -> impl Iterator<Item = &Memtable> {
        self.tables
            .first()
            .into_iter()
            .flat_map(|newest| newest.keyspaces.iter().map(|(_, table)| table.as_ref()))
    }

    /// How many epochs have in-memory tables: the newest and those frozen behind it.
    pub(crate) fn epochs(&self) -> usize {
        self.tables.len()
    }

    /// The range deletes of the in-memory tables and the table files of `keyspace`.
    ///
    /// # Errors
    ///
    /// [`crate::Error::Corruption`] for a damaged range-delete block in a table file.
    pub(crate) fn tombstones(&self, keyspace: u32) -> Result<Vec<RangeTombstone>> {
        let mut tombstones = Vec::new();
        for table in self.memtables(keyspace) {
            table.tombstones(&mut tombstones);
        }
        for file in self.files(keyspace) {
            tables::tombstones(&file.table, &mut tombstones)?;
        }
        Ok(tombstones)
    }

    /// The tables of `keyspace` with their epochs, newest first.
    #[cfg(test)]
    pub(crate) fn epoch_tables(&self, keyspace: u32) -> impl Iterator<Item = (u8, &Memtable)> {
        self.tables.iter().filter_map(move |epoch| {
            Self::of(epoch, keyspace).map(|table| (epoch.epoch, table.as_ref()))
        })
    }

    /// The in-memory tables, shared with a new view.
    pub(super) fn shared_tables(&self) -> Vec<EpochTables> {
        self.tables
            .iter()
            .map(|kept| EpochTables {
                epoch: kept.epoch,
                keyspaces: kept
                    .keyspaces
                    .iter()
                    .map(|(keyspace, table)| (*keyspace, Arc::clone(table)))
                    .collect(),
            })
            .collect()
    }

    fn find(&self, keyspace: u32, epoch: u8) -> Option<&Arc<Memtable>> {
        self.tables
            .iter()
            .find(|candidate| candidate.epoch == epoch)
            .and_then(|found| Self::of(found, keyspace))
    }

    /// The table of `keyspace` among one epoch's, found by binary search.
    fn of(epoch: &EpochTables, keyspace: u32) -> Option<&Arc<Memtable>> {
        epoch
            .keyspaces
            .binary_search_by_key(&keyspace, |(id, _)| *id)
            .ok()
            .and_then(|at| epoch.keyspaces.get(at))
            .map(|(_, table)| table)
    }
}
