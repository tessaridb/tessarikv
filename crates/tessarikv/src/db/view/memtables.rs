//! The in-memory side of a view: one table per keyspace in every epoch, newest epoch first.

use std::sync::Arc;

use tessarikv_sync::next_epoch;

use super::{EpochTables, TableFile, View};
use crate::memtable::Memtable;
use crate::{Error, Result};

impl View {
    /// The epoch of the newest tables.
    pub(crate) fn newest_epoch(&self) -> u8 {
        self.tables.first().map_or(0, |newest| newest.epoch)
    }

    /// This view with new, empty tables in front, one per keyspace it holds, for the epoch after
    /// the newest one.
    pub(crate) fn with_next_table(&self) -> Self {
        let keyspaces = match self.tables.first() {
            Some(newest) => newest
                .keyspaces
                .iter()
                .filter(|(keyspace, _)| self.holds(*keyspace))
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
            deletes: Arc::clone(&self.deletes),
            names: Arc::clone(&self.names),
            next_keyspace: self.next_keyspace,
        }
    }

    /// This view with keyspace `id`, named `name`, beside the others, writing into `table` from
    /// the newest epoch on: a writer that finds the keyspace in it claims in that epoch or a
    /// later one, whose tables succeed this one.
    ///
    /// # Errors
    ///
    /// [`Error::Memtable`] when the view has no epoch to add the table to.
    pub(crate) fn with_keyspace(&self, id: u32, name: &str, table: Memtable) -> Result<Self> {
        let mut tables = self.shared_tables();
        let newest = tables.first_mut().ok_or(Error::Memtable {
            reason: "a view without in-memory tables takes no keyspace",
        })?;
        let at = newest.keyspaces.partition_point(|(held, _)| *held < id);
        newest.keyspaces.insert(at, (id, Arc::new(table)));
        let mut names = self.names.to_vec();
        let at = names.partition_point(|(held, _)| *held < id);
        names.insert(at, (id, name.to_owned()));
        Ok(Self {
            tables,
            files: self.files.clone(),
            deletes: Arc::clone(&self.deletes),
            names: names.into(),
            next_keyspace: self.next_keyspace.max(id.saturating_add(1)),
        })
    }

    /// This view without keyspace `id`, and the table files it held: its name and files go,
    /// while its in-memory tables stay in their epochs, so a write that found it before it was
    /// dropped still finds its table; new tables leave it out (see [`View::with_next_table`]).
    pub(crate) fn without_keyspace(&self, id: u32) -> (Self, Vec<TableFile>) {
        let (removed, files): (Vec<TableFile>, Vec<TableFile>) = self
            .files
            .iter()
            .cloned()
            .partition(|file| file.meta.keyspace == id);
        let names: Vec<(u32, String)> = self
            .names
            .iter()
            .filter(|(held, _)| *held != id)
            .cloned()
            .collect();
        let view = Self {
            tables: self.shared_tables(),
            deletes: super::deletions::gather(&files),
            files,
            names: names.into(),
            next_keyspace: self.next_keyspace,
        };
        (view, removed)
    }

    /// The id the next keyspace takes in a database without a manifest.
    pub(crate) fn next_keyspace(&self) -> u32 {
        self.next_keyspace
    }

    /// This view with the tables of `epoch` replaced by `files`, the table files built from
    /// them, each the newest level-0 file of its keyspace; tables without a file held no
    /// version and are simply dropped.
    ///
    /// # Errors
    ///
    /// [`Error::Memtable`] when no table of this view holds that epoch.
    pub(crate) fn after_flush(&self, epoch: u8, flushed: Vec<TableFile>) -> Result<Self> {
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
        for file in flushed {
            let keyspace = file.meta.keyspace;
            let front = files.partition_point(|older| older.meta.keyspace < keyspace);
            files.insert(front, file);
        }
        Ok(Self {
            tables,
            deletes: super::deletions::gather(&files),
            files,
            names: Arc::clone(&self.names),
            next_keyspace: self.next_keyspace,
        })
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

    /// The tables holding the writes of `epoch`, one per keyspace, shared, for a flush that
    /// outlives this view.
    ///
    /// # Errors
    ///
    /// [`Error::Memtable`] when no table of this view holds that epoch.
    pub(crate) fn shared_epoch(&self, epoch: u8) -> Result<Vec<(u32, Arc<Memtable>)>> {
        self.tables
            .iter()
            .find(|candidate| candidate.epoch == epoch)
            .map(|found| {
                found
                    .keyspaces
                    .iter()
                    .map(|(keyspace, table)| (*keyspace, Arc::clone(table)))
                    .collect()
            })
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

    /// Whether the view serves `keyspace`: the default one, or one it names. A dropped
    /// keyspace's in-memory tables may stay until their epochs are flushed; it is not served.
    pub(crate) fn holds(&self, keyspace: u32) -> bool {
        keyspace == 0
            || self
                .names
                .binary_search_by_key(&keyspace, |(id, _)| *id)
                .is_ok()
    }

    /// How many epochs have in-memory tables: the newest and those frozen behind it.
    pub(crate) fn epochs(&self) -> usize {
        self.tables.len()
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
