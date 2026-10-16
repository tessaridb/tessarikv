//! The read view: the in-memory tables reads consult and writes land in, and the table files
//! flushed from earlier ones, published as one value.
//!
//! Each table belongs to an epoch of the sequencer, and a write puts its batch into the table of
//! the epoch its claim carries, never into "the newest table". A switch publishes a view whose
//! newest table carries the next epoch BEFORE the sequencer starts that epoch, so a claim in the
//! new epoch always finds its table; the table it replaces stays in the view until every claim of
//! its own epoch has completed. A flush replaces a frozen table with the table file built from
//! it in one new view, so every version stays in exactly one place a reader looks. Reads consult
//! the in-memory tables newest first, then the table files: level 0 newest first, then each
//! deeper level, whose files hold disjoint key ranges in key order.
//!
//! Views are immutable and swapped whole. Loading the current view never blocks: the swap cell
//! documents its loads as lock-free, and wait-free except at most once in `usize::MAX / 4` loads;
//! publishing a view is lock-free. A view, and the tables only it still names, are freed when the
//! last reader holding it lets go.

use std::sync::Arc;

use tessarikv_format::{Table, TableMeta};
use tessarikv_types::compare_internal;

use super::table_blocks::TableBlocks;
use super::tables;
use crate::memtable::Memtable;
use crate::tombstones::RangeTombstone;
use crate::{Error, Result};

/// The tables of one read view.
pub(crate) struct View {
    /// In-memory tables, newest first.
    tables: Vec<EpochTable>,
    /// Table files ordered by keyspace, then level; level 0 newest first, every other level by
    /// key.
    files: Vec<TableFile>,
}

/// A table file a view reads, what the manifest records about it, and where its data blocks
/// come from.
#[derive(Clone)]
pub(crate) struct TableFile {
    pub(crate) meta: Arc<TableMeta>,
    pub(crate) table: Table,
    pub(crate) blocks: Arc<TableBlocks>,
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

    /// A view of `table`, holding epoch 0, over `files`: the view of a database just opened.
    /// Within each keyspace and level the files keep the order given, which is newest first in
    /// level 0 and by key in every other level.
    pub(crate) fn recovered(table: Memtable, mut files: Vec<TableFile>) -> Self {
        files.sort_by_key(|file| (file.meta.keyspace, file.meta.level));
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
    /// which becomes the newest level-0 file of its keyspace; without a file, the table (which
    /// held no version) is simply dropped.
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
        let mut files = self.files.clone();
        if let Some(file) = file {
            let keyspace = file.meta.keyspace;
            let front = files.partition_point(|older| older.meta.keyspace < keyspace);
            files.insert(front, file);
        }
        Ok(Self { tables, files })
    }

    /// This view with the table files numbered in `removed` replaced by `added`, the outputs of
    /// a compaction of `keyspace`; every level keeps its order.
    pub(crate) fn after_compaction(
        &self,
        keyspace: u32,
        removed: &[u64],
        added: Vec<TableFile>,
    ) -> Self {
        let mut files: Vec<TableFile> = self
            .files
            .iter()
            .filter(|file| file.meta.keyspace != keyspace || !removed.contains(&file.meta.id))
            .cloned()
            .collect();
        files.extend(added);
        // Stable: level 0 keeps its newest-first order, every other level is put in key order.
        files.sort_by(|left, right| {
            (left.meta.keyspace, left.meta.level)
                .cmp(&(right.meta.keyspace, right.meta.level))
                .then_with(|| match left.meta.level {
                    0 => std::cmp::Ordering::Equal,
                    _ => compare_internal(&left.meta.smallest, &right.meta.smallest),
                })
        });
        Self {
            tables: self.shared_tables(),
            files,
        }
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

    /// The in-memory tables with their epochs, shared with a new view.
    fn shared_tables(&self) -> Vec<EpochTable> {
        self.tables
            .iter()
            .map(|kept| EpochTable {
                epoch: kept.epoch,
                table: Arc::clone(&kept.table),
            })
            .collect()
    }

    /// The in-memory tables, newest first.
    pub(crate) fn memtables(&self) -> impl Iterator<Item = &Memtable> {
        self.tables.iter().map(|entry| entry.table.as_ref())
    }

    /// The range deletes of every in-memory table and of the table files of `keyspace`.
    ///
    /// # Errors
    ///
    /// [`crate::Error::Corruption`] for a damaged range-delete block in a table file.
    pub(crate) fn tombstones(&self, keyspace: u32) -> Result<Vec<RangeTombstone>> {
        let mut tombstones = Vec::new();
        for table in self.memtables() {
            table.tombstones(&mut tombstones);
        }
        for file in self.files(keyspace) {
            tables::tombstones(&file.table, &mut tombstones)?;
        }
        Ok(tombstones)
    }

    /// The in-memory tables, newest first, shared: a holder keeps one alive after the view is
    /// replaced.
    pub(crate) fn shared_memtables(&self) -> impl Iterator<Item = &Arc<Memtable>> {
        self.tables.iter().map(|entry| &entry.table)
    }

    /// The table files of `keyspace`: level 0 newest first, then each deeper level by key.
    pub(crate) fn files(&self, keyspace: u32) -> impl Iterator<Item = &TableFile> {
        self.keyspace_files(keyspace).iter()
    }

    /// The table files of `keyspace` whose key range holds `key`, in the order a point read
    /// consults them: those of level 0 newest first, then at most one of each deeper level.
    ///
    /// The ranges cover point entries only, so a read may skip the others only when no range
    /// delete can hide the key.
    pub(crate) fn candidates<'a>(
        &'a self,
        keyspace: u32,
        key: &'a [u8],
    ) -> impl Iterator<Item = &'a TableFile> {
        let files = self.keyspace_files(keyspace);
        let (level_zero, sorted) =
            files.split_at(files.partition_point(|file| file.meta.level == 0));
        let deeper = sorted
            .chunk_by(|left, right| left.meta.level == right.meta.level)
            .filter_map(move |level| {
                // The first file whose greatest key is not below `key` is the only one that can
                // hold it: the ranges of a level are disjoint and in key order.
                let at = level.partition_point(|file| tables::user_key(&file.meta.largest) < key);
                level.get(at).filter(|file| holds(file, key))
            });
        level_zero
            .iter()
            .filter(move |file| holds(file, key))
            .chain(deeper)
    }

    /// The files of `keyspace`, a contiguous run of the ordered list.
    fn keyspace_files(&self, keyspace: u32) -> &[TableFile] {
        let start = self
            .files
            .partition_point(|file| file.meta.keyspace < keyspace);
        let end = self
            .files
            .partition_point(|file| file.meta.keyspace <= keyspace);
        self.files.get(start..end).unwrap_or_default()
    }

    /// The tables with their epochs, newest first.
    #[cfg(test)]
    pub(crate) fn epoch_tables(&self) -> impl Iterator<Item = (u8, &Memtable)> {
        self.tables
            .iter()
            .map(|entry| (entry.epoch, entry.table.as_ref()))
    }
}

/// Whether the key range of `file` holds `key`.
fn holds(file: &TableFile, key: &[u8]) -> bool {
    tables::user_key(&file.meta.smallest) <= key && key <= tables::user_key(&file.meta.largest)
}
