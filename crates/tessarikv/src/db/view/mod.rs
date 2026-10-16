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

mod memtables;

use std::sync::Arc;

use tessarikv_format::{Table, TableMeta};
use tessarikv_types::compare_internal;

use super::table_blocks::TableBlocks;
use super::tables;
use crate::memtable::Memtable;

/// The tables of one read view.
pub(crate) struct View {
    /// In-memory tables by epoch, newest first.
    tables: Vec<EpochTables>,
    /// Table files ordered by keyspace, then level; level 0 newest first, every other level by
    /// key.
    files: Vec<TableFile>,
    /// The keyspaces beside the default one, by id, with their names.
    names: Names,
    /// Above every keyspace id this view has held: ids are never handed out twice.
    next_keyspace: u32,
}

/// The keyspaces a view serves beside the default one: id and name, by id.
pub(crate) type Names = Arc<[(u32, String)]>;

/// A table file a view reads, what the manifest records about it, and where its data blocks
/// come from.
#[derive(Clone)]
pub(crate) struct TableFile {
    pub(crate) meta: Arc<TableMeta>,
    pub(crate) table: Table,
    pub(crate) blocks: Arc<TableBlocks>,
}

/// The in-memory tables holding the writes of one epoch: one per keyspace, by keyspace id.
struct EpochTables {
    epoch: u8,
    keyspaces: Vec<(u32, Arc<Memtable>)>,
}

impl View {
    /// A view of one table of the default keyspace holding epoch `epoch`.
    #[cfg(test)]
    pub(crate) fn new(epoch: u8, table: Memtable) -> Self {
        Self {
            tables: vec![EpochTables {
                epoch,
                keyspaces: vec![(0, Arc::new(table))],
            }],
            files: Vec::new(),
            names: Arc::new([]),
            next_keyspace: 1,
        }
    }

    /// The view of a database just opened: `tables` holds one empty table per keyspace, the
    /// default one included, for epoch 0; `names` names the keyspaces beside the default one.
    /// Within each keyspace and level the files keep the order given, which is newest first in
    /// level 0 and by key in every other level.
    pub(crate) fn recovered(
        mut tables: Vec<(u32, Memtable)>,
        names: Names,
        mut files: Vec<TableFile>,
    ) -> Self {
        tables.sort_by_key(|(keyspace, _)| *keyspace);
        files.sort_by_key(|file| (file.meta.keyspace, file.meta.level));
        let next_keyspace = names
            .iter()
            .map(|(id, _)| id.saturating_add(1))
            .max()
            .unwrap_or(1);
        Self {
            tables: vec![EpochTables {
                epoch: 0,
                keyspaces: tables
                    .into_iter()
                    .map(|(keyspace, table)| (keyspace, Arc::new(table)))
                    .collect(),
            }],
            files,
            names,
            next_keyspace,
        }
    }

    /// The keyspaces beside the default one, by id, with their names.
    pub(crate) fn names(&self) -> &Names {
        &self.names
    }

    /// The id of the keyspace named `name`: `default` is the default keyspace.
    pub(crate) fn keyspace_named(&self, name: &str) -> Option<u32> {
        if name == "default" {
            return Some(0);
        }
        self.names
            .iter()
            .find(|(_, held)| held == name)
            .map(|(id, _)| *id)
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
        // Level 0 newest first by its last sequence, so a table merged within level 0 takes the
        // place of the runs it merged; every other level in key order.
        files.sort_by(|left, right| {
            (left.meta.keyspace, left.meta.level)
                .cmp(&(right.meta.keyspace, right.meta.level))
                .then_with(|| match left.meta.level {
                    0 => right.meta.max_sequence.cmp(&left.meta.max_sequence),
                    _ => compare_internal(&left.meta.smallest, &right.meta.smallest),
                })
        });
        Self {
            tables: self.shared_tables(),
            files,
            names: Arc::clone(&self.names),
            next_keyspace: self.next_keyspace,
        }
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

    /// The level-0 table files of `keyspace`: found by binary search, cheap enough for every
    /// write to ask.
    pub(crate) fn level0_files(&self, keyspace: u32) -> usize {
        self.keyspace_files(keyspace)
            .partition_point(|file| file.meta.level == 0)
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
}

/// Whether the key range of `file` holds `key`.
fn holds(file: &TableFile, key: &[u8]) -> bool {
    tables::user_key(&file.meta.smallest) <= key && key <= tables::user_key(&file.meta.largest)
}
