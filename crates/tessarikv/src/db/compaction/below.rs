//! The table files outside a compaction that its decisions depend on: those below its output,
//! which may hold older versions of its keys, and every other one of the keyspace, which may
//! hold versions under its range deletes.

use std::sync::Arc;

use tessarikv_constants::LEVELS;
use tessarikv_format::TableMeta;

use super::pick::Pick;
use crate::db::tables::user_key;
use crate::db::view::TableFile;
use crate::tombstones::RangeTombstone;

/// The table files of a keyspace a compaction does not read, and their range deletes.
#[derive(Default)]
pub(crate) struct Outside {
    pub(super) tables: Vec<Arc<TableMeta>>,
    pub(super) tombstones: Vec<RangeTombstone>,
}

impl Outside {
    /// The files among `files` whose ids are not in `merged`.
    pub(super) fn of<'f>(files: impl Iterator<Item = &'f TableFile>, merged: &[u64]) -> Self {
        let mut outside = Self::default();
        for file in files.filter(|file| !merged.contains(&file.meta.id)) {
            outside.tables.push(Arc::clone(&file.meta));
            outside.tombstones.extend(file.tombstones.iter().cloned());
        }
        outside
    }
}

/// The tables below `pick`'s output that may hold older versions of its keys, each list in key
/// order: every level below the output level and, for a merge within level 0, each level-0
/// table it leaves out (older than those it merges), as a list of its own.
pub(super) fn deeper(
    pick: &Pick,
    levels: &[Vec<Arc<TableMeta>>; LEVELS],
    merged: &[u64],
) -> Vec<Vec<Arc<TableMeta>>> {
    let older_runs = levels
        .first()
        .filter(|_| pick.output_level == 0)
        .into_iter()
        .flatten()
        .filter(|table| !merged.contains(&table.id))
        .map(|table| vec![Arc::clone(table)]);
    older_runs
        .chain(
            levels
                .iter()
                .skip(pick.output_level.saturating_add(1))
                .cloned(),
        )
        .collect()
}

/// Whether a table of a level below the output may hold `key`.
pub(super) fn deeper_holds(deeper: &[Vec<Arc<TableMeta>>], key: &[u8]) -> bool {
    deeper.iter().any(|level| {
        let at = level.partition_point(|table| user_key(&table.largest) < key);
        level
            .get(at)
            .is_some_and(|table| user_key(&table.smallest) <= key)
    })
}
