//! A compaction's blocking part: merging its input tables into output tables.
//!
//! Every input is walked from its first version, its blocks read on this thread and not kept
//! in the cache; the walks are merged in internal-key order, so each user key's versions arrive
//! newest first, and [`Retention`] decides which are written. Range deletes are carried to the
//! outputs unchanged.

use std::sync::Arc;

use tessarikv_format::{TableIter, TableMeta};
use tessarikv_types::{InternalKeyRef, ValueKind, compare_internal};

use super::output::{Destination, Outputs};
use super::retain::Retention;
use crate::db::tables::{self, format_error, user_key};
use crate::db::view::TableFile;
use crate::{Error, Result};

/// The tables a compaction reads and what decides what it writes.
pub(crate) struct Job {
    pub(crate) to: Destination,
    pub(crate) inputs: Vec<TableFile>,
    /// The tables of every level below the output level, by level, each in key order.
    pub(crate) deeper: Vec<Vec<Arc<TableMeta>>>,
    pub(crate) retention: Retention,
}

/// One input's walk.
struct Walk<'a> {
    versions: TableIter,
    file: &'a TableFile,
}

impl Job {
    /// Merges the inputs and writes and syncs the output tables and their directory. Runs on a
    /// blocking thread.
    ///
    /// # Errors
    ///
    /// [`Error::Corruption`] for a damaged input, and the storage's error when reading an input
    /// or writing an output fails.
    pub(crate) fn run(self) -> Result<Vec<TableFile>> {
        let mut tombstones = Vec::new();
        for file in &self.inputs {
            tables::tombstones(&file.table, &mut tombstones)?;
        }
        let mut outputs = Outputs::new(&self.to, &tombstones);
        let mut walks = Vec::with_capacity(self.inputs.len());
        for file in &self.inputs {
            let mut versions = file.table.iter();
            versions.seek_to_first();
            settle(file, &mut versions)?;
            walks.push(Walk { versions, file });
        }
        let mut key: Vec<u8> = Vec::new();
        let mut decisions = self.retention.key();
        let mut first = true;
        // The inputs are few (level 0 and the tables it touches), so the least is found by a scan.
        while let Some(walk) = walks
            .iter_mut()
            .filter(|walk| walk.versions.valid())
            .min_by(|left, right| compare_internal(left.versions.key(), right.versions.key()))
        {
            let stored = walk.versions.key();
            let parsed = InternalKeyRef::parse(stored).map_err(|_| corrupt())?;
            let kind = parsed.trailer().kind().map_err(|_| corrupt())?;
            let starts_key = first || parsed.user_key() != key.as_slice();
            if starts_key {
                key.clear();
                key.extend_from_slice(parsed.user_key());
                decisions = self.retention.key();
                first = false;
            }
            let sequence = parsed.trailer().sequence().get();
            let keep = decisions.keeps(sequence, kind, || deeper_holds(&self.deeper, &key));
            if keep && matches!(kind, ValueKind::Value | ValueKind::Delete) {
                outputs.add(stored, starts_key, walk.versions.value())?;
            }
            walk.versions.next();
            settle(walk.file, &mut walk.versions)?;
        }
        let written = outputs.finish()?;
        if !written.is_empty() {
            self.to.storage.sync_dir(&self.to.dir)?;
        }
        Ok(written)
    }
}

/// Loads the blocks the last move of `versions` waits for, and reports a damaged block.
fn settle(file: &TableFile, versions: &mut TableIter) -> Result<()> {
    file.blocks.complete_blocking(versions)?;
    versions
        .status()
        .map_err(|error| format_error(&error, None))
}

/// Whether a table of a level below the output may hold `key`.
fn deeper_holds(deeper: &[Vec<Arc<TableMeta>>], key: &[u8]) -> bool {
    deeper.iter().any(|level| {
        let at = level.partition_point(|table| user_key(&table.largest) < key);
        level
            .get(at)
            .is_some_and(|table| user_key(&table.smallest) <= key)
    })
}

fn corrupt() -> Error {
    Error::Corruption {
        what: "table",
        path: None,
        offset: None,
        detail: "a stored key has no trailer or a kind this build does not read".to_owned(),
    }
}
