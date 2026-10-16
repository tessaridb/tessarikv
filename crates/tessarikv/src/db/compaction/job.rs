//! A compaction's blocking part: merging its input tables into output tables.
//!
//! Every input is walked from its first version, its blocks read on this thread and not kept
//! in the cache; the walks are merged in internal-key order, so each user key's versions arrive
//! newest first, and [`Retention`] decides which are written. Range deletes are carried to the
//! outputs unchanged.

use std::sync::Arc;

use tessarikv_constants::LEVELS;
use tessarikv_format::{TableIter, TableMeta};
use tessarikv_types::{InternalKeyRef, ValueKind, compare_internal};

use super::KEYSPACE;
use super::output::{Destination, Outputs};
use super::pick::Pick;
use super::retain::Retention;
use crate::db::flush_job::table_options;
use crate::db::installer::Flusher;
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
    /// The job merging `pick`'s tables, reading what it keeps from the snapshots held now.
    ///
    /// # Errors
    ///
    /// [`Error::CompactionFailed`] when an input is not in the current view or the output
    /// level is out of range; the keyspace's table options' error otherwise.
    pub(super) fn start(
        flusher: &Flusher,
        pick: &Pick,
        levels: &[Vec<Arc<TableMeta>>; LEVELS],
    ) -> Result<Self> {
        let current = flusher.view.load();
        let wanted: Vec<u64> = pick
            .inputs
            .iter()
            .chain(&pick.overlaps)
            .map(|table| table.id)
            .collect();
        let inputs: Vec<TableFile> = current
            .files(KEYSPACE)
            .filter(|file| wanted.contains(&file.meta.id))
            .cloned()
            .collect();
        if inputs.len() != wanted.len() {
            return Err(Error::CompactionFailed);
        }
        // The visible sequence is read before the snapshots are, so no snapshot taken meanwhile
        // is missed.
        let horizon = flusher.snapshots.horizon(flusher.sequencer.visible());
        Ok(Self {
            to: Destination {
                storage: Arc::clone(&flusher.storage),
                dir: flusher.dir.clone(),
                options: table_options(&flusher.keyspace)?,
                target_bytes: flusher.keyspace.target_file_bytes(),
                numbers: Arc::clone(&flusher.numbers),
                cache: Arc::clone(&flusher.cache),
                keyspace: KEYSPACE,
                level: u8::try_from(pick.output_level).map_err(|_| Error::CompactionFailed)?,
            },
            inputs,
            deeper: levels
                .iter()
                .skip(pick.output_level.saturating_add(1))
                .cloned()
                .collect(),
            retention: Retention::new(horizon.visible(), horizon.snapshots()),
        })
    }

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
