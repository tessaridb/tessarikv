//! A compaction's blocking part: merging its input tables into output tables.
//!
//! Every input is walked from its first version, its blocks read on this thread and not kept
//! in the cache; the walks are merged in internal-key order, so each user key's versions arrive
//! newest first, and [`Retention`] decides which are written. Range deletes are carried to the
//! outputs unchanged.
//!
//! A job whose inputs start at more than one key is split at up to
//! [`crate::KeyspaceOptions::max_subcompactions`] `- 1` of those keys: each part merges its own
//! key range, with the range deletes starting there, on its own thread, and the parts' tables
//! follow one another in key order.

use std::sync::Arc;

use tessarikv_constants::LEVELS;
use tessarikv_format::{TableIter, TableMeta};
use tessarikv_types::{
    InternalKeyRef, SequenceNumber, ValueKind, compare_internal, encode_internal, seek_trailer,
};

use super::output::{Destination, Outputs};
use super::pick::Pick;
use super::retain::Retention;
use super::split;
use crate::db::flush_job::table_options;
use crate::db::installer::Flusher;
use crate::db::tables::{self, format_error, user_key};
use crate::db::view::TableFile;
use crate::tombstones::RangeTombstone;
use crate::{Error, Result};

/// The tables a compaction reads and what decides what it writes.
pub(crate) struct Job {
    pub(crate) to: Destination,
    pub(crate) inputs: Vec<TableFile>,
    /// The tables of every level below the output level, by level, each in key order.
    pub(crate) deeper: Vec<Vec<Arc<TableMeta>>>,
    pub(crate) retention: Retention,
    /// Parts the merge is split into at most.
    pub(crate) parts: usize,
}

/// One input's walk.
struct Walk<'a> {
    versions: TableIter,
    file: &'a TableFile,
}

impl Job {
    /// The job merging `pick`'s tables of `keyspace`, reading what it keeps from the snapshots
    /// held now.
    ///
    /// # Errors
    ///
    /// [`Error::CompactionFailed`] when an input is not in the current view or the output
    /// level is out of range; the keyspace's table options' error otherwise.
    pub(super) fn start(
        flusher: &Flusher,
        keyspace: u32,
        pick: &Pick,
        levels: &[Vec<Arc<TableMeta>>; LEVELS],
    ) -> Result<Self> {
        let options = flusher.keyspace_options(keyspace);
        let current = flusher.view.load();
        let wanted: Vec<u64> = pick
            .inputs
            .iter()
            .chain(&pick.overlaps)
            .map(|table| table.id)
            .collect();
        let inputs: Vec<TableFile> = current
            .files(keyspace)
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
                options: table_options(options, pick.output_level)?,
                // A merge within level 0 writes one table: one run takes the place of those merged.
                target_bytes: if pick.output_level == 0 {
                    u64::MAX
                } else {
                    options.target_file_bytes()
                },
                numbers: Arc::clone(&flusher.numbers),
                cache: Arc::clone(&flusher.cache),
                keyspace,
                level: u8::try_from(pick.output_level).map_err(|_| Error::CompactionFailed)?,
                limiter: flusher.limiter.clone(),
            },
            inputs,
            deeper: deeper(pick, levels, &wanted),
            retention: Retention::new(horizon.visible(), horizon.snapshots()),
            // Split parts write tables of one age side by side; within level 0 each would be a
            // run of its own, so a merge there is never split.
            parts: if pick.output_level == 0 {
                1
            } else {
                usize::try_from(options.max_subcompactions()).unwrap_or(1)
            },
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
        let cuts = split::boundaries(
            self.inputs
                .iter()
                .map(|file| user_key(&file.meta.smallest).to_vec()),
            self.parts,
        );
        let written = if cuts.is_empty() {
            self.merge(None, None, &tombstones)?
        } else {
            let lows = std::iter::once(None).chain(cuts.iter().map(|cut| Some(cut.as_slice())));
            let highs = cuts.iter().map(|cut| Some(cut.as_slice())).chain([None]);
            let job = &self;
            let tombstones = &tombstones;
            std::thread::scope(|scope| {
                let parts: Vec<_> = lows
                    .zip(highs)
                    .map(|(low, high)| {
                        std::thread::Builder::new()
                            .name("tessarikv-subcompaction".to_owned())
                            .spawn_scoped(scope, move || job.merge(low, high, tombstones))
                    })
                    .collect();
                let mut written = Vec::new();
                for part in parts {
                    let part = part.map_err(|_| Error::CompactionFailed)?;
                    written.extend(part.join().map_err(|_| Error::CompactionFailed)??);
                }
                Ok::<_, Error>(written)
            })?
        };
        if !written.is_empty() {
            self.to.storage.sync_dir(&self.to.dir)?;
        }
        Ok(written)
    }

    /// Merges the user keys in `[low, high)` (unbounded where `None`) with the range deletes
    /// starting there, and writes and syncs their output tables.
    fn merge(
        &self,
        low: Option<&[u8]>,
        high: Option<&[u8]>,
        tombstones: &[RangeTombstone],
    ) -> Result<Vec<TableFile>> {
        let inside =
            |key: &[u8]| low.is_none_or(|low| low <= key) && high.is_none_or(|high| key < high);
        let tombstones: Vec<RangeTombstone> = tombstones
            .iter()
            .filter(|tombstone| inside(&tombstone.start))
            .cloned()
            .collect();
        let mut outputs = Outputs::new(&self.to, &tombstones);
        let mut target = Vec::new();
        if let Some(low) = low {
            encode_internal(low, seek_trailer(SequenceNumber::MAX), &mut target);
        }
        let mut walks = Vec::with_capacity(self.inputs.len());
        for file in &self.inputs {
            let mut versions = file.table.iter();
            if target.is_empty() {
                versions.seek_to_first();
            } else {
                versions.seek(&target);
            }
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
            if !inside(parsed.user_key()) {
                break;
            }
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
        outputs.finish()
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
