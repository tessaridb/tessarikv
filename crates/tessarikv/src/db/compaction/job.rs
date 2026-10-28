//! A compaction's blocking part: merging its input tables into output tables.
//!
//! Every input is walked from its first version, its blocks read on this thread and not kept
//! in the cache; the walks are merged in internal-key order, so each user key's versions arrive
//! newest first, and [`Retention`] decides which are written, less what a range delete hides
//! (see [`super::hidden`]).
//!
//! A job whose inputs start at more than one key is split at up to
//! [`crate::KeyspaceOptions::max_subcompactions`] `- 1` of those keys: each part merges its own
//! key range, with the range deletes starting there, on its own thread, and the parts' tables
//! follow one another in key order.

use std::sync::Arc;

use tessarikv_constants::LEVELS;
use tessarikv_format::TableMeta;
use tessarikv_types::{
    InternalKeyRef, SequenceNumber, ValueKind, compare_internal, encode_internal, seek_trailer,
};

use super::below::{Outside, deeper, deeper_holds};
use super::blob_values::BlobInputs;
use super::hidden::Hidden;
use super::output::{self, Destination, Outputs};
use super::pick::Pick;
use super::removal::Removing;
use super::retain::Retention;
use super::split;
use super::walk::{Walk, corrupt, settle};
use crate::db::flush_job::table_options;
use crate::db::installer::Flusher;
use crate::db::tables::user_key;
use crate::db::view::TableFile;
use crate::tombstones::RangeTombstone;
use crate::{Error, Result};

/// The tables a compaction reads and what decides what it writes.
pub(crate) struct Job {
    pub(crate) to: Destination,
    pub(crate) inputs: Vec<TableFile>,
    /// The tables of every level below the output level, by level, each in key order.
    pub(crate) deeper: Vec<Vec<Arc<TableMeta>>>,
    /// Every other table of the keyspace, and their range deletes: one may hold a version under
    /// an input's range delete, and theirs hide versions of the inputs too.
    pub(crate) outside: Outside,
    pub(crate) retention: Retention,
    /// Parts the merge is split into at most.
    pub(crate) parts: usize,
    pub(crate) removing: Removing,
    pub(crate) blobs: BlobInputs,
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
                blob_threshold: options.blob_threshold(),
            },
            blobs: BlobInputs::of(current.files(keyspace), &inputs, options),
            inputs,
            deeper: deeper(pick, levels, &wanted),
            outside: Outside::of(current.files(keyspace), &wanted),
            retention: Retention::new(horizon.visible(), horizon.snapshots()),
            // Split parts write tables of one age side by side; within level 0 each would be a
            // run of its own, so a merge there is never split.
            parts: if pick.output_level == 0 {
                1
            } else {
                usize::try_from(options.max_subcompactions()).unwrap_or(1)
            },
            removing: Removing::of(options),
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
        let tombstones: Vec<RangeTombstone> = self
            .inputs
            .iter()
            .flat_map(|file| file.tombstones.iter().cloned())
            .collect();
        let (hidden, tombstones) = Hidden::new(&tombstones, &self.retention, &self.outside);
        let cuts = split::boundaries(
            self.inputs
                .iter()
                .map(|file| user_key(&file.meta.smallest).to_vec()),
            self.parts,
        );
        let parts = if cuts.is_empty() {
            vec![self.merge(None, None, &tombstones, &hidden)]
        } else {
            let lows = std::iter::once(None).chain(cuts.iter().map(|cut| Some(cut.as_slice())));
            let highs = cuts.iter().map(|cut| Some(cut.as_slice())).chain([None]);
            let job = &self;
            let tombstones = &tombstones;
            let hidden = &hidden;
            std::thread::scope(|scope| {
                let parts: Vec<_> = lows
                    .zip(highs)
                    .map(|(low, high)| {
                        std::thread::Builder::new()
                            .name("tessarikv-subcompaction".to_owned())
                            .spawn_scoped(scope, move || job.merge(low, high, tombstones, hidden))
                    })
                    .collect();
                // Every part is waited for, so a failed one's siblings' tables can be removed.
                parts
                    .into_iter()
                    .map(|part| {
                        part.map_err(|_| Error::CompactionFailed)?
                            .join()
                            .map_err(|_| Error::CompactionFailed)?
                    })
                    .collect()
            })
        };
        let mut written = Vec::new();
        let mut failed = None;
        for part in parts {
            match part {
                Ok(tables) => written.extend(tables),
                Err(error) => failed = failed.or(Some(error)),
            }
        }
        if failed.is_none()
            && !written.is_empty()
            && let Err(error) = self.to.storage.sync_dir(&self.to.dir)
        {
            failed = Some(error);
        }
        if let Some(error) = failed {
            output::discard(&self.to, written, &self.blobs.files);
            return Err(error);
        }
        if let Some(statistics) = self.to.cache.statistics() {
            output::count(statistics, &self.inputs, &written);
        }
        Ok(written)
    }

    /// Merges the user keys in `[low, high)` (unbounded where `None`) with the range deletes
    /// starting there, leaving out what `hidden` hides, and writes and syncs their output tables.
    fn merge(
        &self,
        low: Option<&[u8]>,
        high: Option<&[u8]>,
        tombstones: &[RangeTombstone],
        hidden: &Hidden,
    ) -> Result<Vec<TableFile>> {
        let inside =
            |key: &[u8]| low.is_none_or(|low| low <= key) && high.is_none_or(|high| key < high);
        let tombstones: Vec<RangeTombstone> = tombstones
            .iter()
            .filter(|tombstone| inside(&tombstone.start))
            .cloned()
            .collect();
        let mut outputs = Outputs::new(&self.to, &tombstones);
        match self.fill(&mut outputs, low, high, hidden) {
            Ok(()) => outputs.finish(),
            Err(error) => {
                outputs.discard();
                Err(error)
            }
        }
    }

    /// Writes the versions of the user keys in `[low, high)` that are kept to `outputs`.
    fn fill(
        &self,
        outputs: &mut Outputs<'_>,
        low: Option<&[u8]>,
        high: Option<&[u8]>,
        hidden: &Hidden,
    ) -> Result<()> {
        let inside =
            |key: &[u8]| low.is_none_or(|low| low <= key) && high.is_none_or(|high| key < high);
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
        let mut removal = self.removing.with(&self.retention);
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
            let value = walk.versions.value();
            let version = self.blobs.version(kind, value)?;
            let (written, hides) =
                removal.kind_of(&decisions, hidden, version.kind, &parsed, &version.value)?;
            let keep =
                decisions.keeps(sequence, written, || deeper_holds(&self.deeper, &key)) && !hides;
            if keep && written != version.kind {
                outputs.add(removal.delete_key(&parsed), starts_key, &[])?;
            } else if keep && kind != ValueKind::RangeDelete {
                version.write(outputs, stored, &parsed, value, starts_key)?;
            }
            walk.versions.next();
            settle(walk.file, &mut walk.versions)?;
        }
        Ok(())
    }
}
