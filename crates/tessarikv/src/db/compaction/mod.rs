//! Compaction: merging table files into the level below so reads consult fewer of them and
//! versions no reader can see leave the disk.
//!
//! The installer runs up to [`crate::KeyspaceOptions::max_compactions`] compactions at once,
//! next to flushes. When its manifest is at hand and fewer run it asks [`pick`] for one that
//! reads no table another runs on and writes outside every range another writes in the same
//! level: a table that moves down is recorded at once; anything else becomes a job on a
//! blocking thread that merges the inputs and writes and syncs the outputs, holding no lock.
//! For each finished job the installer appends ONE manifest edit, synced, that removes the
//! inputs and adds the outputs, and only after it publishes the view in which the outputs
//! replace the inputs. A crash before the edit leaves output files no edit
//! names, removed when the database opens; after it, opening reads the outputs.
//!
//! What a job keeps is decided at its start from the visible sequence and the snapshots then
//! held (see [`retain`]). A reader loads the view before it reads the visible sequence, so a
//! reader of the new view reads at or above the one the compaction kept versions for.
//!
//! An input's file is removed once no view names it any more: until then a reader that loaded
//! an older view may still read it. The installer checks after each install, every
//! [`SWEEP_INTERVAL`] while a file waits, and when it stops.

mod fifo;
mod job;
mod output;
mod pick;
mod retain;
mod split;

#[cfg(test)]
mod fifo_tests;
#[cfg(test)]
mod job_tests;
#[cfg(test)]
mod pick_tests;
#[cfg(test)]
mod retain_tests;
#[cfg(test)]
mod running_tests;
#[cfg(test)]
mod split_tests;

use std::path::PathBuf;
use std::sync::{Arc, Weak};

use tessarikv_constants::LEVELS;
use tessarikv_format::{ManifestEdit, TableMeta, TableRef};
use tokio::task::JoinSet;

use self::job::Job;
use self::pick::{Busy, Pick, Shape};
use super::flush_job::table_path;
use super::installer::Flusher;
use super::table_blocks::TableBlocks;
use super::view::TableFile;
use crate::storage::Storage;
use crate::{CompactionStyle, KeyspaceOptions};
use crate::{Error, Result};

/// The keyspace compactions run in; there is one so far.
const KEYSPACE: u32 = 0;

/// How often the installer looks again at input files a reader still held.
pub(super) const SWEEP_INTERVAL: std::time::Duration = std::time::Duration::from_millis(100);

/// A finished job: its number and its outputs.
type Finished = (u64, Result<Vec<TableFile>>);

/// The installer's compactions: those running, outputs waiting for the manifest, and the
/// input files waiting for their last reader.
#[derive(Default)]
pub(super) struct Compactions {
    jobs: JoinSet<Finished>,
    /// What each running job took, by its number.
    running: Vec<(u64, Pick)>,
    next_job: u64,
    ready: Vec<(Pick, Vec<TableFile>)>,
    obsolete: Vec<(Weak<TableBlocks>, PathBuf)>,
}

impl Compactions {
    /// Whether input files wait for their last reader.
    pub(super) fn has_obsolete(&self) -> bool {
        !self.obsolete.is_empty()
    }

    /// Whether a job is running.
    pub(super) fn is_running(&self) -> bool {
        !self.jobs.is_empty()
    }

    /// Waits for a running job and keeps its outputs for [`Compactions::step`]; never returns
    /// while none runs.
    ///
    /// # Errors
    ///
    /// The job's error, or [`Error::CompactionFailed`] when its thread died.
    pub(super) async fn finished(&mut self) -> Result<()> {
        let Some(joined) = self.jobs.join_next().await else {
            return std::future::pending().await;
        };
        let (number, outputs) = joined.map_err(|_| Error::CompactionFailed)?;
        let at = self
            .running
            .iter()
            .position(|(running, _)| *running == number)
            .ok_or(Error::CompactionFailed)?;
        let (_, pick) = self.running.swap_remove(at);
        self.ready.push((pick, outputs?));
        Ok(())
    }

    /// Records finished work and starts the next job, while the manifest is at hand: commits
    /// waiting outputs, records moves, removes files no view names, and starts a job when one
    /// is due. Returns whether a new view was published.
    ///
    /// # Errors
    ///
    /// [`Error::CompactionFailed`] when the manifest edit or a job's start fails.
    pub(super) async fn step(&mut self, flusher: &mut Flusher, start: bool) -> Result<bool> {
        let mut published = false;
        if flusher.manifest.is_none() {
            return Ok(published);
        }
        for (pick, outputs) in std::mem::take(&mut self.ready) {
            self.commit(flusher, &pick, outputs).await?;
            published = true;
        }
        self.sweep(Arc::clone(&flusher.storage)).await;
        while start && self.has_room(&flusher.keyspace) {
            let Some((levels, shape)) = shape_of(flusher) else {
                return Ok(published);
            };
            let busy = self.busy();
            let next = match flusher.keyspace.compaction() {
                CompactionStyle::Fifo { max_table_bytes } => {
                    fifo::pick(&levels, max_table_bytes, &busy)
                }
                _ => pick::pick_with(&levels, shape, &busy),
            };
            let Some(pick) = next else {
                return Ok(published);
            };
            if pick.is_move() || pick.drop {
                self.commit(flusher, &pick, Vec::new()).await?;
                published = true;
                continue;
            }
            let job = Job::start(flusher, &pick, &levels)?;
            let number = self.next_job;
            self.next_job = number.wrapping_add(1);
            self.running.push((number, pick));
            self.jobs.spawn_blocking(move || (number, job.run()));
        }
        Ok(published)
    }

    /// Whether fewer jobs run than the keyspace allows at once.
    fn has_room(&self, keyspace: &KeyspaceOptions) -> bool {
        let most = usize::try_from(keyspace.max_compactions()).unwrap_or(usize::MAX);
        self.running.len() < most
    }

    /// The tables and ranges running jobs hold.
    fn busy(&self) -> Busy {
        let mut busy = Busy::default();
        for (_, pick) in &self.running {
            busy.tables.extend(
                pick.inputs
                    .iter()
                    .chain(&pick.overlaps)
                    .map(|table| table.id),
            );
            busy.ranges.extend(pick.busy_range());
        }
        busy
    }

    /// Appends the edit replacing `pick`'s tables by `outputs` (by the moved table when there
    /// are none), then publishes the view that reads them.
    async fn commit(
        &mut self,
        flusher: &mut Flusher,
        pick: &Pick,
        outputs: Vec<TableFile>,
    ) -> Result<()> {
        let moved = outputs.is_empty() && pick.is_move();
        let added: Vec<TableFile> = if moved {
            let current = flusher.view.load();
            let level = u8::try_from(pick.output_level).map_err(|_| Error::CompactionFailed)?;
            current
                .files(KEYSPACE)
                .filter(|file| pick.inputs.iter().any(|input| input.id == file.meta.id))
                .map(|file| TableFile {
                    meta: Arc::new(TableMeta {
                        level,
                        ..(*file.meta).clone()
                    }),
                    ..file.clone()
                })
                .collect()
        } else {
            outputs
        };
        let edit = ManifestEdit {
            next_file: Some(flusher.numbers.bound()),
            added: added.iter().map(|file| (*file.meta).clone()).collect(),
            removed: pick
                .inputs
                .iter()
                .chain(&pick.overlaps)
                .map(|table| TableRef {
                    keyspace: table.keyspace,
                    level: table.level,
                    id: table.id,
                })
                .collect(),
            ..ManifestEdit::default()
        };
        let mut manifest = flusher.manifest.take().ok_or(Error::CompactionFailed)?;
        let (manifest, appended) = tokio::task::spawn_blocking(move || {
            let appended = manifest.append(&edit);
            (manifest, appended)
        })
        .await
        .map_err(|_| Error::CompactionFailed)?;
        flusher.manifest = Some(manifest);
        appended.map_err(|_| Error::CompactionFailed)?;
        let removed: Vec<u64> = pick
            .inputs
            .iter()
            .chain(&pick.overlaps)
            .map(|table| table.id)
            .collect();
        let current = flusher.view.load_full();
        if !moved {
            self.obsolete.extend(
                current
                    .files(KEYSPACE)
                    .filter(|file| removed.contains(&file.meta.id))
                    .map(|file| {
                        (
                            Arc::downgrade(&file.blocks),
                            table_path(&flusher.dir, file.meta.id),
                        )
                    }),
            );
        }
        flusher.view.store(Arc::new(
            current.after_compaction(KEYSPACE, &removed, added),
        ));
        Ok(())
    }

    /// Removes the input files no view names any more; a removal that fails is tried again at
    /// the next sweep.
    async fn sweep(&mut self, storage: Arc<dyn Storage>) {
        let (unread, read): (Vec<_>, Vec<_>) = std::mem::take(&mut self.obsolete)
            .into_iter()
            .partition(|(blocks, _)| blocks.strong_count() == 0);
        self.obsolete = read;
        if unread.is_empty() {
            return;
        }
        let failed = tokio::task::spawn_blocking(move || {
            unread
                .into_iter()
                .filter(|(_, path)| storage.remove(path).is_err())
                .collect::<Vec<_>>()
        })
        .await
        .unwrap_or_default();
        self.obsolete.extend(failed);
    }
}

/// The levels the manifest holds and the shape the keyspace gives them; none while the
/// manifest is lent to a flush.
fn shape_of(flusher: &Flusher) -> Option<([Vec<Arc<TableMeta>>; LEVELS], Shape)> {
    let version = flusher.manifest.as_ref()?.version();
    let keyspace = &flusher.keyspace;
    Some((
        std::array::from_fn(|level| version.tables(KEYSPACE, level).to_vec()),
        Shape {
            level0_trigger: keyspace.level0_compaction_trigger(),
            base_bytes: keyspace.level_base_bytes(),
            multiplier: keyspace.level_size_multiplier(),
        },
    ))
}

/// The bytes compaction owes now (see [`pick::pending_bytes`]); none while the manifest is lent
/// to a flush. A first-in-first-out keyspace owes nothing: it never merges.
pub(super) fn pending_bytes(flusher: &Flusher) -> Option<u64> {
    if !matches!(flusher.keyspace.compaction(), CompactionStyle::Leveled) {
        return Some(0);
    }
    shape_of(flusher).map(|(levels, shape)| pick::pending_bytes(&levels, shape))
}
