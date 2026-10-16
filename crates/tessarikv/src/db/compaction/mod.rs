//! Compaction: merging table files into the level below so reads consult fewer of them and
//! versions no reader can see leave the disk.
//!
//! The installer runs at most one compaction at a time, next to flushes. When its manifest is
//! at hand and no compaction runs it asks [`pick`] for one: a table that moves down is recorded
//! at once; anything else becomes a job on a blocking thread that merges the inputs and writes
//! and syncs the outputs, holding no lock. The installer then appends ONE manifest edit,
//! synced, that removes the inputs and adds the outputs, and only after it publishes the view
//! in which the outputs replace the inputs. A crash before the edit leaves output files no edit
//! names, removed when the database opens; after it, opening reads the outputs.
//!
//! What a job keeps is decided at its start from the visible sequence and the snapshots then
//! held (see [`retain`]). A reader loads the view before it reads the visible sequence, so a
//! reader of the new view reads at or above the one the compaction kept versions for.
//!
//! An input's file is removed once no view names it any more: until then a reader that loaded
//! an older view may still read it. The installer checks after each install, every
//! [`SWEEP_INTERVAL`] while a file waits, and when it stops.

mod job;
mod output;
mod pick;
mod retain;

#[cfg(test)]
mod job_tests;
#[cfg(test)]
mod pick_tests;
#[cfg(test)]
mod retain_tests;

use std::path::PathBuf;
use std::sync::{Arc, Weak};

use tessarikv_constants::LEVELS;
use tessarikv_format::{ManifestEdit, TableMeta, TableRef};
use tokio::task::JoinHandle;

use self::job::Job;
use self::output::Destination;
use self::pick::{Pick, Shape};
use self::retain::Retention;
use super::flush_job::{table_options, table_path};
use super::installer::Flusher;
use super::table_blocks::TableBlocks;
use super::view::TableFile;
use crate::storage::Storage;
use crate::{Error, Result};

/// The keyspace compactions run in; there is one so far.
const KEYSPACE: u32 = 0;

/// How often the installer looks again at input files a reader still held.
pub(super) const SWEEP_INTERVAL: std::time::Duration = std::time::Duration::from_millis(100);

/// The installer's compactions: the one running, its outputs waiting for the manifest, and
/// the input files waiting for their last reader.
#[derive(Default)]
pub(super) struct Compactions {
    running: Option<(Pick, JoinHandle<Result<Vec<TableFile>>>)>,
    ready: Option<(Pick, Vec<TableFile>)>,
    obsolete: Vec<(Weak<TableBlocks>, PathBuf)>,
}

impl Compactions {
    /// Whether input files wait for their last reader.
    pub(super) fn has_obsolete(&self) -> bool {
        !self.obsolete.is_empty()
    }

    /// Whether a job is running.
    pub(super) fn is_running(&self) -> bool {
        self.running.is_some()
    }

    /// Waits for the running job and keeps its outputs for [`Compactions::step`]; never
    /// returns while none runs.
    ///
    /// # Errors
    ///
    /// The job's error, or [`Error::CompactionFailed`] when its thread died.
    pub(super) async fn finished(&mut self) -> Result<()> {
        let Some((_, handle)) = self.running.as_mut() else {
            return std::future::pending().await;
        };
        let outputs = handle.await.map_err(|_| Error::CompactionFailed)?;
        let (pick, _) = self.running.take().ok_or(Error::CompactionFailed)?;
        self.ready = Some((pick, outputs?));
        Ok(())
    }

    /// Records finished work and starts the next job, while the manifest is at hand: commits
    /// waiting outputs, records moves, removes files no view names, and starts a job when one
    /// is due.
    ///
    /// # Errors
    ///
    /// [`Error::CompactionFailed`] when the manifest edit or a job's start fails.
    pub(super) async fn step(&mut self, flusher: &mut Flusher, start: bool) -> Result<()> {
        if flusher.manifest.is_none() {
            return Ok(());
        }
        if let Some((pick, outputs)) = self.ready.take() {
            self.commit(flusher, &pick, outputs).await?;
        }
        self.sweep(Arc::clone(&flusher.storage)).await;
        while start && self.running.is_none() {
            let Some(version) = flusher.manifest.as_ref().map(|manifest| manifest.version()) else {
                return Ok(());
            };
            let levels: [Vec<Arc<TableMeta>>; LEVELS] =
                std::array::from_fn(|level| version.tables(KEYSPACE, level).to_vec());
            let keyspace = &flusher.keyspace;
            let shape = Shape {
                level0_trigger: keyspace.level0_compaction_trigger(),
                base_bytes: keyspace.level_base_bytes(),
                multiplier: keyspace.level_size_multiplier(),
            };
            let Some(pick) = pick::pick(&levels, shape) else {
                return Ok(());
            };
            if pick.is_move() {
                self.commit(flusher, &pick, Vec::new()).await?;
                continue;
            }
            let job = job(flusher, &pick, &levels)?;
            self.running = Some((pick, tokio::task::spawn_blocking(move || job.run())));
        }
        Ok(())
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

/// The job merging `pick`'s tables, reading what it keeps from the snapshots held now.
fn job(flusher: &Flusher, pick: &Pick, levels: &[Vec<Arc<TableMeta>>; LEVELS]) -> Result<Job> {
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
    // The visible sequence is read before the snapshots are, so no snapshot taken meanwhile is
    // missed.
    let horizon = flusher.snapshots.horizon(flusher.sequencer.visible());
    Ok(Job {
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
