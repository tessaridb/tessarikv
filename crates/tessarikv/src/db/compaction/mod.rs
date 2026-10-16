//! Compaction: merging table files into the level below so reads consult fewer of them and
//! versions no reader can see leave the disk.
//!
//! Every keyspace compacts on its own, by its own options. The installer runs up to
//! [`crate::KeyspaceOptions::max_compactions`] compactions of a keyspace at once, next to flushes.
//! When its manifest is at hand and fewer run it asks [`pick`] for one that reads no table another
//! runs on and writes outside every range another of its keyspace writes in the same level: a
//! table that moves down is recorded at once; anything else becomes a job on a
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

mod commit;
mod fifo;
mod job;
pub(super) mod manual;
mod output;
mod pick;
mod retain;
mod split;
mod universal;

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
#[cfg(test)]
mod universal_tests;

use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::{Arc, Weak};

use tessarikv_constants::LEVELS;
use tessarikv_format::TableMeta;
use tokio::task::JoinSet;

use self::job::Job;
use self::manual::{Active, Request};
use self::pick::{Busy, Pick, Shape};
use super::installer::Flusher;
use super::table_blocks::TableBlocks;
use super::view::TableFile;
use crate::storage::Storage;
use crate::{CompactionStyle, KeyspaceOptions};
use crate::{Error, Result};

/// How often the installer looks again at input files a reader still held.
pub(super) const SWEEP_INTERVAL: std::time::Duration = std::time::Duration::from_millis(100);

/// A finished job: its number and its outputs.
type Finished = (u64, Result<Vec<TableFile>>);

/// The installer's compactions: those running, outputs waiting for the manifest, and the
/// input files waiting for their last reader.
#[derive(Default)]
pub(super) struct Compactions {
    jobs: JoinSet<Finished>,
    /// What each running job took, by its number, with its keyspace.
    running: Vec<(u64, u32, Pick)>,
    next_job: u64,
    /// Finished jobs: number, keyspace, what each took and its outputs, waiting for the
    /// manifest.
    ready: Vec<(u64, u32, Pick, Vec<TableFile>)>,
    obsolete: Vec<(Weak<TableBlocks>, PathBuf)>,
    /// Range requests waiting, and the one being served.
    queued: VecDeque<Request>,
    active: Option<Active>,
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
            .position(|(running, _, _)| *running == number)
            .ok_or(Error::CompactionFailed)?;
        let (_, keyspace, pick) = self.running.swap_remove(at);
        self.ready.push((number, keyspace, pick, outputs?));
        Ok(())
    }

    /// Queues a range request; [`Compactions::step`] serves it.
    pub(super) fn request(&mut self, request: Request) {
        self.queued.push_back(request);
    }

    /// Records finished work and starts the next jobs, while the manifest is at hand: commits
    /// waiting outputs, records moves, removes files no view names, and starts the jobs due in
    /// every keyspace. Returns whether a new view was published.
    ///
    /// # Errors
    ///
    /// [`Error::CompactionFailed`] when the manifest edit or a job's start fails.
    pub(super) async fn step(&mut self, flusher: &mut Flusher, start: bool) -> Result<bool> {
        let mut published = false;
        if flusher.manifest.is_none() {
            return Ok(published);
        }
        for (number, keyspace, pick, outputs) in std::mem::take(&mut self.ready) {
            self.commit(flusher, keyspace, &pick, outputs).await?;
            self.recorded(number);
            published = true;
        }
        self.sweep(Arc::clone(&flusher.storage)).await;
        // Keyspace 0, then the named ones in id order: index 0 is the default keyspace.
        let mut index = 0_usize;
        while start
            && let Some(keyspace) = index.checked_sub(1).map_or(Some(0), |named| {
                flusher.keyspaces.get(named).map(|(id, _)| *id)
            })
        {
            published |= self.start_jobs(flusher, keyspace).await?;
            index = index.saturating_add(1);
        }
        Ok(published)
    }

    /// Starts the jobs due in `keyspace` while it has room; returns whether a new view was
    /// published.
    async fn start_jobs(&mut self, flusher: &mut Flusher, keyspace: u32) -> Result<bool> {
        let mut published = false;
        while self.has_room(keyspace, flusher.keyspace_options(keyspace)) {
            let Some((levels, shape)) = shape_of(flusher, keyspace) else {
                break;
            };
            let busy = self.busy(keyspace);
            // A range request goes first; while its tables are busy, nothing new starts here.
            let (next, requested) = match self.manual_step(flusher, keyspace, &levels, shape, &busy)
            {
                manual::Next::Run(pick) => (Some(pick), true),
                manual::Next::Blocked => break,
                manual::Next::Idle => (automatic(flusher, keyspace, &levels, shape, &busy), false),
            };
            let Some(pick) = next else {
                break;
            };
            if pick.is_move() || pick.drop {
                self.commit(flusher, keyspace, &pick, Vec::new()).await?;
                published = true;
                continue;
            }
            let job = Job::start(flusher, keyspace, &pick, &levels)?;
            let number = self.next_job;
            self.next_job = number.wrapping_add(1);
            if requested && let Some(active) = &mut self.active {
                active.waiting = Some(number);
            }
            self.running.push((number, keyspace, pick));
            self.jobs.spawn_blocking(move || (number, job.run()));
        }
        Ok(published)
    }

    /// Whether fewer jobs of `keyspace` run than its `options` allow at once.
    fn has_room(&self, keyspace: u32, options: &KeyspaceOptions) -> bool {
        let most = usize::try_from(options.max_compactions()).unwrap_or(usize::MAX);
        let running = self
            .running
            .iter()
            .filter(|(_, running, _)| *running == keyspace)
            .count();
        running < most
    }

    /// The tables and ranges the running jobs of `keyspace` hold; other keyspaces' never meet
    /// its own.
    fn busy(&self, keyspace: u32) -> Busy {
        let mut busy = Busy::default();
        for (_, _, pick) in self.running.iter().filter(|(_, held, _)| *held == keyspace) {
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

/// The next automatic compaction for `keyspace`'s style.
fn automatic(
    flusher: &Flusher,
    keyspace: u32,
    levels: &[Vec<Arc<TableMeta>>; LEVELS],
    shape: Shape,
    busy: &Busy,
) -> Option<Pick> {
    match flusher.keyspace_options(keyspace).compaction() {
        CompactionStyle::Fifo { max_table_bytes } => fifo::pick(levels, max_table_bytes, busy),
        CompactionStyle::Universal {
            size_ratio_percent,
            min_merge,
            max_size_amplification_percent,
        } => universal::pick(
            levels,
            universal::Settings {
                level0_trigger: shape.level0_trigger,
                size_ratio_percent,
                min_merge,
                max_size_amplification_percent,
            },
            busy,
        ),
        _ => pick::pick_with(levels, shape, busy),
    }
}

/// The levels of `keyspace` the manifest holds and the shape its options give them; none
/// while the manifest is lent to a flush.
fn shape_of(flusher: &Flusher, keyspace: u32) -> Option<([Vec<Arc<TableMeta>>; LEVELS], Shape)> {
    let version = flusher.manifest.as_ref()?.version();
    let options = flusher.keyspace_options(keyspace);
    Some((
        std::array::from_fn(|level| version.tables(keyspace, level).to_vec()),
        Shape {
            level0_trigger: options.level0_compaction_trigger(),
            base_bytes: options.level_base_bytes(),
            multiplier: options.level_size_multiplier(),
        },
    ))
}

/// The bytes compaction owes in the default keyspace now (see [`pick::pending_bytes`]); none
/// while the manifest is lent to a flush. A first-in-first-out keyspace owes nothing: it never
/// merges.
pub(super) fn pending_bytes(flusher: &Flusher) -> Option<u64> {
    if !matches!(flusher.keyspace.compaction(), CompactionStyle::Leveled) {
        return Some(0);
    }
    shape_of(flusher, 0).map(|(levels, shape)| pick::pending_bytes(&levels, shape))
}
