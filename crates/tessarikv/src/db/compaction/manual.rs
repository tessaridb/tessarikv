//! Compacting a key range on request (see [`crate::Db::compact_range`]).
//!
//! A request reaches the installer through a bounded channel and waits on a reply channel, so
//! callers and the installer share no lock; the installer serves one request at a time, in order.
//!
//! Leveled, a request walks the levels from the top: all of level 0 when any of it touches the
//! range (a subset could leave older level-0 data above newer), into the base level or the first
//! non-empty level above it; then every level's tables touching the range, with the next level's
//! tables touching their span, into that next level; then the bottom-level tables touching the
//! range that were written before the request began, rewritten in place, so tombstones and
//! versions no snapshot needs leave the disk. Each step is one job, and the next step starts once
//! its job is recorded. While a step's tables are held by a running compaction, no new automatic
//! compaction starts, so the step runs as soon as those finish.
//!
//! Universal, runs are ordered by age, not by key: when any run or bottom table touches the range,
//! every run merges with the bottom into the bottom. A first-in-first-out keyspace never merges, so
//! its requests finish at once.

use std::sync::Arc;

use tessarikv_constants::LEVELS;
use tessarikv_format::TableMeta;
use tokio::sync::{mpsc, oneshot};

use super::Compactions;
use super::pick::{Busy, Pick, Shape, targets};
use crate::db::flush_signals::FlushSignals;
use crate::db::installer::Flusher;
use crate::db::tables::user_key;
use crate::{CompactionStyle, Error, Result};

#[cfg(test)]
#[path = "manual_tests.rs"]
mod tests;

/// Requests waiting for the installer at most; more callers wait to send.
pub(crate) const QUEUE: usize = 16;

/// A caller's request: the user keys `[start, end)` (unbounded where `None`), and where the
/// outcome goes.
pub(crate) struct Request {
    start: Option<Vec<u8>>,
    end: Option<Vec<u8>>,
    done: oneshot::Sender<Result<()>>,
}

/// The request being served: the level its next step reads, the job it waits for, and the first
/// file number written after it began.
pub(super) struct Active {
    request: Request,
    level: usize,
    pub(super) waiting: Option<u64>,
    fresh_from: u64,
}

/// What a request does next.
pub(super) enum Step {
    /// Run this compaction, then continue at the level given.
    Run(Pick, usize),
    /// Its tables are held by a running compaction.
    Blocked,
    /// Nothing is left in its range.
    Done,
}

/// What the installer does for range requests now.
pub(super) enum Next {
    /// Start this compaction for the request being served.
    Run(Pick),
    /// Start nothing: the request's tables are held by a running compaction.
    Blocked,
    /// No request has work to start; automatic compactions may run.
    Idle,
}

impl Compactions {
    /// The next compaction range requests ask for, finishing every request with nothing left.
    pub(super) fn manual_step(
        &mut self,
        flusher: &Flusher,
        levels: &[Vec<Arc<TableMeta>>; LEVELS],
        shape: Shape,
        busy: &Busy,
    ) -> Next {
        loop {
            if self.active.is_none() {
                let Some(request) = self.queued.pop_front() else {
                    return Next::Idle;
                };
                self.active = Some(Active::new(request, flusher.numbers.bound()));
            }
            let Some(active) = self.active.as_mut() else {
                return Next::Idle;
            };
            if active.waiting.is_some() {
                return Next::Idle;
            }
            match active.next(levels, flusher.keyspace.compaction(), shape, busy) {
                Step::Run(pick, next) => {
                    active.level = next;
                    return Next::Run(pick);
                }
                Step::Blocked => return Next::Blocked,
                Step::Done => {
                    if let Some(finished) = self.active.take() {
                        finished.finish();
                    }
                }
            }
        }
    }

    /// Notes that job `number` is in the manifest: a request waiting for it moves on.
    pub(super) fn recorded(&mut self, number: u64) {
        if let Some(active) = &mut self.active
            && active.waiting == Some(number)
        {
            active.waiting = None;
        }
    }
}

/// Sends a request for `[start, end)` and waits for its outcome.
///
/// # Errors
///
/// The installer's failure ([`Error::FlushFailed`] or [`Error::CompactionFailed`]) when it stopped
/// before finishing the request.
pub(crate) async fn compact(
    requests: &mpsc::Sender<Request>,
    signals: &FlushSignals,
    start: Option<&[u8]>,
    end: Option<&[u8]>,
) -> Result<()> {
    let (done, outcome) = oneshot::channel();
    let request = Request {
        start: start.map(<[u8]>::to_vec),
        end: end.map(<[u8]>::to_vec),
        done,
    };
    let stopped = || signals.failure().unwrap_or(Error::CompactionFailed);
    requests.send(request).await.map_err(|_| stopped())?;
    outcome.await.map_err(|_| stopped())?
}

impl Active {
    /// Starts serving `request`; files numbered `fresh_from` or above are written after it began.
    pub(super) fn new(request: Request, fresh_from: u64) -> Self {
        Self {
            request,
            level: 0,
            waiting: None,
            fresh_from,
        }
    }

    /// Reports success to the caller, who may have stopped waiting.
    pub(super) fn finish(self) {
        let _ = self.request.done.send(Ok(()));
    }

    /// The request's next step over `levels`, beside the compactions `busy` describes.
    pub(super) fn next(
        &self,
        levels: &[Vec<Arc<TableMeta>>; LEVELS],
        style: CompactionStyle,
        shape: Shape,
        busy: &Busy,
    ) -> Step {
        let Some(bottom) = LEVELS.checked_sub(1) else {
            return Step::Done;
        };
        let mut level = self.level;
        let found = match style {
            CompactionStyle::Fifo { .. } => None,
            CompactionStyle::Universal { .. } => (level == 0)
                .then(|| self.universal(levels, bottom))
                .flatten()
                .map(|pick| (pick, bottom.saturating_add(1))),
            _ => loop {
                if level > bottom {
                    break None;
                }
                let (pick, next) = self.leveled(levels, level, bottom, shape);
                if let Some(pick) = pick {
                    break Some((pick, next));
                }
                level = next;
            },
        };
        match found {
            None => Step::Done,
            Some((pick, next)) if busy.allows(&pick) => Step::Run(pick, next),
            Some(_) => Step::Blocked,
        }
    }

    /// The leveled step reading `level`, if it has work, and the level after it.
    fn leveled(
        &self,
        levels: &[Vec<Arc<TableMeta>>; LEVELS],
        level: usize,
        bottom: usize,
        shape: Shape,
    ) -> (Option<Pick>, usize) {
        let tables = |at: usize| levels.get(at).map_or(&[][..], Vec::as_slice);
        if level == 0 {
            // Level 0 is newer than every level below it, so it may land at the base level or
            // above, but never below a level that holds data.
            let first_used = (1..bottom).find(|at| !tables(*at).is_empty());
            let output = first_used
                .unwrap_or(bottom)
                .min(targets(levels, shape, bottom).0);
            let inputs = tables(0);
            let pick = inputs
                .iter()
                .any(|table| self.touches(table))
                .then(|| down(inputs.to_vec(), tables(output), output))
                .flatten();
            return (pick, output);
        }
        let next = level.saturating_add(1);
        let inputs: Vec<_> = tables(level)
            .iter()
            .filter(|table| self.touches(table))
            .cloned()
            .collect();
        if level < bottom {
            return (down(inputs, tables(next), next), next);
        }
        // The bottom: tables written since the request began already dropped what they could.
        let stale: Vec<_> = inputs
            .into_iter()
            .filter(|table| table.id < self.fresh_from)
            .collect();
        let pick = (!stale.is_empty()).then(|| Pick {
            output_level: bottom,
            inputs: stale,
            overlaps: Vec::new(),
            drop: false,
        });
        (pick, next)
    }

    /// Every run merged with the bottom into the bottom, when any of them touches the range.
    fn universal(&self, levels: &[Vec<Arc<TableMeta>>; LEVELS], bottom: usize) -> Option<Pick> {
        let runs = levels.first()?;
        let deepest = levels.get(bottom)?;
        if !runs.iter().chain(deepest).any(|table| self.touches(table)) {
            return None;
        }
        let (inputs, overlaps) = if runs.is_empty() {
            (deepest.clone(), Vec::new())
        } else {
            (runs.clone(), deepest.clone())
        };
        Some(Pick {
            output_level: bottom,
            inputs,
            overlaps,
            drop: false,
        })
    }

    /// Whether `table`'s user keys meet `[start, end)`.
    fn touches(&self, table: &TableMeta) -> bool {
        let after_start = self
            .request
            .start
            .as_deref()
            .is_none_or(|start| start <= user_key(&table.largest));
        let before_end = self
            .request
            .end
            .as_deref()
            .is_none_or(|end| user_key(&table.smallest) < end);
        after_start && before_end
    }
}

/// `inputs` into `output`, with the tables there touching their span; none without inputs.
fn down(inputs: Vec<Arc<TableMeta>>, below: &[Arc<TableMeta>], output: usize) -> Option<Pick> {
    let smallest = inputs.iter().map(|table| user_key(&table.smallest)).min()?;
    let largest = inputs.iter().map(|table| user_key(&table.largest)).max()?;
    let overlaps = below
        .iter()
        .filter(|table| {
            user_key(&table.smallest) <= largest && smallest <= user_key(&table.largest)
        })
        .cloned()
        .collect();
    Some(Pick {
        output_level: output,
        inputs,
        overlaps,
        drop: false,
    })
}
