//! Universal compaction: every level-0 table is a sorted run, newest first, and the bottom level
//! is the oldest run.
//!
//! Once level 0 holds its trigger's worth of runs, the first rule that applies picks:
//! - space: when the runs above the bottom hold at least the amplification bound, in percent of
//!   the bottom's bytes (always, while the bottom is empty), everything merges into the bottom;
//! - size: the newest runs merge while each next one is at most the size ratio larger than those
//!   merged before it, when that takes at least `min_merge` of them;
//! - count: otherwise adjacent runs merge, enough of them to bring level 0 under its trigger, and
//!   of those the window with the fewest bytes. Always taking the newest would merge each flush
//!   into the run behind it, rewriting that growing run on every flush.
//!
//! A merge of level-0 runs writes one level-0 table, which takes the place of the runs it
//! merged: runs cover disjoint, ordered ranges of sequence numbers, so a merge of adjacent runs is
//! newer than every older run and older than every newer one, wherever its window lies.

use std::sync::Arc;

use tessarikv_constants::LEVELS;
use tessarikv_format::TableMeta;

use super::pick::{Busy, Pick, bytes_of};

/// What shapes a universal keyspace's merges.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Settings {
    pub(crate) level0_trigger: u32,
    pub(crate) size_ratio_percent: u32,
    pub(crate) min_merge: u32,
    pub(crate) max_size_amplification_percent: u32,
}

/// The next universal compaction, or none while level 0 is under its trigger or the runs it
/// would take are held by a running compaction.
pub(crate) fn pick(
    levels: &[Vec<Arc<TableMeta>>; LEVELS],
    settings: Settings,
    busy: &Busy,
) -> Option<Pick> {
    let runs = levels.first()?;
    let trigger = usize::try_from(settings.level0_trigger).unwrap_or(usize::MAX);
    if runs.is_empty() || runs.len() < trigger {
        return None;
    }
    let bottom_level = LEVELS.checked_sub(1)?;
    let bottom = levels.get(bottom_level)?;
    let above = u128::from(bytes_of(runs));
    let below = u128::from(bytes_of(bottom));
    let pick = if above.saturating_mul(100)
        >= u128::from(settings.max_size_amplification_percent).saturating_mul(below)
    {
        Pick {
            output_level: bottom_level,
            inputs: runs.clone(),
            overlaps: bottom.clone(),
            drop: false,
        }
    } else {
        let take = by_size(runs, settings.size_ratio_percent);
        let least = usize::try_from(settings.min_merge).unwrap_or(usize::MAX);
        let window = if take >= least {
            0..take
        } else {
            // Enough adjacent runs that level 0 falls under its trigger, the cheapest to rewrite.
            let width = runs
                .len()
                .saturating_sub(trigger)
                .saturating_add(2)
                .max(least)
                .min(runs.len());
            cheapest(runs, width)?
        };
        if window.len() < 2 {
            return None;
        }
        Pick {
            output_level: 0,
            inputs: runs.get(window)?.to_vec(),
            overlaps: Vec::new(),
            drop: false,
        }
    };
    Some(pick).filter(|pick| busy.allows(pick))
}

/// The `width` adjacent runs with the fewest bytes between them; the newest among equals.
fn cheapest(runs: &[Arc<TableMeta>], width: usize) -> Option<std::ops::Range<usize>> {
    let windows = runs.windows(width.max(1));
    let (start, _) = windows
        .map(bytes_of)
        .enumerate()
        .min_by_key(|(start, bytes)| (*bytes, *start))?;
    Some(start..start.saturating_add(width))
}

/// How many of the newest runs merge by size: each next one at most `ratio` percent larger
/// than those before it.
fn by_size(runs: &[Arc<TableMeta>], ratio: u32) -> usize {
    let mut merged: u128 = 0;
    let mut taken: usize = 0;
    for run in runs {
        let limit = merged.saturating_mul(u128::from(ratio).saturating_add(100));
        if taken > 0 && u128::from(run.size).saturating_mul(100) > limit {
            break;
        }
        merged = merged.saturating_add(u128::from(run.size));
        taken = taken.saturating_add(1);
    }
    taken
}
