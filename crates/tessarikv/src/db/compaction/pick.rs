//! Choosing the next compaction from the sizes of the levels.
//!
//! Level targets are computed from the bottom level up: the bottom level's size, divided by the
//! size multiplier once per level, until it is no larger than the base size, marks the BASE
//! level, the first level below level 0 that takes data. An empty keyspace's base level is the
//! bottom one, so the first compactions write straight to it and the levels above fill only as
//! the bottom grows. Each level from the base down to the one above the bottom has a target
//! the multiplier times the one above it; levels above the base have none.
//!
//! Every level that can move data down has a score: level 0 its table count over the trigger,
//! a level with a target its bytes over the target, and a non-empty level above the base the
//! highest score there is. The highest score of at least one wins.
//!
//! Level 0 compacts as a whole, with every table of the output level its key range touches: a
//! subset could leave an older level-0 table above newer data. Its output level is the base
//! level: every level above the base is empty when level 0 is chosen, since a non-empty one
//! outscores it. Any other level compacts one table, the one whose overlap with
//! the next level is the smallest per byte of its own, with the tables of the next level it
//! touches; a table that touches none moves down without being rewritten.

use std::sync::Arc;

use tessarikv_constants::LEVELS;
use tessarikv_format::TableMeta;

use crate::db::tables::user_key;

/// Scores are compared as integers, scaled so that one is this.
const SCORE_ONE: u128 = 1000;

/// The sizes that shape a keyspace's levels.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Shape {
    pub(crate) level0_trigger: u32,
    pub(crate) base_bytes: u64,
    pub(crate) multiplier: u32,
}

/// One compaction: the tables of one level it takes, and those of `output_level` they touch;
/// or, when `drop` is set, tables removed with nothing written in their place.
#[derive(Debug, Clone)]
pub(crate) struct Pick {
    pub(crate) output_level: usize,
    pub(crate) inputs: Vec<Arc<TableMeta>>,
    pub(crate) overlaps: Vec<Arc<TableMeta>>,
    pub(crate) drop: bool,
}

impl Pick {
    /// Whether the compaction only moves one table down: nothing in the output level touches it.
    pub(crate) fn is_move(&self) -> bool {
        !self.drop && self.overlaps.is_empty() && self.inputs.len() == 1
    }
}

/// What running compactions hold: the tables they read, and the user-key range each writes
/// into its output level.
#[derive(Debug, Default, Clone)]
pub(crate) struct Busy {
    pub(crate) tables: Vec<u64>,
    pub(crate) ranges: Vec<(usize, Vec<u8>, Vec<u8>)>,
}

/// The next compaction that can run beside those `busy` describes: levels are tried from the
/// highest score down, so a level whose work is all taken lets the next one go.
pub(crate) fn pick_with(
    levels: &[Vec<Arc<TableMeta>>; LEVELS],
    shape: Shape,
    busy: &Busy,
) -> Option<Pick> {
    let bottom = LEVELS.checked_sub(1)?;
    let (base, targets) = targets(levels, shape, bottom);
    let level_zero = levels.first().map_or(0, Vec::len);
    let mut scored = vec![(
        score(
            u128::try_from(level_zero).unwrap_or(u128::MAX),
            u128::from(shape.level0_trigger),
        ),
        0,
    )];
    for level in 1..bottom {
        let bytes = levels.get(level).map_or(0, |tables| bytes_of(tables));
        let target = targets.get(level).copied().unwrap_or(0);
        let level_score = match (bytes, target) {
            (0, _) => 0,
            (_, 0) => u128::MAX,
            _ => score(u128::from(bytes), u128::from(target)),
        };
        scored.push((level_score, level));
    }
    // Highest score first; among equal scores the upper level.
    scored.sort_by(|left, right| right.0.cmp(&left.0).then(left.1.cmp(&right.1)));
    scored
        .into_iter()
        .take_while(|(level_score, _)| *level_score >= SCORE_ONE)
        .find_map(|(_, level)| match level {
            0 => level_zero_pick(levels, base, busy),
            _ => level_pick(levels, level, busy),
        })
}

/// The bytes compaction still owes before every level is within bounds: level 0 at its trigger
/// owes its own bytes and those of the base-level tables it touches; a level over its target
/// owes its excess, and the next level's bytes merged with it, `multiplier` times as many.
pub(crate) fn pending_bytes(levels: &[Vec<Arc<TableMeta>>; LEVELS], shape: Shape) -> u64 {
    let Some(bottom) = LEVELS.checked_sub(1) else {
        return 0;
    };
    let (base, targets) = targets(levels, shape, bottom);
    let mut owed: u64 = 0;
    if let Some(level_zero) = levels.first().filter(|tables| {
        !tables.is_empty()
            && u64::try_from(tables.len()).unwrap_or(u64::MAX) >= u64::from(shape.level0_trigger)
    }) {
        owed = owed.saturating_add(bytes_of(level_zero));
        let smallest = level_zero
            .iter()
            .map(|table| user_key(&table.smallest))
            .min();
        let largest = level_zero
            .iter()
            .map(|table| user_key(&table.largest))
            .max();
        if let (Some(smallest), Some(largest), Some(base_tables)) =
            (smallest, largest, levels.get(base))
        {
            owed = owed.saturating_add(bytes_of(&touching(base_tables, smallest, largest)));
        }
    }
    let rewritten = u64::from(shape.multiplier.max(2)).saturating_add(1);
    for level in base..bottom {
        let bytes = levels.get(level).map_or(0, |tables| bytes_of(tables));
        let target = targets.get(level).copied().unwrap_or(0);
        if target > 0 && bytes > target {
            owed = owed.saturating_add(bytes.saturating_sub(target).saturating_mul(rewritten));
        }
    }
    owed
}

/// The next compaction of a keyspace whose levels hold `levels` (level 0 newest first, every
/// other level by key) when none runs, or none when every score is below one.
#[cfg(test)]
pub(crate) fn pick(levels: &[Vec<Arc<TableMeta>>; LEVELS], shape: Shape) -> Option<Pick> {
    pick_with(levels, shape, &Busy::default())
}

/// All of level 0 into `base`, unless a table it needs is busy or its range is being written.
fn level_zero_pick(
    levels: &[Vec<Arc<TableMeta>>; LEVELS],
    base: usize,
    busy: &Busy,
) -> Option<Pick> {
    let inputs = levels.first()?.clone();
    let smallest = inputs.iter().map(|table| user_key(&table.smallest)).min()?;
    let largest = inputs.iter().map(|table| user_key(&table.largest)).max()?;
    let overlaps = touching(levels.get(base)?, smallest, largest);
    Some(Pick {
        output_level: base,
        inputs,
        overlaps,
        drop: false,
    })
    .filter(|pick| busy.allows(pick))
}

/// One table of `level` into the next: the least overlap per byte among those that can run.
fn level_pick(levels: &[Vec<Arc<TableMeta>>; LEVELS], level: usize, busy: &Busy) -> Option<Pick> {
    let output_level = level.checked_add(1)?;
    let next = levels.get(output_level)?;
    let mut candidates: Vec<(u128, Pick)> = levels
        .get(level)?
        .iter()
        .map(|table| {
            let overlaps = touching(next, user_key(&table.smallest), user_key(&table.largest));
            (
                u128::from(bytes_of(&overlaps)),
                Pick {
                    output_level,
                    inputs: vec![Arc::clone(table)],
                    overlaps,
                    drop: false,
                },
            )
        })
        .collect();
    // Min overlapping ratio: overlap bytes per byte of the table, compared without division;
    // each product of two u64 values fits in a u128. Stable, so ties keep key order.
    candidates.sort_by(|(left_cost, left), (right_cost, right)| {
        let size =
            |pick: &Pick| u128::from(pick.inputs.first().map_or(1, |table| table.size.max(1)));
        left_cost
            .saturating_mul(size(right))
            .cmp(&right_cost.saturating_mul(size(left)))
    });
    candidates
        .into_iter()
        .map(|(_, pick)| pick)
        .find(|pick| busy.allows(pick))
}

impl Pick {
    /// The user-key range the compaction reads and writes.
    fn range(&self) -> Option<(&[u8], &[u8])> {
        let tables = || self.inputs.iter().chain(&self.overlaps);
        let smallest = tables().map(|table| user_key(&table.smallest)).min()?;
        let largest = tables().map(|table| user_key(&table.largest)).max()?;
        Some((smallest, largest))
    }

    /// The range written into the output level, as [`Busy::ranges`] records it.
    pub(crate) fn busy_range(&self) -> Option<(usize, Vec<u8>, Vec<u8>)> {
        self.range()
            .map(|(smallest, largest)| (self.output_level, smallest.to_vec(), largest.to_vec()))
    }
}

impl Busy {
    /// Whether `pick` reads no busy table and writes outside every range another compaction
    /// writes in the same level.
    pub(super) fn allows(&self, pick: &Pick) -> bool {
        let free = pick
            .inputs
            .iter()
            .chain(&pick.overlaps)
            .all(|table| !self.tables.contains(&table.id));
        free && pick.range().is_some_and(|(smallest, largest)| {
            self.ranges.iter().all(|(level, low, high)| {
                *level != pick.output_level
                    || high.as_slice() < smallest
                    || largest < low.as_slice()
            })
        })
    }
}

/// The base level and every level's target bytes; levels above the base, and the bottom one,
/// have target zero.
fn targets(
    levels: &[Vec<Arc<TableMeta>>; LEVELS],
    shape: Shape,
    bottom: usize,
) -> (usize, [u64; LEVELS]) {
    let multiplier = u64::from(shape.multiplier.max(2));
    let mut size = levels
        .get(bottom)
        .map_or(0, |tables| bytes_of(tables))
        .max(shape.base_bytes);
    let mut base = bottom;
    while size > shape.base_bytes
        && let Some(above) = base.checked_sub(1).filter(|above| *above >= 1)
    {
        size = size.checked_div(multiplier).unwrap_or(0);
        base = above;
    }
    let mut targets = [0; LEVELS];
    let mut target = size.max(1);
    for level in base..bottom {
        if let Some(slot) = targets.get_mut(level) {
            *slot = target;
        }
        target = target.saturating_mul(multiplier);
    }
    (base, targets)
}

/// `numerator / denominator` scaled so one is [`SCORE_ONE`].
fn score(numerator: u128, denominator: u128) -> u128 {
    numerator
        .saturating_mul(SCORE_ONE)
        .checked_div(denominator)
        .unwrap_or(u128::MAX)
}

/// The bytes of `tables`.
pub(super) fn bytes_of(tables: &[Arc<TableMeta>]) -> u64 {
    tables
        .iter()
        .fold(0, |sum, table| sum.saturating_add(table.size))
}

/// The tables of a sorted level that touch `[smallest, largest]`, in key order.
fn touching(level: &[Arc<TableMeta>], smallest: &[u8], largest: &[u8]) -> Vec<Arc<TableMeta>> {
    level
        .iter()
        .filter(|table| touches(table, smallest, largest))
        .cloned()
        .collect()
}

/// Whether `table`'s key range touches `[smallest, largest]` (user keys).
fn touches(table: &TableMeta, smallest: &[u8], largest: &[u8]) -> bool {
    user_key(&table.smallest) <= largest && smallest <= user_key(&table.largest)
}
