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

/// One compaction: the tables of one level it takes, and those of `output_level` they touch.
#[derive(Debug, Clone)]
pub(crate) struct Pick {
    pub(crate) output_level: usize,
    pub(crate) inputs: Vec<Arc<TableMeta>>,
    pub(crate) overlaps: Vec<Arc<TableMeta>>,
}

impl Pick {
    /// Whether the compaction only moves one table down: nothing in the output level touches it.
    pub(crate) fn is_move(&self) -> bool {
        self.overlaps.is_empty() && self.inputs.len() == 1
    }
}

/// The next compaction of a keyspace whose levels hold `levels` (level 0 newest first, every
/// other level by key), or none when every score is below one.
pub(crate) fn pick(levels: &[Vec<Arc<TableMeta>>; LEVELS], shape: Shape) -> Option<Pick> {
    let bottom = LEVELS.checked_sub(1)?;
    let (base, targets) = targets(levels, shape, bottom);
    let level_zero = levels.first().map_or(0, Vec::len);
    let mut best = (
        score(
            u128::try_from(level_zero).unwrap_or(u128::MAX),
            u128::from(shape.level0_trigger),
        ),
        0,
    );
    for level in 1..bottom {
        let bytes = levels.get(level).map_or(0, |tables| bytes_of(tables));
        let target = targets.get(level).copied().unwrap_or(0);
        let level_score = match (bytes, target) {
            (0, _) => 0,
            (_, 0) => u128::MAX,
            _ => score(u128::from(bytes), u128::from(target)),
        };
        if level_score > best.0 {
            best = (level_score, level);
        }
    }
    let (best_score, level) = best;
    if best_score < SCORE_ONE {
        return None;
    }
    if level == 0 {
        let output_level = base;
        let inputs = levels.first()?.clone();
        let smallest = inputs.iter().map(|table| user_key(&table.smallest)).min()?;
        let largest = inputs.iter().map(|table| user_key(&table.largest)).max()?;
        let overlaps = touching(levels.get(output_level)?, smallest, largest);
        return Some(Pick {
            output_level,
            inputs,
            overlaps,
        });
    }
    let output_level = level.checked_add(1)?;
    let next = levels.get(output_level)?;
    // Min overlapping ratio: overlap bytes per byte of the table, compared without division.
    let input = levels.get(level)?.iter().min_by(|left, right| {
        let cost = |table: &TableMeta| {
            u128::from(bytes_of(&touching(
                next,
                user_key(&table.smallest),
                user_key(&table.largest),
            )))
        };
        let (left_cost, right_cost) = (cost(left), cost(right));
        // Each product of two u64 values fits in a u128.
        left_cost
            .saturating_mul(u128::from(right.size.max(1)))
            .cmp(&right_cost.saturating_mul(u128::from(left.size.max(1))))
    })?;
    Some(Pick {
        output_level,
        overlaps: touching(next, user_key(&input.smallest), user_key(&input.largest)),
        inputs: vec![Arc::clone(input)],
    })
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
fn bytes_of(tables: &[Arc<TableMeta>]) -> u64 {
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
