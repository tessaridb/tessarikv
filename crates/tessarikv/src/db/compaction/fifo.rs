//! First in, first out: the oldest level-0 tables are dropped once the tables hold more than
//! their limit, and nothing is ever merged.
//!
//! Level 0 is newest first, so the drop takes tables from its end, the oldest, until what is
//! left fits; a dropped table never sits above a kept one, so a key reads its newest kept
//! value or nothing, never an older value than one that was dropped.

use std::sync::Arc;

use tessarikv_constants::LEVELS;
use tessarikv_format::TableMeta;

use super::pick::{Busy, Pick, bytes_of};

/// The oldest level-0 tables to drop so the rest hold at most `max_table_bytes`; none when they
/// already fit, or when a table to drop is held by a running compaction.
pub(crate) fn pick(
    levels: &[Vec<Arc<TableMeta>>; LEVELS],
    max_table_bytes: u64,
    busy: &Busy,
) -> Option<Pick> {
    let level_zero = levels.first()?;
    let mut kept = bytes_of(level_zero);
    let mut dropped = Vec::new();
    for table in level_zero.iter().rev() {
        if kept <= max_table_bytes {
            break;
        }
        kept = kept.saturating_sub(table.size);
        dropped.push(Arc::clone(table));
    }
    (!dropped.is_empty())
        .then_some(Pick {
            output_level: 0,
            inputs: dropped,
            overlaps: Vec::new(),
            drop: true,
        })
        .filter(|pick| busy.allows(pick))
}
