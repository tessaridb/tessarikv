//! The levels of a keyspace, the shape its options give them, and what compaction owes it.

use std::sync::Arc;

use tessarikv_constants::LEVELS;
use tessarikv_format::TableMeta;

use super::pick::{self, Shape};
use crate::db::installer::Flusher;
use crate::db::view::View;
use crate::{CompactionStyle, KeyspaceOptions};

/// The levels of `keyspace` the manifest holds and the shape its options give them; none
/// while the manifest is lent to a flush.
pub(super) fn shape_of(
    flusher: &Flusher,
    keyspace: u32,
) -> Option<([Vec<Arc<TableMeta>>; LEVELS], Shape)> {
    let version = flusher.manifest.as_ref()?.version();
    Some((
        std::array::from_fn(|level| version.tables(keyspace, level).to_vec()),
        shape(flusher.keyspace_options(keyspace)),
    ))
}

/// The shape `options` give a keyspace's levels.
fn shape(options: &KeyspaceOptions) -> Shape {
    Shape {
        level0_trigger: options.level0_compaction_trigger(),
        base_bytes: options.level_base_bytes(),
        multiplier: options.level_size_multiplier(),
    }
}

/// The bytes compaction owes `keyspace` of `view`, shaped by its `options` (see
/// [`pick::pending_bytes`]). A keyspace that does not compact by levels owes nothing here.
pub(crate) fn owed(view: &View, keyspace: u32, options: &KeyspaceOptions) -> u64 {
    if !matches!(options.compaction(), CompactionStyle::Leveled) {
        return 0;
    }
    let levels = std::array::from_fn(|level| {
        view.files(keyspace)
            .filter(|file| usize::from(file.meta.level) == level)
            .map(|file| Arc::clone(&file.meta))
            .collect()
    });
    pick::pending_bytes(&levels, shape(options))
}
