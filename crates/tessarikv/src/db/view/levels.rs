//! The files and bytes of each level of each keyspace, counted once when a view is built so that
//! reading them costs no walk of the files.

use std::sync::Arc;

use tessarikv_constants::LEVELS;

use super::{TableFile, View};

/// The files and bytes in each level of one keyspace.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct LevelFigures {
    pub(crate) files: [u32; LEVELS],
    pub(crate) bytes: [u64; LEVELS],
}

/// The figures of every keyspace holding files, by keyspace id; `files` is ordered by keyspace.
pub(super) fn gather(files: &[TableFile]) -> Arc<[(u32, LevelFigures)]> {
    let mut gathered: Vec<(u32, LevelFigures)> = Vec::new();
    for file in files {
        let keyspace = file.meta.keyspace;
        if gathered.last().is_none_or(|(id, _)| *id != keyspace) {
            gathered.push((keyspace, LevelFigures::default()));
        }
        let level = usize::from(file.meta.level);
        if let Some((_, figures)) = gathered.last_mut()
            && let (Some(count), Some(bytes)) =
                (figures.files.get_mut(level), figures.bytes.get_mut(level))
        {
            *count = count.saturating_add(1);
            *bytes = bytes.saturating_add(file.meta.size);
        }
    }
    gathered.into()
}

impl View {
    /// The files and bytes in each level of `keyspace`; zeros for one holding no files.
    pub(crate) fn level_figures(&self, keyspace: u32) -> LevelFigures {
        self.levels
            .binary_search_by_key(&keyspace, |(id, _)| *id)
            .ok()
            .and_then(|at| self.levels.get(at))
            .map_or_else(LevelFigures::default, |(_, figures)| *figures)
    }
}
