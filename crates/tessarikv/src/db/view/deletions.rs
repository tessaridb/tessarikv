//! The range deletes a view's readers apply: those of its table files cut into fragments once,
//! when the view is made, and those of its in-memory tables, which still take writes, per read.

use std::sync::Arc;

use tessarikv_types::SequenceNumber;

use super::{TableFile, View};
use crate::tombstones::{Deletions, Fragments, RangeTombstone};

/// The range deletes of one keyspace's table files.
#[derive(Debug)]
pub(super) struct FileDeletes {
    keyspace: u32,
    tombstones: Vec<RangeTombstone>,
    /// Every one of them, for a reader at or above `newest`.
    fragments: Arc<Fragments>,
    newest: SequenceNumber,
}

/// The range deletes of `files`, ordered by keyspace, for every keyspace that has any.
pub(super) fn gather(files: &[TableFile]) -> Arc<[FileDeletes]> {
    files
        .chunk_by(|left, right| left.meta.keyspace == right.meta.keyspace)
        .filter_map(|keyspace_files| {
            let tombstones: Vec<RangeTombstone> = keyspace_files
                .iter()
                .flat_map(|file| file.tombstones.iter().cloned())
                .collect();
            let newest = tombstones
                .iter()
                .map(|tombstone| tombstone.sequence)
                .max()?;
            Some(FileDeletes {
                keyspace: keyspace_files.first()?.meta.keyspace,
                fragments: Arc::new(Fragments::new(&tombstones, newest)),
                tombstones,
                newest,
            })
        })
        .collect()
}

impl View {
    /// What the range deletes of `keyspace` visible at `snapshot` hide.
    pub(crate) fn deletions(&self, keyspace: u32, snapshot: SequenceNumber) -> Deletions {
        let files = self
            .deletes
            .binary_search_by_key(&keyspace, |held| held.keyspace)
            .ok()
            .and_then(|at| self.deletes.get(at))
            .map(|held| {
                // A reader below the newest of them sees only those at or below its snapshot.
                if snapshot >= held.newest {
                    Arc::clone(&held.fragments)
                } else {
                    Arc::new(Fragments::new(&held.tombstones, snapshot))
                }
            });
        let mut recent = Vec::new();
        for table in self.memtables(keyspace) {
            if table.has_range_deletes() {
                table.tombstones(&mut recent);
            }
        }
        Deletions::new(files, Fragments::new(&recent, snapshot))
    }
}
