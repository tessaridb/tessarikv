//! Versions: the set of table files a read consults, as built from manifest edits.
//!
//! A version is immutable. Applying an edit makes a new version and leaves the old one as it
//! was, so a reader holding an old version keeps a consistent set of tables while flushes and
//! compactions install newer ones. An edit that contradicts the version it is applied to (a
//! removed table that is not there, a table added twice, overlapping tables in a sorted level,
//! counters going back) is refused as corruption: the manifest that produced it cannot be trusted.
//!
//! Level 0 holds flushed tables newest first, and their key ranges may overlap. Every other level
//! holds tables sorted by key with ranges that never overlap.
//!
//! The default keyspace, id 0, always exists; every other keyspace is added by an edit naming its
//! id and name and dropped by one that removes its last tables with it. Ids are handed out in
//! order and never reused, names are unique, and a table belongs to a keyspace the version holds.

pub(crate) mod manifest;

#[cfg(test)]
mod keyspace_tests;
#[cfg(test)]
mod manifest_tests;
#[cfg(test)]
mod tests;

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::sync::Arc;

use tessarikv_constants::LEVELS;
use tessarikv_format::{KeyspaceRecord, ManifestEdit, TableMeta};
use tessarikv_types::compare_internal;

use crate::{Error, Result};

/// See the module documentation.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct Version {
    /// Per keyspace, the tables of each level.
    keyspaces: BTreeMap<u32, Levels>,
    flushed_sequence: u64,
    next_file: u64,
    /// The keyspaces added and not dropped, by id; the default keyspace is not listed.
    names: BTreeMap<u32, String>,
    next_keyspace: u32,
}

/// The tables of one keyspace, per level.
type Levels = [Vec<Arc<TableMeta>>; LEVELS];

impl Version {
    /// A version with no tables.
    pub(crate) fn empty() -> Self {
        Self::default()
    }

    /// Every write at or below this sequence is in a table file.
    pub(crate) fn flushed_sequence(&self) -> u64 {
        self.flushed_sequence
    }

    /// The next file number to hand out.
    pub(crate) fn next_file(&self) -> u64 {
        self.next_file
    }

    /// The keyspaces added and not dropped, by id, with their names; not the default keyspace.
    pub(crate) fn keyspaces(&self) -> impl Iterator<Item = (u32, &str)> {
        self.names.iter().map(|(id, name)| (*id, name.as_str()))
    }

    /// The next keyspace id to hand out.
    pub(crate) fn next_keyspace(&self) -> u32 {
        self.next_keyspace
    }

    /// The tables of `level` in `keyspace`: newest first in level 0, by key elsewhere.
    pub(crate) fn tables(&self, keyspace: u32, level: usize) -> &[Arc<TableMeta>] {
        self.keyspaces
            .get(&keyspace)
            .and_then(|levels| levels.get(level))
            .map_or(&[], Vec::as_slice)
    }

    /// One edit that rebuilds this version from an empty one.
    pub(crate) fn as_edit(&self) -> ManifestEdit {
        ManifestEdit {
            flushed_sequence: Some(self.flushed_sequence),
            next_file: Some(self.next_file),
            added: self
                .keyspaces
                .values()
                .flat_map(|levels| levels.iter())
                .flatten()
                .map(|table| TableMeta::clone(table))
                .collect(),
            removed: Vec::new(),
            added_keyspaces: self
                .names
                .iter()
                .map(|(id, name)| KeyspaceRecord {
                    id: *id,
                    name: name.clone(),
                })
                .collect(),
            dropped_keyspaces: Vec::new(),
            next_keyspace: Some(self.next_keyspace),
        }
    }

    /// The version `edit` makes of this one.
    ///
    /// # Errors
    ///
    /// [`Error::Corruption`] when the edit contradicts this version.
    pub(crate) fn apply(&self, edit: &ManifestEdit) -> Result<Self> {
        let mut next = self.clone();
        if let Some(sequence) = edit.flushed_sequence {
            if sequence < next.flushed_sequence {
                return Err(corrupt("the flushed sequence goes back"));
            }
            next.flushed_sequence = sequence;
        }
        if let Some(file) = edit.next_file {
            if file < next.next_file {
                return Err(corrupt("the next file number goes back"));
            }
            next.next_file = file;
        }
        if let Some(id) = edit.next_keyspace {
            if id < next.next_keyspace {
                return Err(corrupt("the next keyspace id goes back"));
            }
            next.next_keyspace = id;
        }
        for keyspace in &edit.added_keyspaces {
            next.add_keyspace(keyspace)?;
        }
        for removed in &edit.removed {
            let tables = next
                .keyspaces
                .get_mut(&removed.keyspace)
                .and_then(|levels| levels.get_mut(usize::from(removed.level)))
                .ok_or_else(|| corrupt("a removed table is not in the version"))?;
            let position = tables
                .iter()
                .position(|table| table.id == removed.id)
                .ok_or_else(|| corrupt("a removed table is not in the version"))?;
            tables.remove(position);
        }
        for added in &edit.added {
            next.add(added)?;
        }
        for id in &edit.dropped_keyspaces {
            next.drop_keyspace(*id)?;
        }
        Ok(next)
    }

    /// Adds a keyspace, checking its id is fresh and its name free.
    fn add_keyspace(&mut self, keyspace: &KeyspaceRecord) -> Result<()> {
        if keyspace.id == 0 || keyspace.id >= self.next_keyspace {
            return Err(corrupt(
                "a keyspace id is the default's or not handed out yet",
            ));
        }
        if self.names.contains_key(&keyspace.id) {
            return Err(corrupt("a keyspace is added twice"));
        }
        if self.names.values().any(|name| *name == keyspace.name) {
            return Err(corrupt("a keyspace name is taken"));
        }
        self.names.insert(keyspace.id, keyspace.name.clone());
        Ok(())
    }

    /// Drops a keyspace whose tables the same edit removed.
    fn drop_keyspace(&mut self, id: u32) -> Result<()> {
        if id == 0 || self.names.remove(&id).is_none() {
            return Err(corrupt("a dropped keyspace is not one the version holds"));
        }
        let holds_tables = self
            .keyspaces
            .remove(&id)
            .is_some_and(|levels| levels.iter().any(|tables| !tables.is_empty()));
        if holds_tables {
            return Err(corrupt("a dropped keyspace keeps tables"));
        }
        Ok(())
    }

    /// Adds `table` to its level, keeping the level's order and checking its invariants.
    fn add(&mut self, table: &TableMeta) -> Result<()> {
        if table.id >= self.next_file {
            return Err(corrupt(
                "a table is numbered at or past the next file number",
            ));
        }
        if self.contains(table.id) {
            return Err(corrupt("a table is added twice"));
        }
        if table.keyspace != 0 && !self.names.contains_key(&table.keyspace) {
            return Err(corrupt("a table's keyspace is not in the version"));
        }
        let level = usize::from(table.level);
        let tables = self
            .keyspaces
            .entry(table.keyspace)
            .or_default()
            .get_mut(level)
            .ok_or_else(|| corrupt("a table's level is out of range"))?;
        let table = Arc::new(table.clone());
        if level == 0 {
            // Newest first: a flush adds a table holding later sequences than every other.
            let position = tables
                .iter()
                .position(|older| older.max_sequence < table.max_sequence)
                .unwrap_or(tables.len());
            tables.insert(position, table);
            return Ok(());
        }
        let position = tables
            .partition_point(|earlier| compare_internal(&earlier.largest, &table.smallest).is_lt());
        let overlaps_next = tables.get(position).is_some_and(|later| {
            compare_internal(&later.smallest, &table.largest) != Ordering::Greater
        });
        if overlaps_next {
            return Err(corrupt("tables overlap in a sorted level"));
        }
        tables.insert(position, table);
        Ok(())
    }

    /// Whether any keyspace holds a table numbered `id`.
    fn contains(&self, id: u64) -> bool {
        self.keyspaces
            .values()
            .flat_map(|levels| levels.iter())
            .flatten()
            .any(|table| table.id == id)
    }
}

fn corrupt(detail: &str) -> Error {
    Error::Corruption {
        what: "manifest",
        path: None,
        offset: None,
        detail: detail.to_owned(),
    }
}
