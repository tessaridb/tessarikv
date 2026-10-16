//! The keyspaces a database serves: the default one and those named when it was opened.

use std::sync::Arc;

use tessarikv_types::KeyspaceId;

use super::Db;
use super::view::Names;
use crate::memtable::Memtable;
use crate::{DbOptions, Error, KeyspaceOptions, Result};

impl Db {
    /// Refuses a keyspace this database does not hold; reads the current view, never waits.
    /// The default keyspace is always held and is never dropped.
    pub(crate) fn check_keyspace(&self, keyspace: KeyspaceId) -> Result<()> {
        if keyspace == KeyspaceId::DEFAULT || self.view.load().holds(keyspace.get()) {
            Ok(())
        } else {
            Err(Error::UnknownKeyspace {
                keyspace: keyspace.get(),
            })
        }
    }

    /// The id of the keyspace named `name`; `default` is the default keyspace.
    #[cfg(test)]
    pub(crate) fn keyspace(&self, name: &str) -> Option<KeyspaceId> {
        self.view.load().keyspace_named(name).map(KeyspaceId::new)
    }
}

/// One empty table per keyspace, the default one first, each sized by its options, and the
/// names of the keyspaces beside the default one; `held` gives their ids and names.
pub(super) fn tables_for<'a>(
    options: &DbOptions,
    held: impl Iterator<Item = (u32, &'a str)>,
) -> (Vec<(u32, Memtable)>, Names) {
    let mut tables = vec![(
        0,
        Memtable::sized(options.default_keyspace().memtable_bytes()),
    )];
    let mut names = Vec::new();
    for (id, name) in held {
        let bytes = options
            .keyspaces()
            .iter()
            .find(|(named, _)| named == name)
            .map_or(options.default_keyspace(), |(_, keyspace)| keyspace)
            .memtable_bytes();
        tables.push((id, Memtable::sized(bytes)));
        names.push((id, name.to_owned()));
    }
    (tables, Arc::from(names))
}

/// The options of each keyspace beside the default one, by id; `held` gives their ids and
/// names.
pub(super) fn options_by_id<'a>(
    options: &DbOptions,
    held: impl Iterator<Item = (u32, &'a str)>,
) -> Vec<(u32, KeyspaceOptions)> {
    held.map(|(id, name)| {
        let named = options.keyspaces().iter().find(|(named, _)| named == name);
        let keyspace = named.map_or(options.default_keyspace(), |(_, keyspace)| keyspace);
        (id, keyspace.clone())
    })
    .collect()
}

/// The options of keyspace `id` among `keyspaces`; `default` for the default keyspace.
pub(super) fn options_of<'a>(
    keyspaces: &'a [(u32, KeyspaceOptions)],
    default: &'a KeyspaceOptions,
    id: u32,
) -> &'a KeyspaceOptions {
    keyspaces
        .iter()
        .find(|(held, _)| *held == id)
        .map_or(default, |(_, options)| options)
}
