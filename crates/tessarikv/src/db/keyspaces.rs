//! The keyspaces a database serves: the default one, those named when it was opened, and those
//! created since.

use std::sync::Arc;

use tessarikv_types::KeyspaceId;

use tokio::sync::oneshot;

use super::Db;
use super::catalog::{Command, Create, DropKeyspace, check_new};
use super::installer::Flusher;
use super::view::Names;
use crate::memtable::Memtable;
use crate::options::DEFAULT_NAME;
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
    #[must_use]
    pub fn keyspace(&self, name: &str) -> Option<KeyspaceId> {
        self.view.load().keyspace_named(name).map(KeyspaceId::new)
    }

    /// Every keyspace this database holds, the default one first, then by id, with its name.
    #[must_use]
    pub fn keyspaces(&self) -> Vec<(KeyspaceId, String)> {
        let view = self.view.load();
        std::iter::once((KeyspaceId::DEFAULT, DEFAULT_NAME.to_owned()))
            .chain(
                view.names()
                    .iter()
                    .map(|(id, name)| (KeyspaceId::new(*id), name.clone())),
            )
            .collect()
    }

    /// Creates the keyspace `name`, an independent set of keys with its own `options`, and
    /// returns its id, which no other keyspace of this database ever takes. Writes and reads
    /// may name it as soon as this returns, and a batch may span it and other keyspaces.
    ///
    /// On storage the keyspace is recorded before this returns; its options are not, so every
    /// later open must name it with [`crate::DbOptionsBuilder::keyspace`] or be refused.
    ///
    /// # Errors
    ///
    /// [`Error::InvalidArgument`] for an empty or overlong name, the default keyspace's name, a
    /// name already held, or one keyspace past [`tessarikv_constants::MAX_KEYSPACES`]; on
    /// storage, [`Error::FlushFailed`] or [`Error::CompactionFailed`] when the background work
    /// stopped, and the storage's error when recording the keyspace failed.
    pub async fn create_keyspace(
        &self,
        name: &str,
        options: KeyspaceOptions,
    ) -> Result<KeyspaceId> {
        check_new(name, std::iter::empty(), 0)?;
        let Some(installer) = &self.installer else {
            return self.create_in_memory(name, &options);
        };
        let (done, created) = oneshot::channel();
        let stopped = || installer.signals.failure().unwrap_or(Error::FlushFailed);
        installer
            .requests
            .send(Command::Create(Create {
                name: name.to_owned(),
                options,
                done,
            }))
            .await
            .map_err(|_| stopped())?;
        created.await.map_err(|_| stopped())?
    }

    /// Drops `keyspace` with every key it holds: reads and writes naming it are refused from now
    /// on, while a write that found it before still applies its other entries. Its table files
    /// are removed once no reader holds them. Its name may be taken again, by a keyspace with a
    /// new id. On storage the drop is recorded before this returns, and later opens must no longer
    /// name it.
    ///
    /// # Errors
    ///
    /// [`Error::InvalidArgument`] for the default keyspace, which is never dropped,
    /// [`Error::UnknownKeyspace`] for a keyspace this database does not hold; on storage,
    /// [`Error::FlushFailed`] or [`Error::CompactionFailed`] when the background work stopped,
    /// and the storage's error when recording the drop failed.
    pub async fn drop_keyspace(&self, keyspace: KeyspaceId) -> Result<()> {
        if keyspace == KeyspaceId::DEFAULT {
            return Err(Error::InvalidArgument {
                what: "dropping a keyspace",
                reason: "the default keyspace is never dropped",
            });
        }
        let Some(installer) = &self.installer else {
            return self.drop_in_memory(keyspace.get());
        };
        let (done, dropped) = oneshot::channel();
        let stopped = || installer.signals.failure().unwrap_or(Error::FlushFailed);
        installer
            .requests
            .send(Command::Drop(DropKeyspace {
                keyspace: keyspace.get(),
                done,
            }))
            .await
            .map_err(|_| stopped())?;
        dropped.await.map_err(|_| stopped())?
    }

    /// Drops a keyspace of a database without a manifest, publishing the view without it by
    /// compare-and-swap.
    fn drop_in_memory(&self, keyspace: u32) -> Result<()> {
        loop {
            let current = self.view.load_full();
            if !current.holds(keyspace) {
                return Err(Error::UnknownKeyspace { keyspace });
            }
            let (next, _) = current.without_keyspace(keyspace);
            let previous = self.view.compare_and_swap(&current, Arc::new(next));
            if Arc::ptr_eq(&previous, &current) {
                return Ok(());
            }
        }
    }

    /// Creates a keyspace in a database without a manifest: the view naming it is published by
    /// compare-and-swap, so creations racing each other each take their own id.
    fn create_in_memory(&self, name: &str, options: &KeyspaceOptions) -> Result<KeyspaceId> {
        loop {
            let current = self.view.load_full();
            let names = current.names();
            check_new(
                name,
                names.iter().map(|(_, held)| held.as_str()),
                names.len(),
            )?;
            let id = current.next_keyspace();
            let table = Memtable::sized(options.memtable_bytes());
            let next = Arc::new(current.with_keyspace(id, name, table)?);
            let previous = self.view.compare_and_swap(&current, next);
            if Arc::ptr_eq(&previous, &current) {
                return Ok(KeyspaceId::new(id));
            }
        }
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

impl Flusher {
    /// The options of `keyspace`; the default keyspace's for 0.
    pub(super) fn keyspace_options(&self, keyspace: u32) -> &KeyspaceOptions {
        options_of(&self.keyspaces, &self.keyspace, keyspace)
    }

    /// Whether `keyspace` is held: the default one, or one opened or created and not dropped.
    pub(super) fn holds(&self, keyspace: u32) -> bool {
        keyspace == 0 || self.keyspaces.iter().any(|(id, _)| *id == keyspace)
    }

    /// Every keyspace held, the default one first, with its options.
    pub(super) fn all_keyspaces(&self) -> impl Iterator<Item = (u32, &KeyspaceOptions)> {
        std::iter::once((0, &self.keyspace))
            .chain(self.keyspaces.iter().map(|(id, options)| (*id, options)))
    }
}
