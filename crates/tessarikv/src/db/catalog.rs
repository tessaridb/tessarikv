//! Keyspaces created and dropped while the database runs.
//!
//! On storage the installer is the one writer of the manifest and of the view, so a change goes
//! to it as a [`Command`] and waits on its own reply channel. It is made once the manifest is at
//! hand (a running flush holds it), as one synced manifest edit and then a new view. A creation
//! records the keyspace with an id never handed out before and gives it an in-memory table. A
//! drop removes the keyspace and its tables; its files go once no reader holds them, a flush or
//! compaction of it writes nothing, its range requests are refused, and a write that found it
//! before the drop still applies its other entries (see `Db::apply`). A failed edit stops the
//! installer like a failed flush.

use std::collections::VecDeque;
use std::sync::Arc;

use tessarikv_constants as limits;
use tessarikv_constants::LEVELS;
use tessarikv_format::{KeyspaceRecord, ManifestEdit, TableRef};
use tessarikv_types::KeyspaceId;
use tokio::sync::oneshot;

use super::compaction::Compactions;
use super::compaction::manual::Request;
use super::installer::Flusher;
use super::view::View;
use crate::memtable::Memtable;
use crate::options::DEFAULT_NAME;
use crate::version::Version;
use crate::{Error, KeyspaceOptions, Result};

/// What the installer is asked to do.
pub(crate) enum Command {
    /// Compact a key range (see [`crate::Db::compact_range`]).
    Compact(Request),
    /// Create a keyspace (see [`crate::Db::create_keyspace`]).
    Create(Create),
    /// Drop a keyspace (see [`crate::Db::drop_keyspace`]).
    Drop(DropKeyspace),
    /// Hand over the version and the view naming the same table files (see
    /// [`crate::Db::checkpoint`]).
    Checkpoint(oneshot::Sender<Pinned>),
}

/// A version and the view naming its table files; the view keeps them from removal while held.
pub(crate) type Pinned = (Arc<Version>, Arc<View>);

/// A request to create the keyspace `name` with `options`, and where its id goes.
pub(crate) struct Create {
    pub(crate) name: String,
    pub(crate) options: KeyspaceOptions,
    pub(crate) done: oneshot::Sender<Result<KeyspaceId>>,
}

/// A request to drop `keyspace`, and where the outcome goes.
pub(crate) struct DropKeyspace {
    pub(crate) keyspace: u32,
    pub(crate) done: oneshot::Sender<Result<()>>,
}

/// A keyspace change waiting for the manifest.
enum Change {
    Create(Create),
    Drop(DropKeyspace),
    Checkpoint(oneshot::Sender<Pinned>),
}

/// Keyspace changes waiting for the manifest, in the order asked.
#[derive(Default)]
pub(super) struct Catalog {
    queued: VecDeque<Change>,
}

impl Catalog {
    /// Queues a creation; [`Catalog::step`] makes it.
    pub(super) fn create(&mut self, create: Create) {
        self.queued.push_back(Change::Create(create));
    }

    /// Queues a drop; [`Catalog::step`] makes it.
    pub(super) fn drop_keyspace(&mut self, drop: DropKeyspace) {
        self.queued.push_back(Change::Drop(drop));
    }

    /// Queues a request for the version and its view; [`Catalog::step`] answers it.
    pub(super) fn checkpoint(&mut self, reply: oneshot::Sender<Pinned>) {
        self.queued.push_back(Change::Checkpoint(reply));
    }

    /// Makes the waiting changes while the manifest is at hand; returns whether a new view was
    /// published. A change the keyspaces refuse is answered with the refusal.
    ///
    /// # Errors
    ///
    /// The manifest's error when an edit fails: the installer stops.
    pub(super) async fn step(
        &mut self,
        flusher: &mut Flusher,
        compactions: &mut Compactions,
    ) -> Result<bool> {
        let mut published = false;
        while flusher.manifest.is_some()
            && let Some(change) = self.queued.pop_front()
        {
            published |= match change {
                Change::Create(create) => created(flusher, create).await?,
                Change::Drop(drop) => dropped(flusher, compactions, drop).await?,
                Change::Checkpoint(reply) => {
                    // The installer writes both, so they name the same files here.
                    if let Some(manifest) = flusher.manifest.as_ref() {
                        let _ = reply.send((manifest.version(), flusher.view.load_full()));
                    }
                    false
                }
            };
        }
        Ok(published)
    }
}

/// Creates a keyspace: one synced edit naming it with an id never handed out before, then its
/// options and a view in which it holds a table. Returns whether it was created.
async fn created(flusher: &mut Flusher, create: Create) -> Result<bool> {
    let Create {
        name,
        options,
        done,
    } = create;
    let Some(manifest) = flusher.manifest.as_ref() else {
        return Ok(false);
    };
    let version = manifest.version();
    let held = version.keyspaces().count();
    let id = version.next_keyspace().max(1);
    let checked =
        check_new(&name, version.keyspaces().map(|(_, held)| held), held).and_then(|()| {
            id.checked_add(1)
                .ok_or(refuse("its keyspace ids are exhausted"))
        });
    let next = match checked {
        Ok(next) => next,
        Err(refused) => {
            let _ = done.send(Err(refused));
            return Ok(false);
        }
    };
    let added = vec![KeyspaceRecord {
        id,
        name: name.clone(),
    }];
    append(
        flusher,
        ManifestEdit {
            added_keyspaces: added,
            next_keyspace: Some(next),
            ..ManifestEdit::default()
        },
    )
    .await?;
    let table = Memtable::sized(options.memtable_bytes());
    flusher.keyspaces.push((id, options));
    let current = flusher.view.load_full();
    flusher
        .view
        .store(Arc::new(current.with_keyspace(id, &name, table)?));
    // The caller may have stopped waiting.
    let _ = done.send(Ok(KeyspaceId::new(id)));
    Ok(true)
}

/// Drops a keyspace: one synced edit removing its tables and the keyspace, then a view without
/// it; its files go once no reader holds them, its range requests are answered. Returns
/// whether it was dropped.
async fn dropped(
    flusher: &mut Flusher,
    compactions: &mut Compactions,
    drop: DropKeyspace,
) -> Result<bool> {
    let DropKeyspace { keyspace, done } = drop;
    let Some(manifest) = flusher.manifest.as_ref() else {
        return Ok(false);
    };
    let version = manifest.version();
    if keyspace == 0 {
        let _ = done.send(Err(Error::InvalidArgument {
            what: "dropping a keyspace",
            reason: "the default keyspace is never dropped",
        }));
        return Ok(false);
    }
    if !version.keyspaces().any(|(id, _)| id == keyspace) {
        let _ = done.send(Err(Error::UnknownKeyspace { keyspace }));
        return Ok(false);
    }
    let removed = (0..LEVELS)
        .flat_map(|level| version.tables(keyspace, level))
        .map(|table| TableRef {
            keyspace,
            level: table.level,
            id: table.id,
        })
        .collect();
    append(
        flusher,
        ManifestEdit {
            removed,
            dropped_keyspaces: vec![keyspace],
            ..ManifestEdit::default()
        },
    )
    .await?;
    flusher.keyspaces.retain(|(id, _)| *id != keyspace);
    let (view, files) = flusher.view.load().without_keyspace(keyspace);
    flusher.view.store(Arc::new(view));
    compactions.retire(files, &flusher.dir);
    let _ = done.send(Ok(()));
    Ok(true)
}

/// Appends `edit` to the manifest, synced, on a blocking thread.
async fn append(flusher: &mut Flusher, edit: ManifestEdit) -> Result<()> {
    let mut manifest = flusher.manifest.take().ok_or(Error::FlushFailed)?;
    let (manifest, appended) = tokio::task::spawn_blocking(move || {
        let appended = manifest.append(&edit);
        (manifest, appended)
    })
    .await
    .map_err(|_| Error::FlushFailed)?;
    flusher.manifest = Some(manifest);
    appended
}

/// Why creating a keyspace is refused.
pub(super) fn refuse(reason: &'static str) -> Error {
    Error::InvalidArgument {
        what: "creating a keyspace",
        reason,
    }
}

/// Refuses `name` for a new keyspace beside the `count` keyspaces named `held`: empty, too
/// long, the default keyspace's, already held, or one keyspace too many.
pub(super) fn check_new<'a>(
    name: &str,
    mut held: impl Iterator<Item = &'a str>,
    count: usize,
) -> Result<()> {
    if name.is_empty() || name.len() > limits::MAX_KEYSPACE_NAME_BYTES {
        return Err(refuse("a keyspace name is empty or too long"));
    }
    if name == DEFAULT_NAME || held.any(|taken| taken == name) {
        return Err(refuse("a keyspace already has that name"));
    }
    // The default keyspace counts too.
    let most = usize::try_from(limits::MAX_KEYSPACES).unwrap_or(usize::MAX);
    if count.saturating_add(1) >= most {
        return Err(refuse("the database holds as many keyspaces as it may"));
    }
    Ok(())
}
