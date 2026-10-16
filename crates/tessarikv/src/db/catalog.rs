//! Keyspaces created while the database runs.
//!
//! On storage the installer is the one writer of the manifest and of the view, so a change goes
//! to it as a [`Command`] and waits on its own reply channel. It is made once the manifest is at
//! hand (a running flush holds it): one synced manifest edit records the keyspace with an id never
//! handed out before, then the installer takes the keyspace's options and publishes a view in
//! which it holds an in-memory table. A failed edit stops the installer like a failed flush.

use std::collections::VecDeque;
use std::sync::Arc;

use tessarikv_constants as limits;
use tessarikv_format::{KeyspaceRecord, ManifestEdit};
use tessarikv_types::KeyspaceId;
use tokio::sync::oneshot;

use super::compaction::manual::Request;
use super::installer::Flusher;
use crate::memtable::Memtable;
use crate::options::DEFAULT_NAME;
use crate::{Error, KeyspaceOptions, Result};

/// What the installer is asked to do.
pub(crate) enum Command {
    /// Compact a key range (see [`crate::Db::compact_range`]).
    Compact(Request),
    /// Create a keyspace (see [`crate::Db::create_keyspace`]).
    Create(Create),
}

/// A request to create the keyspace `name` with `options`, and where its id goes.
pub(crate) struct Create {
    pub(crate) name: String,
    pub(crate) options: KeyspaceOptions,
    pub(crate) done: oneshot::Sender<Result<KeyspaceId>>,
}

/// Keyspace changes waiting for the manifest.
#[derive(Default)]
pub(super) struct Catalog {
    queued: VecDeque<Create>,
}

impl Catalog {
    /// Queues a change; [`Catalog::step`] makes it.
    pub(super) fn request(&mut self, create: Create) {
        self.queued.push_back(create);
    }

    /// Makes the waiting changes while the manifest is at hand; returns whether a new view was
    /// published. A change the keyspaces refuse is answered with the refusal.
    ///
    /// # Errors
    ///
    /// The manifest's error when an edit fails: the installer stops.
    pub(super) async fn step(&mut self, flusher: &mut Flusher) -> Result<bool> {
        let mut published = false;
        while flusher.manifest.is_some()
            && let Some(Create {
                name,
                options,
                done,
            }) = self.queued.pop_front()
        {
            let Some(manifest) = flusher.manifest.as_ref() else {
                break;
            };
            let version = manifest.version();
            let held = version.keyspaces().count();
            let id = version.next_keyspace().max(1);
            let checked = check_new(&name, version.keyspaces().map(|(_, held)| held), held)
                .and_then(|()| {
                    id.checked_add(1)
                        .ok_or(refuse("its keyspace ids are exhausted"))
                });
            let next = match checked {
                Ok(next) => next,
                Err(refused) => {
                    let _ = done.send(Err(refused));
                    continue;
                }
            };
            append(
                flusher,
                ManifestEdit {
                    added_keyspaces: vec![KeyspaceRecord {
                        id,
                        name: name.clone(),
                    }],
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
            published = true;
            // The caller may have stopped waiting.
            let _ = done.send(Ok(KeyspaceId::new(id)));
        }
        Ok(published)
    }
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
