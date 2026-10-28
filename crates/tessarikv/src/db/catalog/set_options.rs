//! Changing a keyspace's options while the database runs.
//!
//! The installer owns every keyspace's options on storage, so a change goes to it as a
//! [`Command`] and is answered once made. Work that starts afterwards takes the new options:
//! the compaction triggers and limits, what writers are asked to wait for, and how new table
//! files are written (block size, filters, compression, blob threshold, prefix filter, the
//! compaction filter). Work already running keeps the options it started with. Options that
//! describe the keyspace's in-memory tables or the shape of its levels are refused.

use std::mem::discriminant;
use std::sync::Arc;

use tessarikv_types::KeyspaceId;
use tokio::sync::oneshot;

use super::Command;
use crate::db::Db;
use crate::db::installer::Flusher;
use crate::{Error, KeyspaceOptions, Result};

/// A request to replace the options of `keyspace`, and where the outcome goes.
pub(crate) struct SetOptions {
    pub(crate) keyspace: u32,
    pub(crate) options: KeyspaceOptions,
    pub(crate) done: oneshot::Sender<Result<()>>,
}

impl Db {
    /// Replaces the options of `keyspace`: flushes and compactions starting after the call, and
    /// the stalls asked of writers, follow the new ones, and table files written from then on
    /// are built with them; a flush or compaction already running keeps the options it began
    /// with. The options of the next open are those its caller passes, as always. In memory,
    /// where no background work runs, nothing reads them and it returns at once.
    ///
    /// # Errors
    ///
    /// [`Error::UnknownKeyspace`] for a keyspace this database does not hold,
    /// [`Error::InvalidArgument`] when the options change the compaction style, the sizes of
    /// the in-memory tables (`memtable_bytes`, `max_immutable_memtables`) or the merge operator,
    /// which hold while the database is open, [`Error::ReadOnly`] on a database opened read-only, and
    /// [`Error::FlushFailed`] or [`Error::CompactionFailed`] when the background work stopped.
    pub async fn set_keyspace_options(
        &self,
        keyspace: KeyspaceId,
        options: KeyspaceOptions,
    ) -> Result<()> {
        self.refuse_read_only("changing a keyspace's options")?;
        self.check_keyspace(keyspace)?;
        let Some(installer) = &self.installer else {
            return Ok(());
        };
        let (done, changed) = oneshot::channel();
        let stopped = || installer.signals.failure().unwrap_or(Error::FlushFailed);
        installer
            .requests
            .send(Command::SetOptions(SetOptions {
                keyspace: keyspace.get(),
                options,
                done,
            }))
            .await
            .map_err(|_| stopped())?;
        changed.await.map_err(|_| stopped())?
    }
}

impl Flusher {
    /// Makes the change `set` asks for and answers it.
    pub(in crate::db) fn set_options(&mut self, set: SetOptions) {
        let SetOptions {
            keyspace,
            options,
            done,
        } = set;
        let current = if keyspace == 0 {
            Some(&mut self.keyspace)
        } else {
            self.keyspaces
                .iter_mut()
                .find(|(id, _)| *id == keyspace)
                .map(|(_, held)| held)
        };
        let outcome = match current {
            // Dropped since the request was checked.
            None => Err(Error::UnknownKeyspace { keyspace }),
            Some(current) => changeable(current, &options).map(|()| *current = options),
        };
        // The caller may have stopped waiting; the change holds either way.
        let _ = done.send(outcome);
    }
}

/// Refuses a change from `current` to `next` that an open keyspace cannot take.
fn changeable(current: &KeyspaceOptions, next: &KeyspaceOptions) -> Result<()> {
    let refuse = |reason| {
        Err(Error::InvalidArgument {
            what: "changing a keyspace's options",
            reason,
        })
    };
    if discriminant(&current.compaction()) != discriminant(&next.compaction()) {
        return refuse("the compaction style of an open keyspace cannot change");
    }
    if current.memtable_bytes() != next.memtable_bytes()
        || current.max_immutable_memtables() != next.max_immutable_memtables()
    {
        return refuse("the in-memory table sizes of an open keyspace cannot change");
    }
    let operator = |options: &KeyspaceOptions| options.merge_operator().map(Arc::as_ptr);
    if operator(current) != operator(next) {
        return refuse("the merge operator of an open keyspace cannot change");
    }
    Ok(())
}

#[cfg(test)]
mod tests;
