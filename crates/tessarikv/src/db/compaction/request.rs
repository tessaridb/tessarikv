//! Range requests and the keyspaces they name: a request for a keyspace no longer held, dropped
//! before or while it waited, is answered with [`Error::UnknownKeyspace`] instead of waiting for a
//! keyspace the installer no longer serves.

use std::path::Path;
use std::sync::Arc;

use tokio::sync::oneshot;

use super::Compactions;
use crate::db::flush_job::table_path;
use crate::db::installer::Flusher;
use crate::db::view::TableFile;
use crate::{Error, Result};

/// A caller's request: the user keys `[start, end)` of `keyspace` (unbounded above when `end`
/// is `None`), and where the outcome goes.
pub(crate) struct Request {
    pub(super) keyspace: u32,
    pub(super) start: Vec<u8>,
    pub(super) end: Option<Vec<u8>>,
    pub(super) done: oneshot::Sender<Result<()>>,
}

impl Request {
    /// A request for the user keys `[start, end)` of `keyspace`, and where its outcome arrives.
    pub(crate) fn new(
        keyspace: u32,
        start: &[u8],
        end: Option<&[u8]>,
    ) -> (Self, oneshot::Receiver<Result<()>>) {
        let (done, outcome) = oneshot::channel();
        let request = Self {
            keyspace,
            start: start.to_vec(),
            end: end.map(<[u8]>::to_vec),
            done,
        };
        (request, outcome)
    }

    /// Answers the caller, who may have stopped waiting, that the keyspace is gone.
    fn refuse(self) {
        let keyspace = self.keyspace;
        let _ = self.done.send(Err(Error::UnknownKeyspace { keyspace }));
    }
}

impl Compactions {
    /// Answers every request, served or waiting, for a keyspace `flusher` no longer holds.
    pub(super) fn refuse_unheld(&mut self, flusher: &Flusher) {
        if let Some(active) = self.active.take() {
            if flusher.holds(active.request().keyspace) {
                self.active = Some(active);
            } else {
                active.into_request().refuse();
            }
        }
        let (held, unheld) = std::mem::take(&mut self.queued)
            .into_iter()
            .partition(|request| flusher.holds(request.keyspace));
        self.queued = held;
        unheld.into_iter().for_each(Request::refuse);
    }

    /// Removes `files`, in `dir`, once no view reads them: a dropped keyspace's tables, or the
    /// outputs of a compaction of one.
    pub(crate) fn retire(&mut self, files: Vec<TableFile>, dir: &Path) {
        self.obsolete.extend(files.iter().map(|file| {
            (
                Arc::downgrade(&file.blocks),
                file.meta.id,
                table_path(dir, file.meta.id),
            )
        }));
    }
}
