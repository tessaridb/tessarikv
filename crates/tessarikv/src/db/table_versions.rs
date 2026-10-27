//! The versions of one table file, as a merge reads them.

use std::sync::Arc;

use tessarikv_format::TableIter;
use tessarikv_types::{InternalKeyRef, SequenceNumber, ValueKind, encode_internal, seek_trailer};

use super::merge::KeyVersion;
use super::table_blocks::TableBlocks;
use super::tables::format_error;
use super::view::TableFile;
use crate::Error;

/// The versions of a table file, read through its iterator. Each move finishes once the blocks
/// it needs are loaded. A damaged block or key, or a failed load, ends them, and the error is
/// kept until the walk reports it.
pub(super) struct TableVersions {
    versions: TableIter,
    blocks: Arc<TableBlocks>,
    fill_cache: bool,
    target: Vec<u8>,
    error: Option<Error>,
}

impl TableVersions {
    /// The versions of `file`, keeping the blocks they load in the cache unless `fill_cache`
    /// is false.
    pub(super) fn new(file: &TableFile, fill_cache: bool) -> Self {
        Self {
            versions: file.table.iter(),
            blocks: Arc::clone(&file.blocks),
            fill_cache,
            target: Vec::new(),
            error: None,
        }
    }

    pub(super) async fn seek(&mut self, key: &[u8], snapshot: SequenceNumber) {
        self.target.clear();
        encode_internal(key, seek_trailer(snapshot), &mut self.target);
        self.versions.seek(&self.target);
        self.finish_move().await;
    }

    pub(super) async fn seek_to_first(&mut self) {
        self.versions.seek_to_first();
        self.finish_move().await;
    }

    pub(super) async fn next(&mut self) {
        if self.error.is_none() {
            self.versions.next();
            self.finish_move().await;
        }
    }

    pub(super) async fn seek_to_last(&mut self) {
        self.versions.seek_to_last();
        self.finish_move().await;
    }

    pub(super) async fn seek_for_prev(&mut self, target: &[u8]) {
        self.versions.seek_for_prev(target);
        self.finish_move().await;
    }

    pub(super) async fn prev(&mut self) {
        if self.error.is_none() {
            self.versions.prev();
            self.finish_move().await;
        }
    }

    pub(super) fn entry(&self) -> Option<KeyVersion<'_>> {
        if self.error.is_some() || !self.versions.valid() {
            return None;
        }
        let (key, kind) = parse(self.versions.key())?;
        Some(KeyVersion {
            user_key: key.user_key(),
            sequence: key.trailer().sequence(),
            kind,
            value: self.versions.value(),
        })
    }

    /// Whether the versions ended early at a damaged block or key, or a failed load.
    pub(super) fn failed(&self) -> bool {
        self.error.is_some()
    }

    /// Why the versions ended early, taken once.
    pub(super) fn take_error(&mut self) -> Option<Error> {
        self.error.take()
    }

    /// Loads the blocks the last move waits for, then records why the versions ended, when they
    /// ended early.
    async fn finish_move(&mut self) {
        if self.error.is_some() {
            return;
        }
        if let Err(error) = self
            .blocks
            .complete(&mut self.versions, self.fill_cache)
            .await
        {
            self.error = Some(error);
        } else if let Err(error) = self.versions.status() {
            self.error = Some(format_error(&error, None));
        } else if self.versions.valid() && parse(self.versions.key()).is_none() {
            self.error = Some(Error::Corruption {
                what: "table",
                path: None,
                offset: None,
                detail: "a stored key has no trailer or a kind this build does not read".to_owned(),
            });
        }
    }
}

/// A stored key and its kind; `None` for a key without a trailer or with a kind a table never
/// holds among its versions (range deletes are kept in their own block).
fn parse(encoded: &[u8]) -> Option<(InternalKeyRef<'_>, ValueKind)> {
    let key = InternalKeyRef::parse(encoded).ok()?;
    match key.trailer().kind() {
        Ok(kind @ (ValueKind::Value | ValueKind::Delete)) => Some((key, kind)),
        Ok(ValueKind::RangeDelete) | Err(_) => None,
    }
}
