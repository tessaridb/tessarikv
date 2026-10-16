//! Walking the versions of several sources, in-memory tables and table files, as one, in
//! internal-key order. A move finishes once every table file it touches holds the blocks it
//! needs, which may mean waiting for storage.

use std::cmp::Ordering;

use tessarikv_types::{SequenceNumber, ValueKind};

use super::table_versions::TableVersions;
use crate::memtable::{MemtableCursor, MemtableEntry, MemtableIter};
use crate::{Error, Result};

/// One version of a key.
#[derive(Debug, Clone, Copy)]
pub(super) struct KeyVersion<'a> {
    pub(super) user_key: &'a [u8],
    pub(super) sequence: SequenceNumber,
    pub(super) kind: ValueKind,
    /// The value of a put; empty for a delete.
    pub(super) value: &'a [u8],
}

impl<'a> From<MemtableEntry<'a>> for KeyVersion<'a> {
    fn from(entry: MemtableEntry<'a>) -> Self {
        Self {
            user_key: entry.user_key,
            sequence: entry.sequence,
            kind: entry.kind,
            value: entry.value,
        }
    }
}

/// Where versions come from.
pub(super) enum Source<'a> {
    /// An in-memory table.
    Memtable(MemtableIter<'a>),
    /// An in-memory table the source keeps alive itself.
    Shared(MemtableCursor),
    /// A table file; boxed, as its iterator is several times an in-memory one's size.
    Table(Box<TableVersions>),
}

impl Source<'_> {
    async fn seek(&mut self, key: &[u8], snapshot: SequenceNumber) {
        match self {
            Self::Memtable(versions) => versions.seek(key, snapshot),
            Self::Shared(versions) => versions.seek(key, snapshot),
            Self::Table(versions) => versions.seek(key, snapshot).await,
        }
    }

    async fn next(&mut self) {
        match self {
            Self::Memtable(versions) => versions.next(),
            Self::Shared(versions) => versions.next(),
            Self::Table(versions) => versions.next().await,
        }
    }

    async fn seek_to_last(&mut self) {
        match self {
            Self::Memtable(versions) => versions.seek_to_last(),
            Self::Shared(versions) => versions.seek_to_last(),
            Self::Table(versions) => versions.seek_to_last().await,
        }
    }

    async fn seek_for_prev(&mut self, target: &[u8]) {
        match self {
            Self::Memtable(versions) => versions.seek_for_prev(target),
            Self::Shared(versions) => versions.seek_for_prev(target),
            Self::Table(versions) => versions.seek_for_prev(target).await,
        }
    }

    async fn prev(&mut self) {
        match self {
            Self::Memtable(versions) => versions.prev(),
            Self::Shared(versions) => versions.prev(),
            Self::Table(versions) => versions.prev().await,
        }
    }

    fn entry(&self) -> Option<KeyVersion<'_>> {
        match self {
            Self::Memtable(versions) => versions.entry().map(KeyVersion::from),
            Self::Shared(versions) => versions.entry().map(KeyVersion::from),
            Self::Table(versions) => versions.entry(),
        }
    }
}

/// The versions of every source of a view, by user key ascending and newest version first.
///
/// Each sequence number lives in exactly one source, so two sources never hold the same version
/// and the merge needs no tie rule. The sources of one view are few, so the next version is
/// found by comparing the current version of each.
///
/// The walk runs in the direction of the last positioning: forward after [`Self::seek`],
/// backward after [`Self::seek_to_last`] and [`Self::seek_for_prev`]; a caller turning around
/// positions again.
pub(super) struct MergedVersions<'a> {
    sources: Vec<Source<'a>>,
    backward: bool,
}

impl<'a> MergedVersions<'a> {
    pub(super) fn new(sources: Vec<Source<'a>>) -> Self {
        Self {
            sources,
            backward: false,
        }
    }

    /// Moves every source to the first version of the least key not less than `key` that is
    /// visible at `snapshot`.
    pub(super) async fn seek(&mut self, key: &[u8], snapshot: SequenceNumber) {
        self.backward = false;
        for source in &mut self.sources {
            source.seek(key, snapshot).await;
        }
    }

    /// Moves every source to its last version, to walk backward.
    pub(super) async fn seek_to_last(&mut self) {
        self.backward = true;
        for source in &mut self.sources {
            source.seek_to_last().await;
        }
    }

    /// Moves every source to its last version whose internal key is not greater than
    /// `target`, to walk backward.
    pub(super) async fn seek_for_prev(&mut self, target: &[u8]) {
        self.backward = true;
        for source in &mut self.sources {
            source.seek_for_prev(target).await;
        }
    }

    /// The current version: the least among the sources walking forward, the greatest walking
    /// backward; `None` when every source is past its end.
    pub(super) fn entry(&self) -> Option<KeyVersion<'_>> {
        self.current().map(|(_, entry)| entry)
    }

    /// Moves past the current version, forward.
    pub(super) async fn next(&mut self) {
        if let Some((index, _)) = self.current()
            && let Some(source) = self.sources.get_mut(index)
        {
            source.next().await;
        }
    }

    /// Moves past the current version, backward.
    pub(super) async fn prev(&mut self) {
        if let Some((index, _)) = self.current()
            && let Some(source) = self.sources.get_mut(index)
        {
            source.prev().await;
        }
    }

    /// Why a table file's versions ended early, taken once; `None` while every source is whole.
    pub(super) fn take_error(&mut self) -> Option<Error> {
        self.sources.iter_mut().find_map(|source| match source {
            Source::Table(versions) => versions.take_error(),
            Source::Memtable(_) | Source::Shared(_) => None,
        })
    }

    /// Whether every source ended where its data ended.
    ///
    /// # Errors
    ///
    /// [`Error::Corruption`] when a table file ended early at a damaged block or key: the walk
    /// may have missed versions after it.
    pub(super) fn finish(mut self) -> Result<()> {
        match self.take_error() {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    /// The source holding the current version, and that version.
    fn current(&self) -> Option<(usize, KeyVersion<'_>)> {
        let versions = self
            .sources
            .iter()
            .enumerate()
            .filter_map(|(index, source)| source.entry().map(|entry| (index, entry)));
        if self.backward {
            versions.max_by(|(_, left), (_, right)| version_order(left, right))
        } else {
            versions.min_by(|(_, left), (_, right)| version_order(left, right))
        }
    }
}

/// User key ascending, then the newer version first.
fn version_order(left: &KeyVersion<'_>, right: &KeyVersion<'_>) -> Ordering {
    left.user_key
        .cmp(right.user_key)
        .then_with(|| right.sequence.cmp(&left.sequence))
}
