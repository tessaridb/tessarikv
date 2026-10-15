//! Walking the versions of several sources, in-memory tables and table files, as one, in
//! internal-key order.

use std::cmp::Ordering;

use tessarikv_format::TableIter;
use tessarikv_types::{InternalKeyRef, SequenceNumber, ValueKind, encode_internal, seek_trailer};

use super::tables::format_error;
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
    fn seek(&mut self, key: &[u8], snapshot: SequenceNumber) {
        match self {
            Self::Memtable(versions) => versions.seek(key, snapshot),
            Self::Shared(versions) => versions.seek(key, snapshot),
            Self::Table(versions) => versions.seek(key, snapshot),
        }
    }

    fn next(&mut self) {
        match self {
            Self::Memtable(versions) => versions.next(),
            Self::Shared(versions) => versions.next(),
            Self::Table(versions) => versions.next(),
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

/// The versions of a table file, read through its iterator. A damaged block or key ends them,
/// and the error is kept until the walk reports it.
pub(super) struct TableVersions {
    versions: TableIter,
    target: Vec<u8>,
    error: Option<Error>,
}

impl TableVersions {
    pub(super) fn new(versions: TableIter) -> Self {
        Self {
            versions,
            target: Vec::new(),
            error: None,
        }
    }

    fn seek(&mut self, key: &[u8], snapshot: SequenceNumber) {
        self.target.clear();
        encode_internal(key, seek_trailer(snapshot), &mut self.target);
        self.versions.seek(&self.target);
        self.check();
    }

    fn next(&mut self) {
        if self.error.is_none() {
            self.versions.next();
            self.check();
        }
    }

    /// Records why the versions ended, when they ended early.
    fn check(&mut self) {
        if let Err(error) = self.versions.status() {
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

    fn entry(&self) -> Option<KeyVersion<'_>> {
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
}

/// A stored key and its kind; `None` for a key without a trailer or with a kind a table never
/// holds (range deletes do not reach tables yet: batches refuse them).
fn parse(encoded: &[u8]) -> Option<(InternalKeyRef<'_>, ValueKind)> {
    let key = InternalKeyRef::parse(encoded).ok()?;
    match key.trailer().kind() {
        Ok(kind @ (ValueKind::Value | ValueKind::Delete)) => Some((key, kind)),
        Ok(ValueKind::RangeDelete) | Err(_) => None,
    }
}

/// The versions of every source of a view, by user key ascending and newest version first.
///
/// Each sequence number lives in exactly one source, so two sources never hold the same version
/// and the merge needs no tie rule. The sources of one view are few, so the next version is
/// found by comparing the current version of each.
pub(super) struct MergedVersions<'a> {
    sources: Vec<Source<'a>>,
}

impl<'a> MergedVersions<'a> {
    pub(super) fn new(sources: Vec<Source<'a>>) -> Self {
        Self { sources }
    }

    /// Moves every source to the first version of the least key not less than `key` that is
    /// visible at `snapshot`.
    pub(super) fn seek(&mut self, key: &[u8], snapshot: SequenceNumber) {
        for source in &mut self.sources {
            source.seek(key, snapshot);
        }
    }

    /// The least version among the sources, or `None` when every source is past its end.
    pub(super) fn entry(&self) -> Option<KeyVersion<'_>> {
        self.least().map(|(_, entry)| entry)
    }

    /// Moves past the current version.
    pub(super) fn next(&mut self) {
        if let Some((index, _)) = self.least()
            && let Some(source) = self.sources.get_mut(index)
        {
            source.next();
        }
    }

    /// Why a table file's versions ended early, taken once; `None` while every source is whole.
    pub(super) fn take_error(&mut self) -> Option<Error> {
        self.sources.iter_mut().find_map(|source| match source {
            Source::Table(versions) => versions.error.take(),
            Source::Memtable(_) | Source::Shared(_) => None,
        })
    }

    /// Whether every source ended where its data ended.
    ///
    /// # Errors
    ///
    /// [`Error::Corruption`] when a table file ended early at a damaged block or key: the walk
    /// may have missed versions after it.
    pub(super) fn finish(self) -> Result<()> {
        for source in self.sources {
            if let Source::Table(versions) = source
                && let Some(error) = versions.error
            {
                return Err(error);
            }
        }
        Ok(())
    }

    /// The source holding the least version, and that version.
    fn least(&self) -> Option<(usize, KeyVersion<'_>)> {
        self.sources
            .iter()
            .enumerate()
            .filter_map(|(index, source)| source.entry().map(|entry| (index, entry)))
            .min_by(|(_, left), (_, right)| version_order(left, right))
    }
}

/// User key ascending, then the newer version first.
fn version_order(left: &KeyVersion<'_>, right: &KeyVersion<'_>) -> Ordering {
    left.user_key
        .cmp(right.user_key)
        .then_with(|| right.sequence.cmp(&left.sequence))
}
