//! The versions of one sorted level, as a merge reads them: one source for all its table files.
//!
//! The files of a level below 0 hold disjoint user-key ranges in key order, and every version of
//! a user key lies in one file (a compaction cuts its outputs only between user keys). So the
//! level reads as one ordered run: a seek opens the one file that can hold the key, and a walk
//! opens the next file only when the current one ends. A positioned read therefore touches one
//! file of the level however many it holds.

use std::borrow::Cow;

use tessarikv_types::{SequenceNumber, compare_internal};

use super::merge::KeyVersion;
use super::table_versions::TableVersions;
use super::tables::user_key;
use super::view::TableFile;
use crate::Error;

/// The versions of a sorted level's files, read one file at a time.
pub(super) struct LevelVersions<'a> {
    files: Cow<'a, [TableFile]>,
    fill_cache: bool,
    /// The file `open` reads.
    at: usize,
    open: Option<TableVersions>,
}

impl<'a> LevelVersions<'a> {
    /// The versions of `files`, one sorted level in key order, keeping the blocks they load in
    /// the cache unless `fill_cache` is false.
    pub(super) fn new(files: Cow<'a, [TableFile]>, fill_cache: bool) -> Self {
        Self {
            files,
            fill_cache,
            at: 0,
            open: None,
        }
    }

    /// Opens file `at`, when there is one, and returns it.
    fn open(&mut self, at: usize) -> Option<&mut TableVersions> {
        self.at = at;
        self.open = self
            .files
            .get(at)
            .map(|file| TableVersions::new(file, self.fill_cache));
        self.open.as_mut()
    }

    pub(super) async fn seek(&mut self, key: &[u8], snapshot: SequenceNumber) {
        // The first file whose greatest key is not below `key` is the only one that can hold it.
        let at = self
            .files
            .partition_point(|file| user_key(&file.meta.largest) < key);
        if let Some(versions) = self.open(at) {
            versions.seek(key, snapshot).await;
        }
        self.forward().await;
    }

    pub(super) async fn next(&mut self) {
        if let Some(versions) = &mut self.open {
            versions.next().await;
        }
        self.forward().await;
    }

    pub(super) async fn seek_to_last(&mut self) {
        let last = self.files.len().checked_sub(1);
        match last.and_then(|at| self.open(at)) {
            Some(versions) => versions.seek_to_last().await,
            None => self.open = None,
        }
        self.backward().await;
    }

    pub(super) async fn seek_for_prev(&mut self, target: &[u8]) {
        // The last file whose least key is not above `target` is the only one that can hold the
        // greatest version at or below it.
        let after = self
            .files
            .partition_point(|file| compare_internal(&file.meta.smallest, target).is_le());
        match after.checked_sub(1).and_then(|at| self.open(at)) {
            Some(versions) => versions.seek_for_prev(target).await,
            None => self.open = None,
        }
        self.backward().await;
    }

    pub(super) async fn prev(&mut self) {
        if let Some(versions) = &mut self.open {
            versions.prev().await;
        }
        self.backward().await;
    }

    pub(super) fn entry(&self) -> Option<KeyVersion<'_>> {
        self.open.as_ref()?.entry()
    }

    /// Why the versions ended early, taken once.
    pub(super) fn take_error(&mut self) -> Option<Error> {
        self.open.as_mut()?.take_error()
    }

    /// Past the end of the open file, the next file's first version; a file that failed stays
    /// open so its error is reported.
    async fn forward(&mut self) {
        while let Some(versions) = &self.open
            && versions.entry().is_none()
            && !versions.failed()
        {
            let next = self.at.saturating_add(1);
            match self.open(next) {
                Some(versions) => versions.seek_to_first().await,
                None => self.open = None,
            }
        }
    }

    /// Before the start of the open file, the previous file's last version.
    async fn backward(&mut self) {
        while let Some(versions) = &self.open
            && versions.entry().is_none()
            && !versions.failed()
        {
            match self.at.checked_sub(1) {
                Some(previous) => {
                    if let Some(versions) = self.open(previous) {
                        versions.seek_to_last().await;
                    }
                }
                None => self.open = None,
            }
        }
    }
}

/// The user keys a merge reads: from `low` (inclusive) to `high` (exclusive), unbounded where
/// `None`. Table files outside it are left out of the merge.
#[derive(Debug, Clone, Copy, Default)]
pub(super) struct Span<'k> {
    pub(super) low: Option<&'k [u8]>,
    pub(super) high: Option<&'k [u8]>,
}

impl Span<'_> {
    /// Whether `file`'s key range meets the span.
    pub(super) fn touches(&self, file: &TableFile) -> bool {
        self.low
            .is_none_or(|low| user_key(&file.meta.largest) >= low)
            && self
                .high
                .is_none_or(|high| user_key(&file.meta.smallest) < high)
    }

    /// Whether `file` may hold a key the span reads: its key range meets the span, and its
    /// prefix filter, when it has one, does not rule the span out.
    pub(super) fn admits(&self, file: &TableFile) -> bool {
        self.touches(file) && self.may_hold(file)
    }

    /// The files of a sorted level, in key order, that may hold a key the span reads: those that
    /// meet it, less those whose prefix filter rules it out. The level's own slice when no file
    /// is ruled out.
    pub(super) fn files_in<'f>(&self, level: &'f [TableFile]) -> Cow<'f, [TableFile]> {
        let within = self.within(level);
        if within.iter().all(|file| self.may_hold(file)) {
            return Cow::Borrowed(within);
        }
        Cow::Owned(
            within
                .iter()
                .filter(|file| self.may_hold(file))
                .cloned()
                .collect(),
        )
    }

    /// Whether `file`'s prefix filter lets it hold a key the span reads.
    fn may_hold(&self, file: &TableFile) -> bool {
        file.table
            .may_hold_prefix_range(self.low.unwrap_or_default(), self.high)
    }

    /// The files of a sorted level, in key order, that meet the span.
    pub(super) fn within<'f>(&self, level: &'f [TableFile]) -> &'f [TableFile] {
        let start = self.low.map_or(0, |low| {
            level.partition_point(|file| user_key(&file.meta.largest) < low)
        });
        let end = self.high.map_or(level.len(), |high| {
            level.partition_point(|file| user_key(&file.meta.smallest) < high)
        });
        level.get(start..end.max(start)).unwrap_or_default()
    }
}
