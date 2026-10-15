//! Point reads and ordered scans at a snapshot.

use tessarikv_types::{KeyspaceId, SequenceNumber, ValueKind};

use super::Db;
use super::merge::{MergedVersions, Source, TableVersions};
use super::tables::{TableLookup, lookup};
use super::view::View;
use crate::memtable::Lookup;
use crate::{ReadOptions, Result};

/// One page of a scan: live entries in key order, and where the next page starts.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ScanPage {
    /// Key-value pairs, ascending by key.
    pub entries: Vec<(Vec<u8>, Vec<u8>)>,
    /// The key the next page starts at, when the page stopped at its limit.
    pub next: Option<Vec<u8>>,
}

impl Db {
    /// The value of `key` in `keyspace` at the latest snapshot.
    ///
    /// # Errors
    ///
    /// [`crate::Error::UnknownKeyspace`] for a keyspace this database does not hold.
    pub async fn get(
        &self,
        keyspace: KeyspaceId,
        key: &[u8],
        _options: &ReadOptions,
    ) -> Result<Option<Vec<u8>>> {
        self.get_at(keyspace, key, self.visible_sequence()?)
    }

    /// The value of `key` in `keyspace` at `snapshot`.
    pub(super) fn get_at(
        &self,
        keyspace: KeyspaceId,
        key: &[u8],
        snapshot: SequenceNumber,
    ) -> Result<Option<Vec<u8>>> {
        Self::check_keyspace(keyspace)?;
        get_in(&self.view.load(), keyspace, key, snapshot)
    }

    /// Up to `limit` live entries of `keyspace` with keys in `[start, end)` (unbounded above
    /// when `end` is `None`), at one snapshot: a page never shows part of a batch. A full page
    /// names the key the next page starts at.
    ///
    /// # Errors
    ///
    /// [`crate::Error::UnknownKeyspace`] for a keyspace this database does not hold.
    pub async fn scan(
        &self,
        keyspace: KeyspaceId,
        start: &[u8],
        end: Option<&[u8]>,
        limit: usize,
        _options: &ReadOptions,
    ) -> Result<ScanPage> {
        self.scan_at(keyspace, start, end, limit, self.visible_sequence()?)
    }

    /// A page of `keyspace` at `snapshot`, as [`Db::scan`] returns one.
    pub(super) fn scan_at(
        &self,
        keyspace: KeyspaceId,
        start: &[u8],
        end: Option<&[u8]>,
        limit: usize,
        snapshot: SequenceNumber,
    ) -> Result<ScanPage> {
        Self::check_keyspace(keyspace)?;
        scan_in(&self.view.load(), keyspace, start, end, limit, snapshot)
    }
}

/// The value of `key` in `keyspace` of `view` at `snapshot`.
///
/// # Errors
///
/// [`crate::Error::Corruption`] when a table file on the way is damaged.
pub(super) fn get_in(
    view: &View,
    keyspace: KeyspaceId,
    key: &[u8],
    snapshot: SequenceNumber,
) -> Result<Option<Vec<u8>>> {
    // Newest first, in-memory tables before table files: the first that decides the key
    // answers for it.
    for table in view.memtables() {
        match table.get(key, snapshot) {
            Lookup::Value(value) => return Ok(Some(value.to_vec())),
            Lookup::Deleted => return Ok(None),
            Lookup::Absent => {}
        }
    }
    for file in view.files(keyspace.get()) {
        match lookup(&file.table, key, snapshot)? {
            TableLookup::Value(value) => return Ok(Some(value)),
            TableLookup::Deleted => return Ok(None),
            TableLookup::Absent => {}
        }
    }
    Ok(None)
}

/// A page of `keyspace` of `view` at `snapshot`, as [`Db::scan`] returns one.
///
/// # Errors
///
/// [`crate::Error::Corruption`] when a table file on the way is damaged.
pub(super) fn scan_in(
    view: &View,
    keyspace: KeyspaceId,
    start: &[u8],
    end: Option<&[u8]>,
    limit: usize,
    snapshot: SequenceNumber,
) -> Result<ScanPage> {
    let mut page = ScanPage::default();
    let sources = view
        .memtables()
        .map(|table| Source::Memtable(table.iter()))
        .chain(
            view.files(keyspace.get())
                .map(|file| Source::Table(Box::new(TableVersions::new(file.table.iter())))),
        )
        .collect();
    let mut versions = MergedVersions::new(sources);
    versions.seek(start, snapshot);
    // The user key last decided, kept in one buffer reused across keys.
    let mut decided: Option<Vec<u8>> = None;
    while let Some(version) = versions.entry() {
        if end.is_some_and(|end| version.user_key >= end) {
            break;
        }
        // Versions newer than the snapshot, and older versions of a key already decided, are
        // passed over; the first version at or below the snapshot decides the key.
        if version.sequence <= snapshot && decided.as_deref() != Some(version.user_key) {
            let key = decided.get_or_insert_with(Vec::new);
            key.clear();
            key.extend_from_slice(version.user_key);
            if version.kind == ValueKind::Value {
                if page.entries.len() >= limit {
                    page.next = Some(version.user_key.to_vec());
                    break;
                }
                page.entries
                    .push((version.user_key.to_vec(), version.value.to_vec()));
            }
        }
        versions.next();
    }
    versions.finish()?;
    Ok(page)
}
