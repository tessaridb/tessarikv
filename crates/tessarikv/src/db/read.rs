//! Point reads and ordered scans at a snapshot.

use tessarikv_types::{KeyspaceId, ValueKind};

use super::Db;
use super::merge::MergedVersions;
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
        Self::check_keyspace(keyspace)?;
        let snapshot = self.snapshot()?;
        let view = self.view.load();
        // Newest table first: the first table that decides the key answers for it.
        for table in view.tables() {
            match table.get(key, snapshot) {
                Lookup::Value(value) => return Ok(Some(value.to_vec())),
                Lookup::Deleted => return Ok(None),
                Lookup::Absent => {}
            }
        }
        Ok(None)
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
        Self::check_keyspace(keyspace)?;
        let snapshot = self.snapshot()?;
        let mut page = ScanPage::default();
        let view = self.view.load();
        let mut versions = MergedVersions::new(view.tables().map(|table| table.iter()).collect());
        versions.seek(start, snapshot);
        let mut previous: Option<&[u8]> = None;
        while let Some(version) = versions.entry() {
            if end.is_some_and(|end| version.user_key >= end) {
                break;
            }
            // Versions newer than the snapshot, and older versions of a key already decided, are
            // passed over; the first version at or below the snapshot decides the key.
            if version.sequence <= snapshot && previous != Some(version.user_key) {
                previous = Some(version.user_key);
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
        Ok(page)
    }
}
