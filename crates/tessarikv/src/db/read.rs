//! Point reads and ordered scans at a snapshot.

use std::time::Instant;

use tessarikv_types::{KeyspaceId, SequenceNumber, ValueKind};

use super::Db;
use super::level_versions::Span;
use super::merge::{MergedVersions, sources};
use super::tables::{TableLookup, lookup};
use super::view::View;
use crate::memtable::Lookup;
use crate::statistics::{Histogram, Statistics, Ticker};
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
        options: &ReadOptions,
    ) -> Result<Option<Vec<u8>>> {
        self.check_keyspace(keyspace)?;
        // The view before the sequence: a compaction publishes its view only after it read
        // the visible sequence it keeps versions for, so a reader of that view reads at or
        // above it. The other way round, a compaction could drop a version this read needs.
        let view = self.view.load();
        let snapshot = self.visible_sequence()?;
        get_in(
            &view,
            keyspace,
            key,
            snapshot,
            options.fill_cache,
            self.statistics.as_deref(),
        )
        .await
    }

    /// The value of `key` in `keyspace` at `snapshot`.
    pub(super) async fn get_at(
        &self,
        keyspace: KeyspaceId,
        key: &[u8],
        snapshot: SequenceNumber,
        options: &ReadOptions,
    ) -> Result<Option<Vec<u8>>> {
        self.check_keyspace(keyspace)?;
        // A guard, not a shared count: holding it costs other readers nothing.
        let view = self.view.load();
        get_in(
            &view,
            keyspace,
            key,
            snapshot,
            options.fill_cache,
            self.statistics.as_deref(),
        )
        .await
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
        options: &ReadOptions,
    ) -> Result<ScanPage> {
        self.check_keyspace(keyspace)?;
        // The view before the sequence, as in [`Db::get`].
        let view = self.view.load();
        let snapshot = self.visible_sequence()?;
        let range = ScanRange { start, end, limit };
        scan_in(&view, keyspace, range, snapshot, options.fill_cache).await
    }

    /// A page of `keyspace` at `snapshot`, as [`Db::scan`] returns one.
    pub(super) async fn scan_at(
        &self,
        keyspace: KeyspaceId,
        start: &[u8],
        end: Option<&[u8]>,
        limit: usize,
        snapshot: SequenceNumber,
        options: &ReadOptions,
    ) -> Result<ScanPage> {
        self.check_keyspace(keyspace)?;
        let view = self.view.load();
        let range = ScanRange { start, end, limit };
        scan_in(&view, keyspace, range, snapshot, options.fill_cache).await
    }
}

/// The value of `key` in `keyspace` of `view` at `snapshot`, keeping the blocks it reads in the
/// cache unless `fill_cache` is false, and counted in `statistics` when there are any.
///
/// # Errors
///
/// [`crate::Error::Corruption`] when a table file on the way is damaged, and the storage's error
/// when reading one fails.
pub(super) async fn get_in(
    view: &View,
    keyspace: KeyspaceId,
    key: &[u8],
    snapshot: SequenceNumber,
    fill_cache: bool,
    statistics: Option<&Statistics>,
) -> Result<Option<Vec<u8>>> {
    let Some(statistics) = statistics else {
        return get_uncounted(view, keyspace, key, snapshot, fill_cache, None).await;
    };
    let started = Instant::now();
    statistics.record(Ticker::GetCalls, 1);
    let found = get_uncounted(view, keyspace, key, snapshot, fill_cache, Some(statistics)).await;
    let micros = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX);
    statistics.observe(Histogram::GetMicros, micros);
    found
}

/// [`get_in`] without the call and its time counted; `statistics` counts what the in-memory
/// tables answered.
async fn get_uncounted(
    view: &View,
    keyspace: KeyspaceId,
    key: &[u8],
    snapshot: SequenceNumber,
    fill_cache: bool,
    statistics: Option<&Statistics>,
) -> Result<Option<Vec<u8>>> {
    // A range delete covering the key deletes every version below its sequence.
    let floor = view
        .deletions(keyspace.get(), snapshot)
        .covering(key)
        .map_or(0, SequenceNumber::get);
    // Newest first, in-memory tables before table files: the first that decides the key
    // answers for it, and a table file whose key range misses the key cannot decide it.
    let decided = view.memtables(keyspace.get()).find_map(|table| {
        match table.get_above(key, snapshot, floor) {
            Lookup::Value(value) => Some(Some(value.to_vec())),
            Lookup::Deleted => Some(None),
            Lookup::Absent => None,
        }
    });
    if let Some(statistics) = statistics {
        let ticker = if decided.is_some() {
            Ticker::MemtableHit
        } else {
            Ticker::MemtableMiss
        };
        statistics.record(ticker, 1);
    }
    if let Some(value) = decided {
        return Ok(value);
    }
    for file in view.candidates(keyspace.get(), key) {
        match lookup(file, key, snapshot, floor, fill_cache, statistics).await? {
            TableLookup::Value(value) => return Ok(Some(value)),
            TableLookup::Deleted => return Ok(None),
            TableLookup::Absent => {}
        }
    }
    Ok(None)
}

/// Which keys a scan page lists: up to `limit` of those in `[start, end)`, unbounded above when
/// `end` is `None` (see [`Db::multi_scan`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScanRange<'a> {
    /// The first key the page may list.
    pub start: &'a [u8],
    /// The first key past the page's range; `None` reaches the end of the keyspace.
    pub end: Option<&'a [u8]>,
    /// The most entries the page lists.
    pub limit: usize,
}

/// A page of `keyspace` of `view` at `snapshot`, as [`Db::scan`] returns one, keeping the blocks
/// it reads in the cache unless `fill_cache` is false.
///
/// # Errors
///
/// As [`get_in`].
pub(super) async fn scan_in(
    view: &View,
    keyspace: KeyspaceId,
    range: ScanRange<'_>,
    snapshot: SequenceNumber,
    fill_cache: bool,
) -> Result<ScanPage> {
    let ScanRange { start, end, limit } = range;
    let mut page = ScanPage::default();
    let fragments = view.deletions(keyspace.get(), snapshot);
    let span = Span {
        low: Some(start),
        high: end,
    };
    let mut versions = MergedVersions::new(sources(view, keyspace, fill_cache, span));
    versions.seek(start, snapshot).await;
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
            if version.kind == ValueKind::Value
                && !fragments.deletes(version.user_key, version.sequence)
            {
                if page.entries.len() >= limit {
                    page.next = Some(version.user_key.to_vec());
                    break;
                }
                page.entries
                    .push((version.user_key.to_vec(), version.value.to_vec()));
            }
        }
        versions.next().await;
    }
    versions.finish()?;
    Ok(page)
}
