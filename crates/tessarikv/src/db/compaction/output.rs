//! The output tables of a compaction, written one at a time.
//!
//! A table is closed once it reaches the target size, but only between two keys: all versions
//! of a key go to one table, so a sorted level holds each key in at most one table and a point
//! read finds it by one binary search. A range delete goes to the table being written when the
//! merge reaches its start (or to the last table), so every table's range covers the starts it
//! holds and the tables of a level stay disjoint.

use std::path::PathBuf;
use std::sync::Arc;

use tessarikv_format::TableOptions;
use tessarikv_types::{InternalKeyRef, Trailer, ValueKind, compare_internal, encode_internal};

use super::output_table::Open;
use crate::db::tables::blobs::{BlobFile, blob_path};

use crate::db::block_cache::BlockCache;
use crate::db::flush_job::{FileNumbers, table_path};
use crate::db::rate_limiter::RateLimiter;
use crate::db::tables::user_key;
use crate::db::view::TableFile;
use crate::statistics::{Statistics, Ticker};
use crate::storage::{Storage, WritableFile};
use crate::tombstones::RangeTombstone;
use crate::{Error, Result};

/// Where and how a compaction writes its tables.
pub(crate) struct Destination {
    pub(crate) storage: Arc<dyn Storage>,
    pub(crate) dir: PathBuf,
    pub(crate) options: TableOptions,
    pub(crate) target_bytes: u64,
    pub(crate) numbers: Arc<FileNumbers>,
    pub(crate) cache: Arc<BlockCache>,
    pub(crate) keyspace: u32,
    pub(crate) level: u8,
    /// Paces the output files' writes, when compactions are paced.
    pub(crate) limiter: Option<Arc<RateLimiter>>,
    /// Size from which a value the job writes moves to a blob file.
    pub(crate) blob_threshold: Option<u64>,
}

/// The tables written so far and the one being written.
pub(super) struct Outputs<'a> {
    to: &'a Destination,
    /// Range deletes not yet written, by start, newest first among equal starts.
    tombstones: std::vec::IntoIter<(Vec<u8>, Vec<u8>)>,
    next_tombstone: Option<(Vec<u8>, Vec<u8>)>,
    open: Option<Open>,
    written: Vec<TableFile>,
    /// Every table and blob file created, so a failed compaction can remove them.
    created: Vec<u64>,
    created_blobs: Vec<u64>,
    /// The key of a version written under a kind of its own.
    scratch: Vec<u8>,
}

impl<'a> Outputs<'a> {
    /// No table yet; `tombstones` are the range deletes of every input.
    pub(super) fn new(to: &'a Destination, tombstones: &[RangeTombstone]) -> Self {
        let mut starts: Vec<(Vec<u8>, Vec<u8>)> = tombstones
            .iter()
            .map(|tombstone| {
                let mut start = Vec::new();
                encode_internal(
                    &tombstone.start,
                    Trailer::new(tombstone.sequence, ValueKind::RangeDelete),
                    &mut start,
                );
                (start, tombstone.end.clone())
            })
            .collect();
        starts.sort_by(|left, right| compare_internal(&left.0, &right.0));
        starts.dedup_by(|later, earlier| later.0 == earlier.0);
        let mut tombstones = starts.into_iter();
        let next_tombstone = tombstones.next();
        Self {
            to,
            tombstones,
            next_tombstone,
            open: None,
            written: Vec::new(),
            created: Vec::new(),
            created_blobs: Vec::new(),
            scratch: Vec::new(),
        }
    }

    /// Writes the version at `key`, an internal key; `starts_key` says it is the first version
    /// of its user key, the only place a table may be closed.
    pub(super) fn add(&mut self, key: &[u8], starts_key: bool, value: &[u8]) -> Result<()> {
        let to = self.to;
        self.ready(key, starts_key)?.add(to, key, value, true)
    }

    /// Writes `version` as a value: into the table's blob file when it is large enough, into
    /// the table otherwise. `stored` is its key as an input stores it.
    pub(super) fn add_value(
        &mut self,
        stored: &[u8],
        version: &InternalKeyRef<'_>,
        starts_key: bool,
        value: &[u8],
    ) -> Result<()> {
        let to = self.to;
        let mut scratch = std::mem::take(&mut self.scratch);
        let added = self
            .ready(stored, starts_key)
            .and_then(|open| open.add_value(to, version, value, &mut scratch));
        self.scratch = scratch;
        added
    }

    /// Writes the version at `key` whose stored `index` points into `blob`, as it is.
    pub(super) fn add_carried(
        &mut self,
        key: &[u8],
        starts_key: bool,
        index: &[u8],
        blob: &Arc<BlobFile>,
    ) -> Result<()> {
        let to = self.to;
        self.ready(key, starts_key)?
            .add_carried(to, key, index, blob)
    }

    /// The table a version at `key` goes to: a new one when the last is full and `starts_key`
    /// lets it close, after the range deletes starting at or before the key.
    fn ready(&mut self, key: &[u8], starts_key: bool) -> Result<&mut Open> {
        let full = self.open.as_ref().is_some_and(|open| {
            u64::try_from(open.estimated_size()).unwrap_or(u64::MAX) >= self.to.target_bytes
        });
        if starts_key && full {
            self.close()?;
        }
        self.write_tombstones(Some(user_key(key)))?;
        self.current()
    }

    /// Writes the remaining range deletes and closes the last table; returns every table, or
    /// removes them all when that fails.
    pub(super) fn finish(mut self) -> Result<Vec<TableFile>> {
        match self.write_tombstones(None).and_then(|()| self.close()) {
            Ok(()) => Ok(std::mem::take(&mut self.written)),
            Err(error) => {
                self.discard();
                Err(error)
            }
        }
    }

    /// Removes every table file this compaction created: no manifest edit will name them.
    pub(super) fn discard(mut self) {
        self.written.clear();
        if let Some(open) = self.open.take() {
            open.discard();
        }
        remove(self.to, &self.created);
        remove_blobs(self.to, &self.created_blobs);
    }

    /// Writes the range deletes starting at or before `through` (all of them for `None`).
    fn write_tombstones(&mut self, through: Option<&[u8]>) -> Result<()> {
        while let Some((start, end)) = self
            .next_tombstone
            .take_if(|(start, _)| through.is_none_or(|through| user_key(start) <= through))
        {
            let to = self.to;
            self.current()?.add(to, &start, &end, false)?;
            self.next_tombstone = self.tombstones.next();
        }
        Ok(())
    }

    /// The table being written, opened with its file when there is none.
    fn current(&mut self) -> Result<&mut Open> {
        if self.open.is_none() {
            let open = Open::create(self.to)?;
            self.created.push(open.id);
            self.open = Some(open);
        }
        self.open.as_mut().ok_or(Error::CompactionFailed)
    }

    /// Writes and syncs the table being written, if any, and opens it for reading.
    fn close(&mut self) -> Result<()> {
        let Some(open) = self.open.take() else {
            return Ok(());
        };
        self.created_blobs.extend(open.blob_id());
        if let Some(table) = open.close(self.to)? {
            self.written.push(table);
        }
        Ok(())
    }
}

/// Removes the output tables `tables`, written by a compaction that failed, and the blob files
/// they hold that are not among `inputs`, the blob files the job read.
pub(super) fn discard(to: &Destination, tables: Vec<TableFile>, inputs: &[Arc<BlobFile>]) {
    let ids: Vec<u64> = tables.iter().map(|table| table.meta.id).collect();
    let mut blobs: Vec<u64> = tables
        .iter()
        .flat_map(|table| table.blobs.iter().map(|blob| blob.id()))
        .filter(|id| !inputs.iter().any(|input| input.id() == *id))
        .collect();
    blobs.dedup();
    // The files are closed before they are removed.
    drop(tables);
    remove(to, &ids);
    remove_blobs(to, &blobs);
}

/// Removes the blob files `ids`; a removal that fails leaves a file no table names, which the
/// next open removes.
fn remove_blobs(to: &Destination, ids: &[u64]) {
    for &id in ids {
        let _ = to.storage.remove(&blob_path(&to.dir, id));
    }
}

/// Removes the table files `ids` from the cache and the disk. A removal that fails leaves a file
/// no manifest edit names, which the next open removes.
fn remove(to: &Destination, ids: &[u64]) {
    for &id in ids {
        to.cache.forget_file(id);
        let _ = to.storage.remove(&table_path(&to.dir, id));
    }
}

/// Appends `piece` of an output table to `file`, paced when compactions are.
pub(super) fn write(to: &Destination, file: &mut dyn WritableFile, piece: &[u8]) -> Result<()> {
    if let Some(limiter) = &to.limiter {
        limiter.pace(u64::try_from(piece.len()).unwrap_or(u64::MAX));
    }
    file.append(piece)
}

/// Counts a compaction of `inputs` into `outputs`: the bytes it read and wrote, and the entries
/// it left out.
pub(super) fn count(statistics: &Statistics, inputs: &[TableFile], outputs: &[TableFile]) {
    let bytes = |files: &[TableFile]| {
        files
            .iter()
            .fold(0_u64, |total, file| total.saturating_add(file.meta.size))
    };
    let entries = |files: &[TableFile]| {
        files.iter().fold(0_u64, |total, file| {
            total.saturating_add(file.table.entries())
        })
    };
    statistics.record(Ticker::CompactionBytesRead, bytes(inputs));
    statistics.record(Ticker::CompactionBytesWritten, bytes(outputs));
    let dropped = entries(inputs).saturating_sub(entries(outputs));
    statistics.record(Ticker::CompactionKeysDropped, dropped);
}
