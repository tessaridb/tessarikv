//! The output tables of a compaction, written one at a time.
//!
//! A table is closed once it reaches the target size, but only between two keys: all versions
//! of a key go to one table, so a sorted level holds each key in at most one table and a point
//! read finds it by one binary search. A range delete goes to the table being written when the
//! merge reaches its start (or to the last table), so every table's range covers the starts it
//! holds and the tables of a level stay disjoint.

use std::path::PathBuf;
use std::sync::Arc;

use tessarikv_format::{TableBuilder, TableMeta, TableOptions};
use tessarikv_types::{Trailer, ValueKind, compare_internal, encode_internal};

use crate::db::block_cache::BlockCache;
use crate::db::flush_job::{FileNumbers, table_path};
use crate::db::rate_limiter::RateLimiter;
use crate::db::table_blocks::open_table;
use crate::db::tables::{finish, format_error, hand_off, user_key};
use crate::db::view::TableFile;
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
}

/// The tables written so far and the one being written.
pub(super) struct Outputs<'a> {
    to: &'a Destination,
    /// Range deletes not yet written, by start, newest first among equal starts.
    tombstones: std::vec::IntoIter<(Vec<u8>, Vec<u8>)>,
    next_tombstone: Option<(Vec<u8>, Vec<u8>)>,
    open: Option<Open>,
    written: Vec<TableFile>,
}

/// The table being written, its file, and the range and sequences it holds.
struct Open {
    builder: TableBuilder,
    id: u64,
    path: PathBuf,
    /// Receives the table's bytes as they are built.
    file: Box<dyn WritableFile>,
    smallest: Option<Vec<u8>>,
    largest: Vec<u8>,
    min_sequence: u64,
    max_sequence: u64,
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
        }
    }

    /// Writes the version at `key`, an internal key; `starts_key` says it is the first version
    /// of its user key, the only place a table may be closed.
    pub(super) fn add(&mut self, key: &[u8], starts_key: bool, value: &[u8]) -> Result<()> {
        let full = self.open.as_ref().is_some_and(|open| {
            u64::try_from(open.builder.estimated_size()).unwrap_or(u64::MAX) >= self.to.target_bytes
        });
        if starts_key && full {
            self.close()?;
        }
        self.write_tombstones(Some(user_key(key)))?;
        let to = self.to;
        self.current()?.add(to, key, value, true)
    }

    /// Writes the remaining range deletes and closes the last table; returns every table.
    pub(super) fn finish(mut self) -> Result<Vec<TableFile>> {
        self.write_tombstones(None)?;
        self.close()?;
        Ok(self.written)
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
            let id = self.to.numbers.take()?;
            let path = table_path(&self.to.dir, id);
            let file = self.to.storage.create(&path)?;
            self.open = Some(Open {
                builder: TableBuilder::new(self.to.options, compare_internal, user_key),
                id,
                path,
                file,
                smallest: None,
                largest: Vec::new(),
                min_sequence: u64::MAX,
                max_sequence: 0,
            });
        }
        self.open.as_mut().ok_or(Error::CompactionFailed)
    }

    /// Writes and syncs the table being written, if any, and opens it for reading.
    fn close(&mut self) -> Result<()> {
        let Some(open) = self.open.take() else {
            return Ok(());
        };
        let to = self.to;
        let Some(smallest) = open.smallest else {
            return Ok(());
        };
        let Open {
            builder,
            id,
            path,
            mut file,
            largest,
            min_sequence,
            max_sequence,
            ..
        } = open;
        let (tail_offset, tail) = finish(builder, &mut |piece| write(to, file.as_mut(), piece))?;
        file.sync()?;
        drop(file);
        let size = tail_offset.saturating_add(u64::try_from(tail.len()).unwrap_or(u64::MAX));
        let meta = TableMeta {
            keyspace: to.keyspace,
            level: to.level,
            id,
            size,
            smallest,
            largest,
            min_sequence,
            max_sequence,
        };
        let written = Some((tail_offset, tail.as_slice()));
        let (table, blocks) = open_table(to.storage.as_ref(), &path, id, &to.cache, written)?;
        self.written.push(TableFile {
            meta: Arc::new(meta),
            table,
            blocks,
        });
        Ok(())
    }
}

/// Appends `piece` of an output table to `file`, paced when compactions are.
fn write(to: &Destination, file: &mut dyn WritableFile, piece: &[u8]) -> Result<()> {
    if let Some(limiter) = &to.limiter {
        limiter.pace(u64::try_from(piece.len()).unwrap_or(u64::MAX));
    }
    file.append(piece)
}

impl Open {
    /// Adds a version (`point`) or a range delete's start and end, widening the recorded range,
    /// and writes the table's finished bytes once they make a chunk.
    fn add(&mut self, to: &Destination, key: &[u8], value: &[u8], point: bool) -> Result<()> {
        if point {
            self.builder.add(key, value)
        } else {
            self.builder.add_range_delete(key, value)
        }
        .map_err(|error| format_error(&error, None))?;
        let file = self.file.as_mut();
        hand_off(&mut self.builder, &mut |piece| write(to, file, piece))?;
        let sequence = tessarikv_types::InternalKeyRef::parse(key)
            .map_err(|_| Error::CompactionFailed)?
            .trailer()
            .sequence()
            .get();
        self.min_sequence = self.min_sequence.min(sequence);
        self.max_sequence = self.max_sequence.max(sequence);
        if self
            .smallest
            .as_deref()
            .is_none_or(|least| compare_internal(key, least).is_lt())
        {
            self.smallest = Some(key.to_vec());
        }
        if self.largest.is_empty() || compare_internal(key, &self.largest).is_gt() {
            self.largest.clear();
            self.largest.extend_from_slice(key);
        }
        Ok(())
    }
}
