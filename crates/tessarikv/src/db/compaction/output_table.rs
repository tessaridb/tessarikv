//! One output table of a compaction while it is written, with the blob file its large values
//! move into and the input blob files its carried indexes point into.

use std::path::PathBuf;
use std::sync::Arc;

use tessarikv_format::{TableBuilder, TableMeta};
use tessarikv_types::{InternalKeyRef, Trailer, ValueKind, compare_internal, encode_internal};

use super::output::{Destination, write};
use crate::db::flush_job::table_path;
use crate::db::table_blocks::open_table;
use crate::db::tables::blobs::{BlobFile, BlobWriter, among};
use crate::db::tables::{finish, format_error, hand_off, table_builder};
use crate::db::view::TableFile;
use crate::storage::WritableFile;
use crate::{Error, Result};

/// The table being written, its file, its blob file, and the range and sequences it holds.
pub(super) struct Open {
    builder: TableBuilder,
    pub(super) id: u64,
    path: PathBuf,
    /// Receives the table's bytes as they are built.
    file: Box<dyn WritableFile>,
    blobs: BlobWriter,
    /// The input blob files the table's carried indexes point into.
    carried: Vec<Arc<BlobFile>>,
    pub(super) smallest: Option<Vec<u8>>,
    largest: Vec<u8>,
    min_sequence: u64,
    max_sequence: u64,
}

impl Open {
    /// A new table of `to`, and its file.
    pub(super) fn create(to: &Destination) -> Result<Self> {
        let id = to.numbers.take()?;
        let path = table_path(&to.dir, id);
        let file = to.storage.create(&path)?;
        let blobs = BlobWriter::new(
            Arc::clone(&to.storage),
            &to.dir,
            Arc::clone(&to.numbers),
            Arc::clone(&to.cache),
            to.blob_threshold,
            to.options.compression,
        )
        .paced(to.limiter.clone());
        Ok(Self {
            builder: table_builder(to.options, to.cache.sealing())?,
            id,
            path,
            file,
            blobs,
            carried: Vec::new(),
            smallest: None,
            largest: Vec::new(),
            min_sequence: u64::MAX,
            max_sequence: 0,
        })
    }

    /// Adds `value` as version `version` of its key: into the blob file when it is large enough
    /// to move there, into the table otherwise. `scratch` holds the key written.
    pub(super) fn add_value(
        &mut self,
        to: &Destination,
        version: &InternalKeyRef<'_>,
        value: &[u8],
        scratch: &mut Vec<u8>,
    ) -> Result<()> {
        scratch.clear();
        let sequence = version.trailer().sequence();
        if self.blobs.separates(value.len()) {
            let index = self.blobs.add(value)?;
            self.builder.references_blob(index.file);
            encode_internal(
                version.user_key(),
                Trailer::new(sequence, ValueKind::BlobIndex),
                scratch,
            );
            self.add(to, scratch, &index.encode(), true)
        } else {
            encode_internal(
                version.user_key(),
                Trailer::new(sequence, ValueKind::Value),
                scratch,
            );
            self.add(to, scratch, value, true)
        }
    }

    /// Adds the version at `key` whose stored `index` points into `blob`, an input's blob file.
    pub(super) fn add_carried(
        &mut self,
        to: &Destination,
        key: &[u8],
        index: &[u8],
        blob: &Arc<BlobFile>,
    ) -> Result<()> {
        self.builder.references_blob(blob.id());
        if !self.carried.iter().any(|held| held.id() == blob.id()) {
            self.carried.push(Arc::clone(blob));
        }
        self.add(to, key, index, true)
    }

    /// Adds a version (`point`) or a range delete's start and end, widening the recorded range,
    /// and writes the table's finished bytes once they make a chunk.
    pub(super) fn add(
        &mut self,
        to: &Destination,
        key: &[u8],
        value: &[u8],
        point: bool,
    ) -> Result<()> {
        if point {
            self.builder.add(key, value)
        } else {
            self.builder.add_range_delete(key, value)
        }
        .map_err(|error| format_error(&error, None))?;
        let file = self.file.as_mut();
        hand_off(&mut self.builder, &mut |piece| write(to, file, piece))?;
        let sequence = InternalKeyRef::parse(key)
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

    /// Bytes the table holds so far, by the builder's estimate.
    pub(super) fn estimated_size(&self) -> usize {
        self.builder.estimated_size()
    }

    /// The number of the table's blob file, once a value moved there.
    pub(super) fn blob_id(&self) -> Option<u64> {
        self.blobs.id()
    }

    /// Writes and syncs the blob file and the table, and opens the table for reading; `None`
    /// for a table that received nothing.
    pub(super) fn close(self, to: &Destination) -> Result<Option<TableFile>> {
        let Self {
            builder,
            id,
            path,
            mut file,
            blobs,
            mut carried,
            smallest,
            largest,
            min_sequence,
            max_sequence,
        } = self;
        let Some(smallest) = smallest else {
            return Ok(None);
        };
        // Both are synced before the edit naming the table.
        carried.extend(blobs.finish()?);
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
        let blobs = among(table.blob_files(), &carried)?;
        Ok(Some(
            TableFile::new(Arc::new(meta), table, blocks)?.with_blobs(blobs)?,
        ))
    }

    /// Drops the table unfinished and removes its blob file; the caller removes the table.
    pub(super) fn discard(self) {
        self.blobs.discard();
    }
}
