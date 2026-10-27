//! Table files as the database uses them: built from a frozen in-memory table and searched for
//! the newest version of a key at a snapshot.
//!
//! A table holds every version of its in-memory table under its internal key, so a table answers
//! a read at any snapshot exactly as the in-memory table did; versions are dropped only by
//! compaction, which knows the snapshots readers hold. A value large enough for its keyspace's
//! blob threshold lives in a blob file beside the table, which keeps an index to it (see
//! `blobs`).

pub(crate) mod blobs;
mod lookup;

use std::path::Path;

use tessarikv_constants::{TABLE_WRITE_CHUNK_BYTES, TRAILER_BYTES};
use tessarikv_format::{FileKind, Table, TableBuilder, TableOptions};
use tessarikv_types::{InternalKeyRef, Trailer, ValueKind, compare_internal, encode_internal};

use blobs::BlobWriter;
pub(crate) use lookup::{TableLookup, lookup};

use crate::memtable::Memtable;
use crate::sealing::Sealing;
use crate::tombstones::RangeTombstone;
use crate::{Error, Result};

/// A table file built from an in-memory table, and what the manifest records about it.
pub(crate) struct BuiltTable {
    /// The last bytes written: the filter, the index, the properties and the footer, which
    /// opening the table reads.
    pub(crate) tail: Vec<u8>,
    /// Where `tail` begins in the file.
    pub(crate) tail_offset: u64,
    pub(crate) smallest: Vec<u8>,
    pub(crate) largest: Vec<u8>,
    pub(crate) min_sequence: u64,
    pub(crate) max_sequence: u64,
}

impl BuiltTable {
    /// Bytes of the whole file.
    pub(crate) fn size(&self) -> u64 {
        self.tail_offset
            .saturating_add(u64::try_from(self.tail.len()).unwrap_or(u64::MAX))
    }
}

/// Hands `builder`'s finished bytes to `write` once they make a chunk, so a table being built
/// holds at most a chunk of itself in memory.
pub(crate) fn hand_off(
    builder: &mut TableBuilder,
    write: &mut dyn FnMut(&[u8]) -> Result<()>,
) -> Result<()> {
    let drained = usize::try_from(builder.drained()).unwrap_or(usize::MAX);
    if builder.estimated_size().saturating_sub(drained) >= TABLE_WRITE_CHUNK_BYTES {
        write(&builder.take_finished())?;
    }
    Ok(())
}

/// Finishes `builder`, hands the rest of the table to `write`, and returns that rest and where
/// it begins in the file.
pub(crate) fn finish(
    builder: TableBuilder,
    write: &mut dyn FnMut(&[u8]) -> Result<()>,
) -> Result<(u64, Vec<u8>)> {
    let tail_offset = builder.drained();
    let tail = builder
        .finish()
        .map_err(|error| format_error(&error, None))?;
    write(&tail)?;
    Ok((tail_offset, tail))
}

/// The user key of an internal key: everything before the trailer.
pub(crate) fn user_key(internal: &[u8]) -> &[u8] {
    internal
        .get(..internal.len().saturating_sub(TRAILER_BYTES))
        .unwrap_or(internal)
}

/// A builder of a table file laid out by `options`, encrypted under a key of its own when the
/// database's tables are (`sealing`).
pub(crate) fn table_builder(
    options: TableOptions,
    sealing: Option<&Sealing>,
) -> Result<TableBuilder> {
    Ok(match sealing {
        Some(sealing) => {
            let (header, sealer) = sealing.new_file(FileKind::Table)?;
            TableBuilder::sealed(options, compare_internal, user_key, sealer, &header)
        }
        None => TableBuilder::new(options, compare_internal, user_key),
    })
}

/// Writes every version and range delete of `memtable` as a table file, handing its bytes to
/// `write` in order as they are built, and every value `blobs` (when given) takes to its blob
/// file. Runs on
/// a blocking thread. The recorded key range spans the point entries and the starts of the range
/// deletes.
///
/// # Errors
///
/// [`Error::Memtable`] when the table holds neither a version nor a range delete (an empty
/// table is never flushed), and
/// [`Error::Corruption`] when the format refuses an entry, which means the table's order broke.
pub(crate) fn build(
    memtable: &Memtable,
    options: TableOptions,
    sealing: Option<&Sealing>,
    mut blobs: Option<&mut BlobWriter>,
    write: &mut dyn FnMut(&[u8]) -> Result<()>,
) -> Result<BuiltTable> {
    let mut builder = table_builder(options, sealing)?;
    let mut versions = memtable.iter();
    versions.seek_to_first();
    let mut key = Vec::new();
    let mut smallest = None;
    let (mut min_sequence, mut max_sequence) = (u64::MAX, 0);
    while let Some(entry) = versions.entry() {
        key.clear();
        let index = match blobs.as_deref_mut() {
            Some(blobs) if entry.kind == ValueKind::Value && blobs.separates(entry.value.len()) => {
                Some(blobs.add(entry.value)?)
            }
            _ => None,
        };
        let kind = if index.is_some() {
            ValueKind::BlobIndex
        } else {
            entry.kind
        };
        encode_internal(entry.user_key, Trailer::new(entry.sequence, kind), &mut key);
        let added = match index {
            Some(index) => {
                builder.references_blob(index.file);
                builder.add(&key, &index.encode())
            }
            None => builder.add(&key, entry.value),
        };
        added.map_err(|error| format_error(&error, None))?;
        hand_off(&mut builder, write)?;
        if smallest.is_none() {
            smallest = Some(key.clone());
        }
        min_sequence = min_sequence.min(entry.sequence.get());
        max_sequence = max_sequence.max(entry.sequence.get());
        versions.next();
    }
    let mut largest = smallest.is_some().then_some(key);
    let mut tombstones = Vec::new();
    memtable.tombstones(&mut tombstones);
    for tombstone in &tombstones {
        let mut start = Vec::new();
        encode_internal(
            &tombstone.start,
            Trailer::new(tombstone.sequence, ValueKind::RangeDelete),
            &mut start,
        );
        builder
            .add_range_delete(&start, &tombstone.end)
            .map_err(|error| format_error(&error, None))?;
        min_sequence = min_sequence.min(tombstone.sequence.get());
        max_sequence = max_sequence.max(tombstone.sequence.get());
        if smallest
            .as_deref()
            .is_none_or(|least| compare_internal(&start, least).is_lt())
        {
            smallest = Some(start.clone());
        }
        if largest
            .as_deref()
            .is_none_or(|most| compare_internal(&start, most).is_gt())
        {
            largest = Some(start);
        }
    }
    let (Some(smallest), Some(largest)) = (smallest, largest) else {
        return Err(Error::Memtable {
            reason: "an empty in-memory table was asked for a table file",
        });
    };
    let (tail_offset, tail) = finish(builder, write)?;
    Ok(BuiltTable {
        tail,
        tail_offset,
        smallest,
        largest,
        min_sequence,
        max_sequence,
    })
}

/// The range deletes of `table`, appended to `out`.
///
/// # Errors
///
/// [`Error::Corruption`] for a damaged range-delete block or a stored start that is not an
/// internal key.
pub(crate) fn tombstones(table: &Table, out: &mut Vec<RangeTombstone>) -> Result<()> {
    for (start, end) in table
        .range_deletes()
        .map_err(|error| format_error(&error, None))?
    {
        let parsed = InternalKeyRef::parse(&start).map_err(|_| Error::Corruption {
            what: "table",
            path: None,
            offset: None,
            detail: "a range delete's start is not an internal key".to_owned(),
        })?;
        out.push(RangeTombstone {
            start: parsed.user_key().to_vec(),
            end,
            sequence: parsed.trailer().sequence(),
        });
    }
    Ok(())
}

/// The engine error for a table the format refused, read from `path` when it came from a file.
pub(super) fn format_error(error: &tessarikv_format::Error, path: Option<&Path>) -> Error {
    Error::Corruption {
        what: "table",
        path: path.map(Path::to_path_buf),
        offset: None,
        detail: error.to_string(),
    }
}
