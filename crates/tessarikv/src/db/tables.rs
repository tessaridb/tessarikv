//! Table files as the database uses them: built from a frozen in-memory table, opened for
//! reading, and searched for the newest version of a key at a snapshot.
//!
//! A table holds every version of its in-memory table under its internal key, so a table answers
//! a read at any snapshot exactly as the in-memory table did; versions are dropped only by
//! compaction, which knows the snapshots readers hold.

use std::path::Path;

use tessarikv_constants::TRAILER_BYTES;
use tessarikv_format::{Table, TableBuilder, TableOptions};
use tessarikv_types::{
    InternalKeyRef, SequenceNumber, Trailer, ValueKind, compare_internal, encode_internal,
    seek_trailer,
};

use crate::memtable::Memtable;
use crate::{Error, Result};

/// What a table holds for a key at a snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TableLookup {
    /// The newest visible version holds this value.
    Value(Vec<u8>),
    /// The newest visible version is a delete: older tables must not be consulted.
    Deleted,
    /// No version is visible here: older tables decide.
    Absent,
}

/// A table file built from an in-memory table, and what the manifest records about it.
pub(crate) struct BuiltTable {
    pub(crate) bytes: Vec<u8>,
    pub(crate) smallest: Vec<u8>,
    pub(crate) largest: Vec<u8>,
    pub(crate) min_sequence: u64,
    pub(crate) max_sequence: u64,
}

/// The user key of an internal key: everything before the trailer.
fn user_key(internal: &[u8]) -> &[u8] {
    internal
        .get(..internal.len().saturating_sub(TRAILER_BYTES))
        .unwrap_or(internal)
}

/// Writes every version of `memtable` into a table file. Runs on a blocking thread.
///
/// # Errors
///
/// [`Error::Memtable`] when the table holds no version (an empty table is never flushed), and
/// [`Error::Corruption`] when the format refuses an entry, which means the table's order broke.
pub(crate) fn build(memtable: &Memtable, options: TableOptions) -> Result<BuiltTable> {
    let mut builder = TableBuilder::new(options, compare_internal, user_key);
    let mut versions = memtable.iter();
    versions.seek_to_first();
    let mut key = Vec::new();
    let mut smallest = None;
    let (mut min_sequence, mut max_sequence) = (u64::MAX, 0);
    while let Some(entry) = versions.entry() {
        key.clear();
        encode_internal(
            entry.user_key,
            Trailer::new(entry.sequence, entry.kind),
            &mut key,
        );
        builder
            .add(&key, entry.value)
            .map_err(|error| format_error(&error, None))?;
        if smallest.is_none() {
            smallest = Some(key.clone());
        }
        min_sequence = min_sequence.min(entry.sequence.get());
        max_sequence = max_sequence.max(entry.sequence.get());
        versions.next();
    }
    let smallest = smallest.ok_or(Error::Memtable {
        reason: "an empty in-memory table was asked for a table file",
    })?;
    Ok(BuiltTable {
        bytes: builder
            .finish()
            .map_err(|error| format_error(&error, None))?,
        smallest,
        largest: key,
        min_sequence,
        max_sequence,
    })
}

/// Opens the table in `bytes`, read from `path` when it came from a file.
///
/// # Errors
///
/// [`Error::Corruption`] when the bytes are not a whole table of this format.
pub(crate) fn open(bytes: Vec<u8>, path: Option<&Path>) -> Result<Table> {
    Table::open(bytes, compare_internal, user_key).map_err(|error| format_error(&error, path))
}

/// The newest version of `key` in `table` at `snapshot`.
///
/// # Errors
///
/// [`Error::Corruption`] when a block on the way is damaged.
pub(crate) fn lookup(table: &Table, key: &[u8], snapshot: SequenceNumber) -> Result<TableLookup> {
    let mut target = Vec::with_capacity(key.len().saturating_add(TRAILER_BYTES));
    encode_internal(key, seek_trailer(snapshot), &mut target);
    if !table.may_contain(&target) {
        return Ok(TableLookup::Absent);
    }
    let mut versions = table.iter();
    versions.seek(&target);
    versions
        .status()
        .map_err(|error| format_error(&error, None))?;
    if !versions.valid() {
        return Ok(TableLookup::Absent);
    }
    let found = InternalKeyRef::parse(versions.key()).map_err(|_| Error::Corruption {
        what: "table",
        path: None,
        offset: None,
        detail: "a stored key has no trailer".to_owned(),
    })?;
    if found.user_key() != key {
        return Ok(TableLookup::Absent);
    }
    // The seek lands on the newest version at or below the snapshot.
    Ok(match found.trailer().kind() {
        Ok(ValueKind::Value) => TableLookup::Value(versions.value().to_vec()),
        Ok(ValueKind::Delete) => TableLookup::Deleted,
        // Range deletes never reach a table yet: batches refuse them.
        Ok(ValueKind::RangeDelete) | Err(_) => {
            return Err(Error::Corruption {
                what: "table",
                path: None,
                offset: None,
                detail: "a stored key has a kind this build does not read".to_owned(),
            });
        }
    })
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
