//! Tables built outside a database, to be added to one whole by [`crate::Db::ingest`].

#[cfg(test)]
mod tests;

use tessarikv_constants::{self as limits, MAX_KEY_BYTES, MAX_VALUE_BYTES};
use tessarikv_format::{Compression, FilterPolicy, Table, TableBuilder, TableOptions};
use tessarikv_types::{
    InternalKeyRef, SequenceNumber, Trailer, ValueKind, compare_internal, encode_internal,
};

use crate::{Error, Result};

/// Writes a table of puts and deletes for [`crate::Db::ingest`], keys in strictly increasing
/// byte order. The table is a transport: an ingest writes its entries again under the target
/// keyspace's options, so the table needs none of them.
///
/// ```
/// let mut table = tessarikv::TableWriter::new();
/// table.put(b"apple", b"red")?;
/// table.delete(b"banana")?;
/// let bytes = table.finish()?;
/// # assert!(!bytes.is_empty());
/// # Ok::<(), tessarikv::Error>(())
/// ```
#[derive(Debug)]
pub struct TableWriter {
    builder: TableBuilder,
    last: Option<Vec<u8>>,
    internal: Vec<u8>,
}

impl Default for TableWriter {
    fn default() -> Self {
        Self::new()
    }
}

impl TableWriter {
    /// An empty table.
    #[must_use]
    pub fn new() -> Self {
        let options = TableOptions {
            block_bytes: usize::try_from(limits::DEFAULT_BLOCK_BYTES).unwrap_or(4096),
            restart_interval: usize::try_from(limits::DEFAULT_BLOCK_RESTART_INTERVAL).unwrap_or(16),
            bloom_bits_per_key: 1,
            filter: FilterPolicy::Bloom,
            compression: Compression::Lz4,
            prefix_length: None,
        };
        Self {
            builder: TableBuilder::new(options, compare_internal, user_key),
            last: None,
            internal: Vec::new(),
        }
    }

    /// Adds `value` for `key`.
    ///
    /// # Errors
    ///
    /// [`Error::InvalidArgument`] when `key` is not above the previous key or is empty, and
    /// [`Error::TooLarge`] when the key or the value passes its limit.
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        if value.len() > MAX_VALUE_BYTES {
            return Err(too_large("an ingested value", value.len(), MAX_VALUE_BYTES));
        }
        self.add(key, ValueKind::Value, value)
    }

    /// Adds a delete of `key`: the ingest hides every older version of it.
    ///
    /// # Errors
    ///
    /// As [`TableWriter::put`].
    pub fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.add(key, ValueKind::Delete, &[])
    }

    /// The table's bytes.
    ///
    /// # Errors
    ///
    /// [`Error::InvalidArgument`] when it holds no entry.
    pub fn finish(self) -> Result<Vec<u8>> {
        if self.last.is_none() {
            return Err(refused("an ingested table holds no entry"));
        }
        self.builder
            .finish()
            .map_err(|_| refused("an ingested table could not be written"))
    }

    fn add(&mut self, key: &[u8], kind: ValueKind, value: &[u8]) -> Result<()> {
        if key.is_empty() {
            return Err(refused("an ingested key is empty"));
        }
        if key.len() > MAX_KEY_BYTES {
            return Err(too_large("an ingested key", key.len(), MAX_KEY_BYTES));
        }
        if self.last.as_deref().is_some_and(|last| key <= last) {
            return Err(refused("ingested keys are not strictly increasing"));
        }
        self.internal.clear();
        encode_internal(
            key,
            Trailer::new(SequenceNumber::ZERO, kind),
            &mut self.internal,
        );
        self.builder
            .add(&self.internal, value)
            .map_err(|_| refused("an ingested table could not be written"))?;
        self.last = Some(key.to_vec());
        Ok(())
    }
}

/// One entry of an ingested table: its key, whether it is a put, and its value.
pub(crate) type Entry = (Vec<u8>, ValueKind, Vec<u8>);

/// The entries of a table [`TableWriter`] wrote, read as hostile input: every block verified,
/// nothing but puts and deletes at sequence zero, keys strictly increasing.
pub(crate) fn decode(bytes: Vec<u8>) -> Result<Vec<Entry>> {
    let table = Table::open(bytes, compare_internal, user_key)
        .map_err(|_| refused("an ingested table does not decode"))?;
    let undecodable = |_| refused("an ingested table does not decode");
    if !table.range_deletes().map_err(undecodable)?.is_empty() || !table.blob_files().is_empty() {
        return Err(refused(
            "an ingested table holds more than puts and deletes",
        ));
    }
    let mut entries: Vec<Entry> = Vec::new();
    let mut versions = table.iter();
    versions.seek_to_first();
    while versions.valid() {
        let key = InternalKeyRef::parse(versions.key())
            .map_err(|_| refused("an ingested table does not decode"))?;
        let Ok(kind @ (ValueKind::Value | ValueKind::Delete)) = key.trailer().kind() else {
            return Err(refused(
                "an ingested table holds more than puts and deletes",
            ));
        };
        if key.trailer().sequence() != SequenceNumber::ZERO {
            return Err(refused(
                "an ingested table was not written by a table writer",
            ));
        }
        if key.user_key().is_empty()
            || entries
                .last()
                .is_some_and(|(last, _, _)| key.user_key() <= last.as_slice())
        {
            return Err(refused("ingested keys are not strictly increasing"));
        }
        entries.push((key.user_key().to_vec(), kind, versions.value().to_vec()));
        versions.next();
    }
    versions
        .status()
        .map_err(|_| refused("an ingested table does not decode"))?;
    if entries.is_empty() {
        return Err(refused("an ingested table holds no entry"));
    }
    Ok(entries)
}

fn refused(reason: &'static str) -> Error {
    Error::InvalidArgument {
        what: "an ingest",
        reason,
    }
}

fn too_large(what: &'static str, size: usize, limit: usize) -> Error {
    let bytes = |count: usize| u64::try_from(count).unwrap_or(u64::MAX);
    Error::TooLarge {
        what,
        size: bytes(size),
        limit: bytes(limit),
    }
}

fn user_key(internal: &[u8]) -> &[u8] {
    InternalKeyRef::parse(internal).map_or(internal, |key| key.user_key())
}
