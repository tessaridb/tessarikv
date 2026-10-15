//! Reading and writing batch entries; every read is bounds-checked.

use tessarikv_constants::{MAX_KEY_BYTES, MAX_VALUE_BYTES};
use tessarikv_types::{KeyspaceId, ValueKind};

use super::{BatchEntry, FORMAT_VERSION_1, check_size, empty_range};
use crate::{Error, Result};

/// Bytes of an entry with `key` and, for a put, `value`.
pub(super) fn entry_bytes(key: &[u8], value: Option<&[u8]>) -> Option<usize> {
    // kind + keyspace + key length, then the value length when there is a value.
    let fixed = 1 + 4 + 4;
    let entry = key.len().checked_add(fixed)?;
    match value {
        Some(value) => entry.checked_add(4)?.checked_add(value.len()),
        None => Some(entry),
    }
}

/// Appends one entry; the caller has checked every bound.
pub(super) fn write_entry(
    rep: &mut Vec<u8>,
    keyspace: KeyspaceId,
    kind: ValueKind,
    key: &[u8],
    value: Option<&[u8]>,
) -> Result<()> {
    rep.push(kind.to_byte());
    rep.extend_from_slice(&keyspace.get().to_le_bytes());
    rep.extend_from_slice(&length_field("key", key.len())?.to_le_bytes());
    rep.extend_from_slice(key);
    if let Some(value) = value {
        rep.extend_from_slice(&length_field("value", value.len())?.to_le_bytes());
        rep.extend_from_slice(value);
    }
    Ok(())
}

/// Decodes the entry starting at `at` of a batch of encoding `version`; returns it and where
/// the next one starts.
pub(super) fn read_entry(rep: &[u8], at: usize, version: u8) -> Result<(BatchEntry<'_>, usize)> {
    let kind_byte = read_u8(rep, at, "write batch entry")?;
    let kind = match ValueKind::from_byte(kind_byte) {
        Ok(kind @ (ValueKind::Value | ValueKind::Delete)) => kind,
        Ok(ValueKind::RangeDelete) if version != FORMAT_VERSION_1 => ValueKind::RangeDelete,
        _ => {
            return Err(Error::Malformed {
                what: "write batch entry kind",
                reason: "not a put, a delete or a range delete of this version",
            });
        }
    };
    let keyspace = KeyspaceId::new(read_u32(rep, offset(at, 1)?, "write batch entry")?);
    let (key, mut next) = read_bytes(rep, offset(at, 5)?, "key", MAX_KEY_BYTES)?;
    let value = match kind {
        ValueKind::Value => {
            let (value, after) = read_bytes(rep, next, "value", MAX_VALUE_BYTES)?;
            next = after;
            Some(value)
        }
        ValueKind::RangeDelete => {
            let (end, after) = read_bytes(rep, next, "range end", MAX_KEY_BYTES)?;
            if key >= end {
                return Err(empty_range());
            }
            next = after;
            Some(end)
        }
        ValueKind::Delete => None,
    };
    Ok((
        BatchEntry {
            keyspace,
            kind,
            key,
            value,
        },
        next,
    ))
}

/// A length-prefixed byte string at `at`, at most `limit` bytes long.
fn read_bytes<'a>(
    rep: &'a [u8],
    at: usize,
    what: &'static str,
    limit: usize,
) -> Result<(&'a [u8], usize)> {
    let len = usize::try_from(read_u32(rep, at, what)?).map_err(|_| Error::Malformed {
        what,
        reason: "length does not fit this platform",
    })?;
    check_size(what, len, limit)?;
    let start = offset(at, 4)?;
    let end = offset(start, len)?;
    let bytes = rep
        .get(start..end)
        .ok_or_else(|| truncated(what, end, rep.len()))?;
    Ok((bytes, end))
}

/// The byte at `at`.
pub(super) fn read_u8(rep: &[u8], at: usize, what: &'static str) -> Result<u8> {
    rep.get(at)
        .copied()
        .ok_or_else(|| truncated(what, offset(at, 1).unwrap_or(usize::MAX), rep.len()))
}

/// The little-endian `u32` at `at`.
pub(super) fn read_u32(rep: &[u8], at: usize, what: &'static str) -> Result<u32> {
    let end = offset(at, 4)?;
    let bytes = rep
        .get(at..end)
        .and_then(|bytes| <[u8; 4]>::try_from(bytes).ok())
        .ok_or_else(|| truncated(what, end, rep.len()))?;
    Ok(u32::from_le_bytes(bytes))
}

/// `at + len`, refused when it overflows.
fn offset(at: usize, len: usize) -> Result<usize> {
    at.checked_add(len).ok_or(Error::Malformed {
        what: "write batch",
        reason: "offset overflows",
    })
}

/// A length as the `u32` field that stores it.
fn length_field(what: &'static str, len: usize) -> Result<u32> {
    u32::try_from(len).map_err(|_| Error::TooLarge {
        what,
        size: u64::try_from(len).unwrap_or(u64::MAX),
        limit: u64::from(u32::MAX),
    })
}

fn truncated(what: &'static str, needed: usize, available: usize) -> Error {
    Error::Truncated {
        what,
        needed: u64::try_from(needed).unwrap_or(u64::MAX),
        available: u64::try_from(available).unwrap_or(u64::MAX),
    }
}
