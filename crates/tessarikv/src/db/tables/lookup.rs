//! A point read of one table file: the newest version of a key at a snapshot, read through the
//! table's filter and blocks, and from its blob file when the version is there.

use tessarikv_types::{InternalKeyRef, ValueKind};

use super::blobs::locate;
use super::format_error;
use crate::db::lookup_key::LookupKey;
use crate::db::view::TableFile;
use crate::expiry::{Seen, malformed, seen};
use crate::statistics::{Statistics, Ticker};
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

/// The newest version of `key` in `file` at `snapshot`, deleted when below `floor`, keeping the
/// blocks it reads in the cache unless `fill_cache` is false, and counting the filter's answer in
/// `statistics`.
///
/// # Errors
///
/// [`Error::Corruption`] when a block on the way is damaged, and the errors of
/// [`super::table_blocks::TableBlocks::load`].
pub(crate) async fn lookup(
    file: &TableFile,
    key: &LookupKey,
    floor: u64,
    fill_cache: bool,
    statistics: Option<&Statistics>,
) -> Result<TableLookup> {
    let target = key.target();
    let may_contain = file.table.may_contain(target);
    if let Some(statistics) = statistics {
        let ticker = if may_contain {
            Ticker::BloomMaybe
        } else {
            Ticker::BloomUseful
        };
        statistics.record(ticker, 1);
    }
    if !may_contain {
        return Ok(TableLookup::Absent);
    }
    let mut versions = file.table.iter();
    versions.seek(target);
    file.blocks.complete(&mut versions, fill_cache).await?;
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
    if found.user_key() != key.user_key() {
        return Ok(TableLookup::Absent);
    }
    // A range delete at `floor` covers the key: every version below it is deleted.
    if found.trailer().sequence().get() < floor {
        return Ok(TableLookup::Deleted);
    }
    // The seek lands on the newest version at or below the snapshot.
    Ok(match found.trailer().kind() {
        Ok(ValueKind::BlobIndex) => {
            let (blob, handle) = locate(&file.blobs, versions.value())?;
            TableLookup::Value(blob.read(handle).await?)
        }
        Ok(kind @ (ValueKind::Value | ValueKind::ExpiringValue | ValueKind::Delete)) => {
            match seen(kind, versions.value()) {
                Seen::Value(value) => TableLookup::Value(value.to_vec()),
                Seen::Absent => TableLookup::Deleted,
                Seen::Malformed => return Err(malformed()),
            }
        }
        // Range deletes are kept in their own block, never among the versions.
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
