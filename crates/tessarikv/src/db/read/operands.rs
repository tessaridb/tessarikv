//! Point reads of a key whose newest versions are merge operands: its versions are read newest
//! first, across in-memory tables and table files, until the first that is not an operand, and
//! the keyspace's merge operator combines that base with the operands above it.

use std::sync::Arc;

use tessarikv_types::{KeyspaceId, SequenceNumber, ValueKind};

use crate::db::merge::MergedVersions;
use crate::db::table_versions::TableVersions;
use crate::db::view::View;
use crate::expiry::{Seen, malformed, seen};
use crate::tombstones::Deletions;
use crate::{Error, MergeOperator, Result};

/// The operands of one key met so far, newest first.
#[derive(Debug, Default)]
pub(in crate::db) struct Operands {
    newest_first: Vec<Vec<u8>>,
}

/// What a version tells a read gathering operands.
pub(in crate::db) enum Gathered {
    /// An operand: older versions are read too.
    More,
    /// The base below the operands: a value, or `None` for a key absent from that version on.
    Base(Option<Vec<u8>>),
}

impl Operands {
    /// The operands of a walk that met them oldest first.
    pub(in crate::db) fn from_oldest_first(mut operands: Vec<Vec<u8>>) -> Self {
        operands.reverse();
        Self {
            newest_first: operands,
        }
    }

    /// Takes in the version at `sequence` of kind `kind` storing `stored`, every version below
    /// `floor` being deleted.
    pub(in crate::db) fn add(
        &mut self,
        sequence: SequenceNumber,
        kind: ValueKind,
        stored: &[u8],
        floor: u64,
    ) -> Result<Gathered> {
        if sequence.get() < floor {
            return Ok(Gathered::Base(None));
        }
        Ok(match seen(kind, stored) {
            Seen::Operand(operand) => {
                self.newest_first.push(operand.to_vec());
                Gathered::More
            }
            Seen::Value(value) => Gathered::Base(Some(value.to_vec())),
            Seen::Absent => Gathered::Base(None),
            Seen::Malformed => return Err(malformed()),
        })
    }

    /// What `operator` makes of `base` and the operands of `key` in `keyspace`.
    ///
    /// # Errors
    ///
    /// [`Error::MergeFailed`] when there is no operator or it refuses the operands.
    pub(in crate::db) fn combine(
        &self,
        operator: Option<&Arc<dyn MergeOperator>>,
        keyspace: u32,
        key: &[u8],
        base: Option<&[u8]>,
    ) -> Result<Vec<u8>> {
        let operands: Vec<&[u8]> = self.newest_first.iter().rev().map(Vec::as_slice).collect();
        operator
            .and_then(|operator| operator.full_merge(key, base, &operands))
            .ok_or(Error::MergeFailed { keyspace })
    }
}

/// The value of `key` in `keyspace` of `view` at `snapshot`, with every version below `floor`
/// deleted, read by gathering its operands.
pub(super) async fn merged(
    view: &View,
    keyspace: KeyspaceId,
    key: &[u8],
    snapshot: SequenceNumber,
    floor: u64,
    fill_cache: bool,
) -> Result<Option<Vec<u8>>> {
    let operator = operator_of(view, keyspace.get());
    let mut operands = Operands::default();
    let base = base_below(
        view,
        keyspace,
        key,
        snapshot,
        floor,
        fill_cache,
        &mut operands,
    )
    .await?;
    operands
        .combine(operator.as_ref(), keyspace.get(), key, base.as_deref())
        .map(Some)
}

/// Gathers the operands of `key` into `operands` and returns the base below them.
async fn base_below(
    view: &View,
    keyspace: KeyspaceId,
    key: &[u8],
    snapshot: SequenceNumber,
    floor: u64,
    fill_cache: bool,
    operands: &mut Operands,
) -> Result<Option<Vec<u8>>> {
    for table in view.memtables(keyspace.get()) {
        for (sequence, kind, stored) in table.versions(key, snapshot) {
            if let Gathered::Base(base) = operands.add(sequence, kind, stored, floor)? {
                return Ok(base);
            }
        }
    }
    for file in view.candidates(keyspace.get(), key) {
        let mut versions = TableVersions::new(file, fill_cache);
        versions.seek(key, snapshot).await;
        while let Some(version) = versions.entry() {
            if version.user_key != key {
                break;
            }
            let gathered = operands.add(version.sequence, version.kind, version.value, floor)?;
            if let Gathered::Base(base) = gathered {
                return Ok(base);
            }
            versions.next().await;
        }
        if let Some(error) = versions.take_error() {
            return Err(error);
        }
    }
    Ok(None)
}

/// The operator of `keyspace` in `view`, which every in-memory table of it carries.
pub(in crate::db) fn operator_of(view: &View, keyspace: u32) -> Option<Arc<dyn MergeOperator>> {
    view.memtables(keyspace)
        .next()
        .and_then(|table| table.operator().cloned())
}

/// The value of the key `versions` stands on, at an operand at or below `snapshot`, read by
/// gathering its operands from the walk, `fragments` deleting what their range deletes cover.
/// The walk is left on or past the key's last version read; the caller seeks back.
///
/// # Errors
///
/// [`Error::MergeFailed`] as [`Operands::combine`], and a damaged table file met on the way.
pub(in crate::db) async fn merged_in_walk(
    versions: &mut MergedVersions<'_>,
    operator: Option<&Arc<dyn MergeOperator>>,
    keyspace: u32,
    snapshot: SequenceNumber,
    fragments: &Deletions,
) -> Result<Vec<u8>> {
    let key = versions
        .entry()
        .map(|version| version.user_key.to_vec())
        .unwrap_or_default();
    let mut operands = Operands::default();
    let mut base = None;
    while let Some(version) = versions.entry() {
        if version.user_key != key.as_slice() {
            break;
        }
        if version.sequence <= snapshot {
            let gathered = if fragments.deletes(version.user_key, version.sequence) {
                Gathered::Base(None)
            } else {
                operands.add(version.sequence, version.kind, version.value, 0)?
            };
            if let Gathered::Base(found) = gathered {
                base = found;
                break;
            }
        }
        versions.next().await;
    }
    if let Some(error) = versions.take_error() {
        return Err(error);
    }
    operands.combine(operator, keyspace, &key, base.as_deref())
}

#[cfg(test)]
mod tests;
