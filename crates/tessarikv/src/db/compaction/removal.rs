//! Versions a compaction writes out as deletes: an expiring put whose stamp the job's clock has
//! reached, and a value the keyspace's compaction filter removes. Each becomes a delete at its
//! own version rather than a dropped entry — dropping it would uncover an older version of the
//! key — and [`Retention`] then decides about that delete as about any other.

use std::sync::Arc;

use tessarikv_types::{InternalKeyRef, Trailer, ValueKind, encode_internal};

use super::hidden::Hidden;
use super::retain::{KeyRetention, Retention};
use crate::expiry::{Seen, malformed, now_millis, seen_at};
use crate::{CompactionFilter, FilterDecision, KeyspaceOptions, Result};

/// What a job writes out as deletes: by the clock when it started, and by the keyspace's
/// filter.
pub(crate) struct Removing {
    /// The wall clock when the job started, in milliseconds since the Unix epoch.
    pub(crate) now: u64,
    pub(crate) filter: Option<Arc<dyn CompactionFilter>>,
}

/// [`Removing`] for one merge, with the delete key it writes.
pub(super) struct Removal<'a> {
    removing: &'a Removing,
    retention: &'a Retention,
    deleted: Vec<u8>,
}

impl Removing {
    /// What a job of a keyspace with `options` starting now removes.
    pub(super) fn of(options: &KeyspaceOptions) -> Self {
        Self {
            now: now_millis(),
            filter: options.compaction_filter().cloned(),
        }
    }

    /// The decisions for one merge reading `retention`.
    pub(super) fn with<'a>(&'a self, retention: &'a Retention) -> Removal<'a> {
        Removal {
            removing: self,
            retention,
            deleted: Vec::new(),
        }
    }
}

impl Removal<'_> {
    /// The kind `version`, of kind `kind` storing `stored`, is written out as —
    /// [`ValueKind::Delete`] for a removed value, `kind` otherwise — and whether a range delete in
    /// `hidden` hides it. The filter is asked only about a value no range delete hides, that is
    /// the version written out for its stripe, and that no snapshot held at the job's start can
    /// read.
    ///
    /// # Errors
    ///
    /// [`crate::Error::Corruption`] for an expiring put too short to hold its stamp.
    pub(super) fn kind_of(
        &self,
        decisions: &KeyRetention<'_>,
        hidden: &Hidden,
        kind: ValueKind,
        version: &InternalKeyRef<'_>,
        stored: &[u8],
    ) -> Result<(ValueKind, bool)> {
        let (key, sequence) = (version.user_key(), version.trailer().sequence().get());
        let hides = hidden.hides(self.retention, key, sequence);
        if !matches!(kind, ValueKind::Value | ValueKind::ExpiringValue) {
            return Ok((kind, hides));
        }
        let removed = match seen_at(kind, stored, self.removing.now) {
            Seen::Value(value) => self.removing.filter.as_ref().is_some_and(|filter| {
                !hides
                    && decisions.newest_in_stripe(sequence)
                    && self.retention.past_every_snapshot(sequence)
                    && filter.filter(key, value) == FilterDecision::Remove
            }),
            Seen::Absent => true,
            // Only values reach here.
            Seen::Operand(_) => false,
            Seen::Malformed => return Err(malformed()),
        };
        Ok((if removed { ValueKind::Delete } else { kind }, hides))
    }

    /// The internal key of a delete of `version`'s user key at its sequence.
    pub(super) fn delete_key(&mut self, version: &InternalKeyRef<'_>) -> &[u8] {
        self.deleted.clear();
        let trailer = Trailer::new(version.trailer().sequence(), ValueKind::Delete);
        encode_internal(version.user_key(), trailer, &mut self.deleted);
        &self.deleted
    }
}
