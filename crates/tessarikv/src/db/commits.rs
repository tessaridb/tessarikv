//! What each commit wrote, kept just long enough for a committing transaction to learn whether
//! anything it writes was written after its snapshot (first committer wins).
//!
//! Every batch, plain or transactional, publishes right after its claim and before anything
//! that can wait: it records the hashes of the keys it writes (and whether it deletes a range)
//! in a [`ConflictTable`] at its first sequence, then announces its claim with
//! [`Sequencer::publish`]: two stores into the claim's own ring slot, no shared counter. A range
//! delete records the prefix buckets of its range's cover (see [`super::prefixes`]) and a bucket
//! of its keyspace, and raises the table's range value. A transaction with snapshot `S` and claim
//! `c` first waits until every claim between `S` and `c` has announced itself
//! ([`Sequencer::wait_published`]), then is refused when one of its keys was recorded above `S`,
//! when a range delete recorded above `S` covers one of its keys (a range bucket of the key's
//! prefixes is above `S`: every key of a range shares a bucket with its cover), or, if it deletes
//! a range itself, when a range delete of the same keyspace was recorded above `S` or a write may
//! have landed in its range since (any claim between `S` and `c`, or, where writes record their
//! prefixes, a prefix bucket of its cover above `S`). The range checks are skipped while the
//! range value is not above `S`. Refused, it announces without recording, so it never refuses
//! anyone else.
//!
//! Why nothing is missed: a writer `w` with `S < w < c` records before it announces, and the
//! validator reads the table after the Acquire that observes the announcement (or a `visible`
//! covering `w`, since a writer announces before it completes), so it reads a value of at least
//! `w` (message passing; see [`ConflictTable`]). Claims at or below `S` committed before the
//! snapshot; claims after `c` can only refuse spuriously. Keys sharing a bucket refuse
//! spuriously too; a refusal is retryable.
//!
//! Serializable transactions (when the database records prefixes, see [`super::prefixes`]):
//! every write also records the prefix buckets of its keys, and a serializable transaction is
//! also refused when a key it read was recorded above its snapshot, or when a range it scanned
//! shares a prefix bucket recorded above it, or a range delete of its keyspace was recorded
//! above it. Committed serializable transactions
//! then behave as if they ran one at a time in commit order: each one's reads were still
//! current when it claimed its place. A transaction that writes nothing is never refused.
//!
//! Blocking window: the wait is for earlier claims only, each of which announces right after
//! claiming without awaiting anything (a transaction after its own check), so it ends unless a
//! thread is descheduled inside that window (spin, then yield).

use tessarikv_constants::CONFLICT_BUCKETS;
use tessarikv_format::{WriteBatch, key_hash};
use tessarikv_sync::{Claim, ConflictTable, Sequencer};
use tessarikv_types::{KeyspaceId, ValueKind};

use super::prefixes::{self, Kind};

/// See the module documentation.
pub(super) struct Commits {
    conflicts: ConflictTable,
    /// Prefix buckets, when serializable transactions are enabled.
    prefixes: Option<ConflictTable>,
}

/// What a transaction's commit is checked against.
pub(crate) struct Check<'a> {
    /// The sequence the transaction read at.
    pub(crate) snapshot: u64,
    /// What it read, when it is serializable.
    pub(crate) reads: Option<&'a Reads>,
    /// The keys it read for update.
    pub(crate) for_update: &'a [(KeyspaceId, Vec<u8>)],
}

/// The keys and ranges a serializable transaction read from its snapshot.
#[derive(Debug, Default)]
pub(crate) struct Reads {
    keys: Vec<(KeyspaceId, Vec<u8>)>,
    ranges: Vec<(KeyspaceId, Vec<u8>, Option<Vec<u8>>)>,
}

impl Reads {
    /// Notes a point read of `key`.
    pub(crate) fn key(&mut self, keyspace: KeyspaceId, key: &[u8]) {
        self.keys.push((keyspace, key.to_vec()));
    }

    /// Notes a scan of `[start, end)` (unbounded above when `end` is `None`).
    pub(crate) fn range(&mut self, keyspace: KeyspaceId, start: &[u8], end: Option<&[u8]>) {
        self.ranges
            .push((keyspace, start.to_vec(), end.map(<[u8]>::to_vec)));
    }
}

/// Spreads keyspace ids over the hash so equal keys in two keyspaces fall apart.
const KEYSPACE_MIX: u64 = 0xd6e8_feb8_6659_fd93;

impl Commits {
    /// Commit tracking; with `serializable`, writes also record prefix buckets.
    pub(super) fn new(serializable: bool) -> Self {
        Self {
            conflicts: ConflictTable::new(CONFLICT_BUCKETS),
            prefixes: serializable.then(|| ConflictTable::new(CONFLICT_BUCKETS)),
        }
    }

    /// Whether serializable transactions can be checked.
    pub(super) fn serializable(&self) -> bool {
        self.prefixes.is_some()
    }

    /// Publishes `batch` under `claim`, checking it first when it is a transaction's; returns
    /// `false` when the check refused it (it then records nothing). Called once per claim,
    /// right after claiming, with nothing awaited in between, and before the claim is completed.
    pub(super) fn publish(
        &self,
        sequencer: &Sequencer,
        claim: &Claim,
        batch: &WriteBatch,
        check: Option<&Check<'_>>,
    ) -> bool {
        let first = claim.first();
        let accepted = check.is_none_or(|check| self.accepts(sequencer, first, batch, check));
        if accepted {
            for entry in batch {
                self.record(entry.keyspace, entry.kind, entry.key, entry.value, first);
            }
        }
        sequencer.publish(claim);
        accepted
    }

    /// Records one entry of the batch claimed from `first`; `end` is a range delete's end.
    fn record(
        &self,
        keyspace: KeyspaceId,
        kind: ValueKind,
        key: &[u8],
        end: Option<&[u8]>,
        first: u64,
    ) {
        if kind == ValueKind::RangeDelete {
            self.conflicts
                .record(range_bucket(keyspace, Kind::Prefix, &[]), first);
            prefixes::cover(key, end, |kind, bytes| {
                self.conflicts
                    .record(range_bucket(keyspace, kind, bytes), first);
            });
            self.conflicts.record_range(first);
        } else {
            self.conflicts.record(hash(keyspace, key), first);
            if let Some(table) = &self.prefixes {
                prefixes::buckets_of(key, |kind, bytes| {
                    table.record(bucket(keyspace, kind, bytes), first);
                });
            }
        }
    }

    /// Whether a transaction checked by `check`, claimed from `first`, may commit `batch`.
    fn accepts(
        &self,
        sequencer: &Sequencer,
        first: u64,
        batch: &WriteBatch,
        check: &Check<'_>,
    ) -> bool {
        let snapshot = check.snapshot;
        let before = first.saturating_sub(1);
        sequencer.wait_published(snapshot, before);
        let ranges = self.conflicts.newest_range() > snapshot;
        let writes_current = batch.into_iter().all(|entry| {
            if entry.kind == ValueKind::RangeDelete {
                !(ranges && self.ranged(entry.keyspace, snapshot))
                    && match &self.prefixes {
                        Some(table) => {
                            current(table, entry.keyspace, entry.key, entry.value, snapshot)
                        }
                        // Any claim since the snapshot may have written into the range.
                        None => before <= snapshot,
                    }
            } else {
                self.key_current(entry.keyspace, entry.key, snapshot, ranges)
            }
        });
        writes_current
            && check
                .for_update
                .iter()
                .all(|(keyspace, key)| self.key_current(*keyspace, key, snapshot, ranges))
            && check
                .reads
                .is_none_or(|reads| self.reads_current(reads, snapshot, ranges))
    }

    /// Whether nothing was recorded above `snapshot` in a bucket of what `reads` read.
    fn reads_current(&self, reads: &Reads, snapshot: u64, ranges: bool) -> bool {
        let Some(table) = &self.prefixes else {
            return false;
        };
        reads
            .keys
            .iter()
            .all(|(keyspace, key)| self.key_current(*keyspace, key, snapshot, ranges))
            && reads.ranges.iter().all(|(keyspace, start, end)| {
                !(ranges && self.ranged(*keyspace, snapshot))
                    && current(table, *keyspace, start, end.as_deref(), snapshot)
            })
    }

    /// Whether `key` was neither written nor covered by a range delete above `snapshot`; range
    /// deletes are looked at only when `ranges` says one was recorded above it.
    fn key_current(&self, keyspace: KeyspaceId, key: &[u8], snapshot: u64, ranges: bool) -> bool {
        if self.conflicts.newest(hash(keyspace, key)) > snapshot {
            return false;
        }
        let mut covered = false;
        if ranges {
            prefixes::buckets_of(key, |kind, bytes| {
                covered |= self.conflicts.newest(range_bucket(keyspace, kind, bytes)) > snapshot;
            });
        }
        !covered
    }

    /// Whether a range delete of `keyspace` was recorded above `snapshot`.
    fn ranged(&self, keyspace: KeyspaceId, snapshot: u64) -> bool {
        self.conflicts
            .newest(range_bucket(keyspace, Kind::Prefix, &[]))
            > snapshot
    }
}

/// Whether nothing was recorded in `table` above `snapshot` in a prefix bucket of the cover of
/// `[start, end)` (unbounded above for `None`) in `keyspace`.
fn current(
    table: &ConflictTable,
    keyspace: KeyspaceId,
    start: &[u8],
    end: Option<&[u8]>,
    snapshot: u64,
) -> bool {
    let mut current = true;
    prefixes::cover(start, end, |kind, bytes| {
        current &= table.newest(bucket(keyspace, kind, bytes)) <= snapshot;
    });
    current
}

/// The key's hash, mixed with its keyspace.
fn hash(keyspace: KeyspaceId, key: &[u8]) -> u64 {
    key_hash(key) ^ u64::from(keyspace.get()).wrapping_mul(KEYSPACE_MIX)
}

/// Keeps the buckets range deletes record apart from those of keys and prefixes.
const RANGE_MIX: u64 = 0x9e37_79b9_7f4a_7c15;

/// The hash of a bucket a range delete of `keyspace` records: one of its cover, or, for the
/// empty prefix (which no cover names), the keyspace's own.
fn range_bucket(keyspace: KeyspaceId, kind: Kind, bytes: &[u8]) -> u64 {
    bucket(keyspace, kind, bytes) ^ RANGE_MIX
}

/// The hash of a prefix bucket: its kind, then its bytes, mixed with the keyspace.
fn bucket(keyspace: KeyspaceId, kind: Kind, bytes: &[u8]) -> u64 {
    let mut tagged = [0_u8; prefixes::DEPTH + 1];
    let tag = match kind {
        Kind::Prefix => 0,
        Kind::Exact => 1,
    };
    let length = bytes.len().min(prefixes::DEPTH);
    if let Some((first, rest)) = tagged.split_first_mut() {
        *first = tag;
        if let Some(rest) = rest.get_mut(..length) {
            rest.copy_from_slice(bytes.get(..length).unwrap_or_default());
        }
    }
    hash(keyspace, tagged.get(..=length).unwrap_or_default())
}
