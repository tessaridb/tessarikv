//! What each commit wrote, kept just long enough for a committing transaction to learn whether
//! anything it writes was written after its snapshot (first committer wins).
//!
//! Every batch, plain or transactional, publishes right after its claim and before anything
//! that can wait: it records the hashes of the keys it writes (and whether it deletes a range)
//! in a [`ConflictTable`] at its first sequence, then announces its claim with
//! [`Sequencer::publish`]: two stores into the claim's own ring slot, no shared counter. A
//! transaction with snapshot `S` and claim `c` first waits until every claim between `S` and `c`
//! has announced itself ([`Sequencer::wait_published`]), then is refused when one of its keys was
//! recorded above `S`, when a range delete was recorded above `S`, or, if it deletes a range
//! itself, when any claim lies between `S` and `c`. Refused, it announces without recording, so
//! it never refuses anyone else.
//!
//! Why nothing is missed: a writer `w` with `S < w < c` records before it announces, and the
//! validator reads the table after the Acquire that observes the announcement (or a `visible`
//! covering `w`, since a writer announces before it completes), so it reads a value of at least
//! `w` (message passing; see [`ConflictTable`]). Claims at or below `S` committed before the
//! snapshot; claims after `c` can only refuse spuriously. Keys sharing a bucket refuse
//! spuriously too; a refusal is retryable.
//!
//! Blocking window: the wait is for earlier claims only, each of which announces right after
//! claiming without awaiting anything (a transaction after its own check), so it ends unless a
//! thread is descheduled inside that window (spin, then yield).

use tessarikv_constants::CONFLICT_BUCKETS;
use tessarikv_format::{WriteBatch, key_hash};
use tessarikv_sync::{Claim, ConflictTable, Sequencer};
use tessarikv_types::{KeyspaceId, ValueKind};

/// See the module documentation.
pub(super) struct Commits {
    conflicts: ConflictTable,
}

/// Spreads keyspace ids over the hash so equal keys in two keyspaces fall apart.
const KEYSPACE_MIX: u64 = 0xd6e8_feb8_6659_fd93;

impl Commits {
    pub(super) fn new() -> Self {
        Self {
            conflicts: ConflictTable::new(CONFLICT_BUCKETS),
        }
    }

    /// Publishes `batch` under `claim`, checking it first against `snapshot` when it is a
    /// transaction's; returns `false` when the check refused it (it then records nothing).
    /// Called once per claim, right after claiming, with nothing awaited in between, and before
    /// the claim is completed.
    pub(super) fn publish(
        &self,
        sequencer: &Sequencer,
        claim: &Claim,
        batch: &WriteBatch,
        snapshot: Option<u64>,
    ) -> bool {
        let first = claim.first();
        let accepted =
            snapshot.is_none_or(|snapshot| self.accepts(sequencer, first, batch, snapshot));
        if accepted {
            for entry in batch {
                if entry.kind == ValueKind::RangeDelete {
                    self.conflicts.record_range(first);
                } else {
                    self.conflicts
                        .record(hash(entry.keyspace, entry.key), first);
                }
            }
        }
        sequencer.publish(claim);
        accepted
    }

    /// Whether a transaction with `snapshot`, claimed from `first`, may commit `batch`.
    fn accepts(
        &self,
        sequencer: &Sequencer,
        first: u64,
        batch: &WriteBatch,
        snapshot: u64,
    ) -> bool {
        let before = first.saturating_sub(1);
        sequencer.wait_published(snapshot, before);
        if self.conflicts.newest_range() > snapshot {
            return false;
        }
        batch.into_iter().all(|entry| {
            if entry.kind == ValueKind::RangeDelete {
                // Conservative: any claim since the snapshot may have written into the range.
                before <= snapshot
            } else {
                self.conflicts.newest(hash(entry.keyspace, entry.key)) <= snapshot
            }
        })
    }
}

/// The key's hash, mixed with its keyspace.
fn hash(keyspace: KeyspaceId, key: &[u8]) -> u64 {
    key_hash(key) ^ u64::from(keyspace.get()).wrapping_mul(KEYSPACE_MIX)
}
