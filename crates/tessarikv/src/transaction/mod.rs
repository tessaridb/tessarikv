//! Transactions: reads at one snapshot, writes kept private until they commit together.
//!
//! A [`Transaction`] begins at the database's latest visible sequence and reads every key and
//! range ([`Transaction::get`], [`Transaction::scan`]) as it was there, except the keys it wrote
//! itself, which it reads as it wrote them. Its puts,
//! deletes and range deletes stay inside it until [`Transaction::commit`] writes them as one
//! atomic batch; dropping it without committing writes nothing. A savepoint marks a point the
//! transaction can return to, forgetting what it wrote since. A transaction that only reads needs
//! none of this: a [`crate::Snapshot`] is the isolated read-only transaction, and it is never
//! refused. Every open transaction or snapshot holds back version cleanup from its sequence on
//! ([`Db::oldest_snapshot`] reports the oldest), so long-lived ones cost disk space.
//!
//! Isolation is snapshot isolation, first committer wins: the commit is refused with
//! [`Error::Conflict`] when a write committed after the transaction began, by another
//! transaction or a plain [`Db::write`], touched a key the transaction writes. A refused
//! transaction writes nothing and may be run again. The check runs concurrently with other
//! commits, with no lock and no single committer: it waits only for commits that claimed their
//! sequence numbers earlier to publish their keys, which they do before anything that can wait.
//!
//! What snapshot isolation does not guarantee: two transactions that each read what the other
//! writes, and write disjoint keys, both commit (write skew). A transaction begun with
//! [`Isolation::Serializable`] (on a database opened with
//! [`crate::DbOptions::serializable_transactions`]) is also refused when a key it read, or a key
//! in a range it scanned, was written after it began; committed serializable transactions behave
//! as if they ran one at a time, in commit order, and one that writes nothing is never refused.
//! Snapshot transactions and plain writes are not checked for what they read. Keys are compared by hash, so a commit is occasionally refused for a key it
//! did not share. A range delete committed after a transaction began refuses it when the range
//! may cover a key it writes (or, serializable, read) in the same keyspace; keys are compared by
//! their leading bytes there, so a key near the range's bounds may be refused too. A
//! transaction's own range delete is refused when a range delete of its keyspace committed after
//! it began, or when a write may have landed in its range since: any later commit, or, on a
//! database opened with [`crate::DbOptions::serializable_transactions`], a later write into the
//! range.

mod scan;
mod write_set;

#[cfg(test)]
mod conflict_tests;
#[cfg(test)]
mod history_tests;
#[cfg(test)]
mod serializable_tests;
#[cfg(test)]
mod tests;

use tessarikv_constants::{MAX_KEY_BYTES, MAX_VALUE_BYTES};
use tessarikv_types::{KeyspaceId, SequenceNumber};

use crate::db::{Check, Reads};
use crate::{
    Db, Error, Isolation, ReadOptions, Result, Snapshot, TransactionOptions, WriteOptions,
};
use write_set::{Own, WriteSet};

/// A point a transaction can roll back to: the writes made before it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Savepoint(usize);

/// See the module documentation.
///
/// # Example
///
/// ```
/// use tessarikv::{Db, DbOptions, KeyspaceId, ReadOptions, WriteOptions};
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let db = Db::open("example", DbOptions::in_memory()).await?;
/// let mut txn = db.begin()?;
/// txn.put(KeyspaceId::DEFAULT, b"apple", b"red")?;
/// let own = txn.get(KeyspaceId::DEFAULT, b"apple", &ReadOptions::default()).await?;
/// assert_eq!(own.as_deref(), Some(&b"red"[..]));
/// txn.commit(&WriteOptions::default()).await?;
/// # Ok(())
/// # }
/// ```
pub struct Transaction<'db> {
    db: &'db Db,
    snapshot: Snapshot<'db>,
    writes: WriteSet,
    /// What it read from its snapshot, when it is serializable.
    reads: Option<Reads>,
}

impl Db {
    /// Begins a transaction at the latest visible sequence, with [`Isolation::Snapshot`].
    ///
    /// # Errors
    ///
    /// As [`Db::snapshot`].
    pub fn begin(&self) -> Result<Transaction<'_>> {
        self.begin_with(&TransactionOptions::default())
    }

    /// Begins a transaction at the latest visible sequence, as `options` ask.
    ///
    /// # Errors
    ///
    /// [`Error::InvalidArgument`] for [`Isolation::Serializable`] on a database opened without
    /// [`crate::DbOptions::serializable_transactions`]; otherwise as [`Db::snapshot`].
    pub fn begin_with(&self, options: &TransactionOptions) -> Result<Transaction<'_>> {
        let reads = match options.isolation {
            Isolation::Snapshot => None,
            Isolation::Serializable if self.checks_reads() => Some(Reads::default()),
            Isolation::Serializable => {
                return Err(Error::InvalidArgument {
                    what: "isolation",
                    reason: "serializable transactions are not enabled for this database",
                });
            }
        };
        Ok(Transaction {
            db: self,
            snapshot: self.snapshot()?,
            writes: WriteSet::default(),
            reads,
        })
    }
}

impl Transaction<'_> {
    /// The sequence number the transaction reads at.
    pub fn sequence(&self) -> SequenceNumber {
        self.snapshot.sequence()
    }

    /// The value of `key` in `keyspace`: as this transaction wrote it, or else as it was when
    /// the transaction began.
    ///
    /// # Errors
    ///
    /// As [`Db::get`].
    pub async fn get(
        &mut self,
        keyspace: KeyspaceId,
        key: &[u8],
        options: &ReadOptions,
    ) -> Result<Option<Vec<u8>>> {
        self.db.check_keyspace(keyspace)?;
        match self.writes.lookup(keyspace, key) {
            Own::Value(value) => Ok(Some(value.to_vec())),
            Own::Deleted => Ok(None),
            Own::Untouched => {
                if let Some(reads) = &mut self.reads {
                    reads.key(keyspace, key);
                }
                self.snapshot.get(keyspace, key, options).await
            }
        }
    }

    /// The values of `keys` in `keyspace`, in the order given, each as [`Transaction::get`]
    /// returns it: all from the transaction's snapshot or its own writes.
    ///
    /// # Errors
    ///
    /// As [`Db::get`].
    pub async fn multi_get<K: AsRef<[u8]>>(
        &mut self,
        keyspace: KeyspaceId,
        keys: impl IntoIterator<Item = K>,
        options: &ReadOptions,
    ) -> Result<Vec<Option<Vec<u8>>>> {
        let mut values = Vec::new();
        for key in keys {
            values.push(self.get(keyspace, key.as_ref(), options).await?);
        }
        Ok(values)
    }

    /// Sets `key` in `keyspace` to `value` when the transaction commits.
    ///
    /// # Errors
    ///
    /// [`Error::UnknownKeyspace`] for a keyspace the database does not hold and
    /// [`Error::TooLarge`] for a key or value longer than a batch entry may be, as
    /// [`crate::WriteBatch::put`] refuses them.
    pub fn put(&mut self, keyspace: KeyspaceId, key: &[u8], value: &[u8]) -> Result<()> {
        self.db.check_keyspace(keyspace)?;
        check_size("key", key.len(), MAX_KEY_BYTES)?;
        check_size("value", value.len(), MAX_VALUE_BYTES)?;
        self.writes.put(keyspace, key, value);
        Ok(())
    }

    /// Deletes `key` in `keyspace` when the transaction commits.
    ///
    /// # Errors
    ///
    /// As [`Transaction::put`].
    pub fn delete(&mut self, keyspace: KeyspaceId, key: &[u8]) -> Result<()> {
        self.db.check_keyspace(keyspace)?;
        check_size("key", key.len(), MAX_KEY_BYTES)?;
        self.writes.delete(keyspace, key);
        Ok(())
    }

    /// Deletes every key in `[start, end)` of `keyspace` when the transaction commits, including
    /// keys written before it and not ones this transaction writes after it.
    ///
    /// # Errors
    ///
    /// [`Error::InvalidArgument`] when `start` is not below `end`, and as [`Transaction::put`]
    /// for the keyspace and the two keys.
    pub fn delete_range(&mut self, keyspace: KeyspaceId, start: &[u8], end: &[u8]) -> Result<()> {
        self.db.check_keyspace(keyspace)?;
        if start >= end {
            return Err(Error::InvalidArgument {
                what: "range delete",
                reason: "the start is not below the end",
            });
        }
        check_size("range start", start.len(), MAX_KEY_BYTES)?;
        check_size("range end", end.len(), MAX_KEY_BYTES)?;
        self.writes.delete_range(keyspace, start, end);
        Ok(())
    }

    /// A point to roll back to.
    pub fn savepoint(&self) -> Savepoint {
        Savepoint(self.writes.len())
    }

    /// Forgets every write made after `savepoint`; a savepoint taken later than one already
    /// rolled back to changes nothing.
    pub fn rollback_to(&mut self, savepoint: Savepoint) {
        self.writes.truncate(savepoint.0);
    }

    /// Writes everything the transaction wrote as one atomic batch, under `options`, unless a
    /// write committed after the transaction began touched one of its keys. Returns the sequence
    /// number of its last entry.
    ///
    /// # Errors
    ///
    /// [`Error::Conflict`] when the commit is refused (nothing is written; the transaction may
    /// be run again), otherwise as [`Db::write`].
    pub async fn commit(self, options: &WriteOptions) -> Result<SequenceNumber> {
        let batch = self.writes.batch()?;
        let check = Check {
            snapshot: self.snapshot.sequence().get(),
            reads: self.reads.as_ref(),
        };
        self.db.commit_batch(&batch, options, Some(check)).await
    }
}

/// Refuses a `size` above `limit`, as a batch entry would.
fn check_size(what: &'static str, size: usize, limit: usize) -> Result<()> {
    if size > limit {
        return Err(Error::TooLarge {
            what,
            size: u64::try_from(size).unwrap_or(u64::MAX),
            limit: u64::try_from(limit).unwrap_or(u64::MAX),
        });
    }
    Ok(())
}
