//! Snapshots: a fixed point in the database's history that reads may return to.

use tessarikv_sync::Registration;
use tessarikv_types::{KeyspaceId, SequenceNumber};

use super::{Db, DbIterator, ScanPage};
use crate::{Error, ReadOptions, Result};

/// A point in the database's history. Reads through it see the database as it was when the
/// snapshot was taken, whatever is written, switched or compacted afterwards; the versions it
/// needs are kept until it is dropped.
///
/// # Example
///
/// ```
/// use tessarikv::{Db, DbOptions, KeyspaceId, ReadOptions, WriteBatch, WriteOptions};
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let db = Db::open("example", DbOptions::in_memory()).await?;
/// let mut batch = WriteBatch::new();
/// batch.put(KeyspaceId::DEFAULT, b"apple", b"red")?;
/// db.write(&batch, &WriteOptions::default()).await?;
/// let snapshot = db.snapshot()?;
/// let mut batch = WriteBatch::new();
/// batch.put(KeyspaceId::DEFAULT, b"apple", b"green")?;
/// db.write(&batch, &WriteOptions::default()).await?;
/// let then = snapshot.get(KeyspaceId::DEFAULT, b"apple", &ReadOptions::default()).await?;
/// assert_eq!(then.as_deref(), Some(&b"red"[..]));
/// # Ok(())
/// # }
/// ```
pub struct Snapshot<'db> {
    db: &'db Db,
    registration: Option<Registration>,
    sequence: SequenceNumber,
}

impl Db {
    /// Takes a snapshot at the latest visible sequence.
    ///
    /// # Errors
    ///
    /// [`Error::Unsupported`] when no more snapshots can be registered (the registry's index
    /// space, 2^64 slots, is exhausted) and [`Error::SequenceExhausted`] when the visible
    /// sequence is out of range.
    pub fn snapshot(&self) -> Result<Snapshot<'_>> {
        let registration =
            self.snapshots
                .register(|| self.sequencer.visible())
                .ok_or(Error::Unsupported {
                    what: "registering more snapshots than the registry can index",
                })?;
        match SequenceNumber::new(registration.sequence()) {
            Ok(sequence) => Ok(Snapshot {
                db: self,
                registration: Some(registration),
                sequence,
            }),
            Err(_) => {
                self.snapshots.release(registration);
                Err(Error::SequenceExhausted)
            }
        }
    }

    /// The sequence of the oldest snapshot still held, by a [`Snapshot`] or a
    /// [`crate::Transaction`]; `None` when none is. Compaction keeps every version a held
    /// snapshot can see, so one long-lived holder keeps old versions on disk for as long as it
    /// lives: this is the number to watch. It walks every registered snapshot; call it as a
    /// metric, not per request.
    pub fn oldest_snapshot(&self) -> Option<SequenceNumber> {
        let horizon = self.snapshots.horizon(self.sequencer.visible());
        horizon
            .snapshots()
            .first()
            .and_then(|&sequence| SequenceNumber::new(sequence).ok())
    }
}

impl Snapshot<'_> {
    /// The values of `keys` in `keyspace` at this snapshot, as [`Db::multi_get`] returns them.
    ///
    /// # Errors
    ///
    /// As [`Db::get`].
    pub async fn multi_get<K: AsRef<[u8]>>(
        &self,
        keyspace: KeyspaceId,
        keys: impl IntoIterator<Item = K>,
        options: &ReadOptions,
    ) -> Result<Vec<Option<Vec<u8>>>> {
        self.db
            .multi_get_at(keyspace, keys, self.sequence, options)
            .await
    }

    /// The sequence number the snapshot reads at.
    pub fn sequence(&self) -> SequenceNumber {
        self.sequence
    }

    /// The value of `key` in `keyspace` at this snapshot.
    ///
    /// # Errors
    ///
    /// As [`Db::get`].
    pub async fn get(
        &self,
        keyspace: KeyspaceId,
        key: &[u8],
        options: &ReadOptions,
    ) -> Result<Option<Vec<u8>>> {
        self.db.get_at(keyspace, key, self.sequence, options).await
    }

    /// An iterator over `keyspace` at this snapshot; the snapshot outlives it.
    ///
    /// # Errors
    ///
    /// [`Error::UnknownKeyspace`] for a keyspace this database does not hold, and
    /// [`Error::Corruption`] for a damaged range-delete block in a table file.
    pub fn iter(&self, keyspace: KeyspaceId, options: &ReadOptions) -> Result<DbIterator<'_>> {
        self.db.check_keyspace(keyspace)?;
        DbIterator::new(
            &self.db.view.load_full(),
            keyspace,
            self.sequence,
            options,
            None,
        )
    }

    /// A page of `keyspace` at this snapshot, as [`Db::scan`] returns one.
    ///
    /// # Errors
    ///
    /// As [`Db::scan`].
    pub async fn scan(
        &self,
        keyspace: KeyspaceId,
        start: &[u8],
        end: Option<&[u8]>,
        limit: usize,
        options: &ReadOptions,
    ) -> Result<ScanPage> {
        self.db
            .scan_at(keyspace, start, end, limit, self.sequence, options)
            .await
    }
}

impl Drop for Snapshot<'_> {
    fn drop(&mut self) {
        if let Some(registration) = self.registration.take() {
            self.db.snapshots.release(registration);
        }
    }
}
