//! Iterators: a walk over one keyspace at one snapshot, inside optional bounds, in either
//! direction.

mod backward;

use std::sync::Arc;

use tessarikv_types::{KeyspaceId, SequenceNumber, ValueKind};

use super::Db;
use super::merge::{MergedVersions, Source, TableVersions};
use super::snapshot::Snapshot;
use super::view::View;
use crate::memtable::MemtableCursor;
use crate::tombstones::Fragments;
use crate::{ReadOptions, Result};

/// A walk over the live keys of one keyspace, in key order forward or backward, at one
/// snapshot and inside the bounds of its [`ReadOptions`]. A deleted key is not shown.
///
/// Turning around (`prev` after `next`, or the other way) positions every source again, which
/// costs about a seek.
///
/// It reads the in-memory tables and table files of the moment it was made and keeps them
/// alive, so later writes and flushes change nothing it shows; holding one for long keeps
/// their memory. It starts unpositioned: call a seek first.
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
/// batch.put(KeyspaceId::DEFAULT, b"fruit/apple", b"red")?;
/// batch.put(KeyspaceId::DEFAULT, b"fruit/pear", b"green")?;
/// batch.put(KeyspaceId::DEFAULT, b"veg/leek", b"white")?;
/// db.write(&batch, &WriteOptions::default()).await?;
///
/// let options = ReadOptions::default().with_prefix(b"fruit/");
/// let mut fruit = db.iter(KeyspaceId::DEFAULT, &options)?;
/// fruit.seek_to_first().await?;
/// let mut keys = Vec::new();
/// while fruit.valid() {
///     keys.push(fruit.key().to_vec());
///     fruit.next().await?;
/// }
/// assert_eq!(keys, [b"fruit/apple".to_vec(), b"fruit/pear".to_vec()]);
/// # Ok(())
/// # }
/// ```
pub struct DbIterator<'a> {
    versions: MergedVersions<'static>,
    snapshot: SequenceNumber,
    /// The range deletes visible at the snapshot, from every source.
    fragments: Fragments,
    lower: Option<Vec<u8>>,
    upper: Option<Vec<u8>>,
    /// The current key; empty when not valid.
    key: Vec<u8>,
    valid: bool,
    /// Walking backward: the merge is already before the current key, whose value is `saved`.
    backward: bool,
    saved: Vec<u8>,
    /// The snapshot's registration, when the iterator took its own.
    _snapshot: Option<Snapshot<'a>>,
}

impl Db {
    /// An iterator over `keyspace` at the latest visible sequence, registered as a snapshot for
    /// as long as the iterator lives.
    ///
    /// # Errors
    ///
    /// [`crate::Error::UnknownKeyspace`] for a keyspace this database does not hold,
    /// [`crate::Error::Corruption`] for a damaged range-delete block in a table file, and the
    /// errors of [`Db::snapshot`].
    pub fn iter(&self, keyspace: KeyspaceId, options: &ReadOptions) -> Result<DbIterator<'_>> {
        Self::check_keyspace(keyspace)?;
        let snapshot = self.snapshot()?;
        let sequence = snapshot.sequence();
        DbIterator::new(
            &self.view.load_full(),
            keyspace,
            sequence,
            options,
            Some(snapshot),
        )
    }
}

impl<'a> DbIterator<'a> {
    pub(super) fn new(
        view: &View,
        keyspace: KeyspaceId,
        snapshot: SequenceNumber,
        options: &ReadOptions,
        registration: Option<Snapshot<'a>>,
    ) -> Result<Self> {
        let fragments = Fragments::new(&view.tombstones(keyspace.get())?, snapshot);
        let sources = view
            .shared_memtables()
            .map(|table| Source::Shared(MemtableCursor::new(Arc::clone(table))))
            .chain(
                view.files(keyspace.get())
                    .map(|file| Source::Table(Box::new(TableVersions::new(file.table.iter())))),
            )
            .collect();
        Ok(Self {
            versions: MergedVersions::new(sources),
            snapshot,
            fragments,
            lower: options.lower_bound.clone(),
            upper: options.upper_bound.clone(),
            key: Vec::new(),
            valid: false,
            backward: false,
            saved: Vec::new(),
            _snapshot: registration,
        })
    }

    /// Whether the iterator is on a key.
    pub fn valid(&self) -> bool {
        self.valid
    }

    /// The current key; empty when not valid.
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    /// The current value; empty when not valid.
    pub fn value(&self) -> &[u8] {
        if !self.valid {
            return &[];
        }
        if self.backward {
            return &self.saved;
        }
        self.versions.entry().map_or(&[], |version| version.value)
    }

    /// Moves to the least key, or to the lower bound.
    ///
    /// # Errors
    ///
    /// [`crate::Error::Corruption`] when a table file on the way is damaged; the iterator is
    /// then not valid.
    pub async fn seek_to_first(&mut self) -> Result<()> {
        let lower = self.lower.take();
        let moved = self.position_at(lower.as_deref().unwrap_or_default());
        self.lower = lower;
        moved
    }

    /// Moves to the least key not less than `key` (and not less than the lower bound).
    ///
    /// # Errors
    ///
    /// As [`DbIterator::seek_to_first`].
    pub async fn seek(&mut self, key: &[u8]) -> Result<()> {
        let lower = self.lower.take();
        let start = match lower.as_deref() {
            Some(lower) if lower > key => lower,
            _ => key,
        };
        let moved = self.position_at(start);
        self.lower = lower;
        moved
    }

    /// Moves to the next key; the iterator is not valid past the last one or the upper bound.
    ///
    /// # Errors
    ///
    /// As [`DbIterator::seek_to_first`].
    pub async fn next(&mut self) -> Result<()> {
        if !self.valid {
            return Ok(());
        }
        if self.backward {
            // Turning around: back onto the current key, walking forward.
            let key = std::mem::take(&mut self.key);
            self.position_at(&key)?;
            if !self.valid {
                return Ok(());
            }
        }
        self.versions.next();
        self.settle(true)
    }

    fn position_at(&mut self, start: &[u8]) -> Result<()> {
        self.backward = false;
        self.versions.seek(start, self.snapshot);
        self.key.clear();
        self.settle(false)
    }

    /// Walks on to the first live key at the snapshot, below the upper bound. While `skipping`,
    /// versions of the current key are passed over: it has been decided.
    fn settle(&mut self, mut skipping: bool) -> Result<()> {
        self.valid = false;
        while let Some(version) = self.versions.entry() {
            if self
                .upper
                .as_deref()
                .is_some_and(|upper| version.user_key >= upper)
            {
                break;
            }
            // The first version at or below the snapshot decides its key.
            let passed = version.sequence > self.snapshot
                || (skipping && version.user_key == self.key.as_slice());
            if !passed {
                self.key.clear();
                self.key.extend_from_slice(version.user_key);
                if version.kind == ValueKind::Value
                    && !self.fragments.deletes(version.user_key, version.sequence)
                {
                    self.valid = true;
                    break;
                }
                skipping = true;
            }
            self.versions.next();
        }
        if !self.valid {
            self.key.clear();
        }
        self.finish_move()
    }

    /// Reports a damaged table file met on the way; the iterator is then not valid.
    fn finish_move(&mut self) -> Result<()> {
        match self.versions.take_error() {
            Some(error) => {
                self.valid = false;
                self.key.clear();
                Err(error)
            }
            None => Ok(()),
        }
    }
}
