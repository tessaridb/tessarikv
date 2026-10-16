//! A transaction's own writes: every operation in the order it was made, and an index that
//! answers "what does this transaction say about this key" without walking them. It belongs
//! to one transaction and is never shared.

use std::collections::BTreeMap;

use tessarikv_types::KeyspaceId;

use crate::{Error, Result, WriteBatch};

/// One operation of a transaction.
#[derive(Debug, Clone)]
enum Op {
    Put {
        keyspace: KeyspaceId,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Delete {
        keyspace: KeyspaceId,
        key: Vec<u8>,
    },
    DeleteRange {
        keyspace: KeyspaceId,
        start: Vec<u8>,
        end: Vec<u8>,
    },
}

/// What a transaction's own writes say about a key.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Own<'a> {
    /// It wrote this value.
    Value(&'a [u8]),
    /// It deleted the key, alone or with a range.
    Deleted,
    /// It did not touch the key: the snapshot decides.
    Untouched,
}

/// See the module documentation.
#[derive(Debug, Default)]
pub(super) struct WriteSet {
    ops: Vec<Op>,
    /// Per keyspace, for each key a point operation touched, the position in `ops` of the
    /// latest one.
    points: BTreeMap<KeyspaceId, BTreeMap<Vec<u8>, usize>>,
    /// The positions of the range deletes in `ops`, oldest first.
    ranges: Vec<usize>,
}

impl WriteSet {
    pub(super) fn put(&mut self, keyspace: KeyspaceId, key: &[u8], value: &[u8]) {
        self.index_point(keyspace, key);
        self.ops.push(Op::Put {
            keyspace,
            key: key.to_vec(),
            value: value.to_vec(),
        });
    }

    pub(super) fn delete(&mut self, keyspace: KeyspaceId, key: &[u8]) {
        self.index_point(keyspace, key);
        self.ops.push(Op::Delete {
            keyspace,
            key: key.to_vec(),
        });
    }

    pub(super) fn delete_range(&mut self, keyspace: KeyspaceId, start: &[u8], end: &[u8]) {
        self.ranges.push(self.ops.len());
        self.ops.push(Op::DeleteRange {
            keyspace,
            start: start.to_vec(),
            end: end.to_vec(),
        });
    }

    /// How many operations were made: a savepoint.
    pub(super) fn len(&self) -> usize {
        self.ops.len()
    }

    /// Forgets every operation made after the first `len`.
    pub(super) fn truncate(&mut self, len: usize) {
        if len >= self.ops.len() {
            return;
        }
        self.ops.truncate(len);
        self.ranges.retain(|position| *position < len);
        // A key whose latest point operation was forgotten may still have an earlier one.
        self.points.clear();
        for (position, op) in self.ops.iter().enumerate() {
            if let Op::Put { keyspace, key, .. } | Op::Delete { keyspace, key } = op {
                self.points
                    .entry(*keyspace)
                    .or_default()
                    .insert(key.clone(), position);
            }
        }
    }

    /// What the operations made so far say about `key`: the later of its latest point
    /// operation and the latest range delete covering it decides.
    pub(super) fn lookup(&self, keyspace: KeyspaceId, key: &[u8]) -> Own<'_> {
        let point = self
            .points
            .get(&keyspace)
            .and_then(|keys| keys.get(key))
            .copied();
        let range = self.ranges.iter().rev().copied().find(|position| {
            matches!(
                self.ops.get(*position),
                Some(Op::DeleteRange { keyspace: of, start, end })
                    if *of == keyspace && start.as_slice() <= key && key < end.as_slice()
            )
        });
        match point {
            Some(point) if range.is_none_or(|range| point > range) => match self.ops.get(point) {
                Some(Op::Put { value, .. }) => Own::Value(value),
                _ => Own::Deleted,
            },
            _ if range.is_some() => Own::Deleted,
            _ => Own::Untouched,
        }
    }

    /// The operations as one batch, in the order they were made: each entry takes a later
    /// sequence than the one before, so the batch reads as the transaction did. A point
    /// operation a later one on the same key replaced is left out.
    pub(super) fn batch(&self) -> Result<WriteBatch> {
        self.encode().map_err(|error| match error {
            tessarikv_format::Error::TooLarge { what, size, limit } => {
                Error::TooLarge { what, size, limit }
            }
            _ => Error::Unsupported {
                what: "a transaction entry the batch encoding refuses",
            },
        })
    }

    fn encode(&self) -> tessarikv_format::Result<WriteBatch> {
        let mut batch = WriteBatch::new();
        for (position, op) in self.ops.iter().enumerate() {
            match op {
                Op::Put {
                    keyspace,
                    key,
                    value,
                } if self.is_latest(*keyspace, key, position) => {
                    batch.put(*keyspace, key, value)?;
                }
                Op::Delete { keyspace, key } if self.is_latest(*keyspace, key, position) => {
                    batch.delete(*keyspace, key)?;
                }
                Op::DeleteRange {
                    keyspace,
                    start,
                    end,
                } => batch.delete_range(*keyspace, start, end)?,
                Op::Put { .. } | Op::Delete { .. } => {}
            }
        }
        Ok(batch)
    }

    fn is_latest(&self, keyspace: KeyspaceId, key: &[u8], position: usize) -> bool {
        self.points.get(&keyspace).and_then(|keys| keys.get(key)) == Some(&position)
    }

    fn index_point(&mut self, keyspace: KeyspaceId, key: &[u8]) {
        self.points
            .entry(keyspace)
            .or_default()
            .insert(key.to_vec(), self.ops.len());
    }
}
