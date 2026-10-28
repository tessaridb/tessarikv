//! Walking an iterator backward.
//!
//! The versions of a key sort newest first, so a backward walk meets them oldest first: the
//! last one at or below the snapshot decides the key, and its value is kept as the walk moves on
//! to the keys before.

use crate::db::read::operands::Operands;
use tessarikv_types::{SequenceNumber, Trailer, ValueKind, encode_internal, seek_trailer};

use super::DbIterator;
use crate::Result;
use crate::expiry::{Seen, malformed, seen};

impl DbIterator<'_> {
    /// Moves to the greatest key, or the greatest below the upper bound.
    ///
    /// # Errors
    ///
    /// As [`DbIterator::seek_to_first`].
    pub async fn seek_to_last(&mut self) -> Result<()> {
        match self.upper.take() {
            Some(upper) => {
                let moved = self.before(&upper).await;
                self.upper = Some(upper);
                moved
            }
            None => {
                self.valid = false;
                self.versions.seek_to_last().await;
                self.settle_backward().await
            }
        }
    }

    /// Moves to the greatest key not greater than `key` (and below the upper bound).
    ///
    /// # Errors
    ///
    /// As [`DbIterator::seek_to_first`].
    pub async fn seek_for_prev(&mut self, key: &[u8]) -> Result<()> {
        if self.upper.as_deref().is_some_and(|upper| upper <= key) {
            return self.seek_to_last().await;
        }
        // The smallest trailer: every version of `key` sorts at or before it.
        let mut target = Vec::new();
        encode_internal(
            key,
            Trailer::new(SequenceNumber::ZERO, ValueKind::Delete),
            &mut target,
        );
        self.valid = false;
        self.versions.seek_for_prev(&target).await;
        self.settle_backward().await
    }

    /// Moves to the previous key; the iterator is not valid before the first one or the lower
    /// bound.
    ///
    /// # Errors
    ///
    /// As [`DbIterator::seek_to_first`].
    pub async fn prev(&mut self) -> Result<()> {
        if !self.valid {
            return Ok(());
        }
        if self.backward {
            // The merge already stands before every version of the current key.
            return self.settle_backward().await;
        }
        let key = std::mem::take(&mut self.key);
        self.before(&key).await
    }

    /// Positions before every version of `key` and walks back to a live key.
    async fn before(&mut self, key: &[u8]) -> Result<()> {
        // The greatest trailer: every version of `key` sorts after it.
        let mut target = Vec::new();
        encode_internal(key, seek_trailer(SequenceNumber::MAX), &mut target);
        self.valid = false;
        self.versions.seek_for_prev(&target).await;
        self.settle_backward().await
    }

    /// Walks back to the greatest live key at the snapshot, not below the lower bound.
    async fn settle_backward(&mut self) -> Result<()> {
        self.backward = true;
        self.valid = false;
        while let Some(version) = self.versions.entry() {
            if self
                .lower
                .as_deref()
                .is_some_and(|lower| version.user_key < lower)
            {
                break;
            }
            self.key.clear();
            self.key.extend_from_slice(version.user_key);
            // Oldest first: the value so far, and the operands above it.
            let mut value: Option<Vec<u8>> = None;
            let mut operands = Vec::new();
            while let Some(version) = self.versions.entry() {
                if version.user_key != self.key.as_slice() {
                    break;
                }
                if version.sequence <= self.snapshot {
                    let deleted = self.fragments.deletes(version.user_key, version.sequence);
                    match seen(version.kind, version.value) {
                        Seen::Operand(operand) if !deleted => operands.push(operand.to_vec()),
                        Seen::Value(found) if !deleted => {
                            value = Some(found.to_vec());
                            operands.clear();
                        }
                        Seen::Value(_) | Seen::Absent | Seen::Operand(_) => {
                            value = None;
                            operands.clear();
                        }
                        Seen::Malformed => {
                            self.key.clear();
                            return Err(malformed());
                        }
                    }
                }
                self.versions.prev().await;
            }
            if !operands.is_empty() {
                let (keyspace, operator) = &self.merging;
                let combined = Operands::from_oldest_first(operands).combine(
                    operator.as_ref(),
                    *keyspace,
                    &self.key,
                    value.as_deref(),
                );
                match combined {
                    Ok(combined) => value = Some(combined),
                    Err(error) => {
                        self.key.clear();
                        return Err(error);
                    }
                }
            }
            let live = value.is_some();
            if let Some(value) = value {
                self.saved = value;
            }
            if live {
                self.valid = true;
                break;
            }
        }
        if !self.valid {
            self.key.clear();
        }
        self.finish_move()
    }
}
