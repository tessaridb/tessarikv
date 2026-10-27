//! Walking an iterator backward.
//!
//! The versions of a key sort newest first, so a backward walk meets them oldest first: the
//! last one at or below the snapshot decides the key, and its value is kept as the walk moves on
//! to the keys before.

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
            let mut live = false;
            while let Some(version) = self.versions.entry() {
                if version.user_key != self.key.as_slice() {
                    break;
                }
                if version.sequence <= self.snapshot {
                    let value = match seen(version.kind, version.value) {
                        Seen::Value(value) => Some(value),
                        Seen::Absent => None,
                        Seen::Malformed => {
                            self.key.clear();
                            return Err(malformed());
                        }
                    };
                    live = value.is_some()
                        && !self.fragments.deletes(version.user_key, version.sequence);
                    if let Some(value) = value.filter(|_| live) {
                        self.saved.clear();
                        self.saved.extend_from_slice(value);
                    }
                }
                self.versions.prev().await;
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
