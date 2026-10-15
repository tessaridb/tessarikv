//! Sequence numbers: the engine's total order of writes.

use tessarikv_constants::MAX_SEQUENCE;

use crate::{Error, Result};

/// The position of one key operation in the database's total order.
///
/// Every operation of a write batch takes its own number, so a batch of N operations spans N
/// consecutive numbers. A snapshot is a sequence number: it sees exactly the operations at or
/// below it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct SequenceNumber(u64);

impl SequenceNumber {
    /// Before any write: the state of an empty database.
    pub const ZERO: Self = Self(0);
    /// The largest number the trailer can hold; reads at it see every write.
    pub const MAX: Self = Self(MAX_SEQUENCE);

    /// Wraps a raw number.
    ///
    /// # Errors
    ///
    /// [`Error::SequenceExhausted`] when `raw` is above [`SequenceNumber::MAX`].
    pub const fn new(raw: u64) -> Result<Self> {
        if raw > MAX_SEQUENCE {
            Err(Error::SequenceExhausted)
        } else {
            Ok(Self(raw))
        }
    }

    /// The raw number.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// The number `count` places after this one.
    ///
    /// # Errors
    ///
    /// [`Error::SequenceExhausted`] when the result would pass [`SequenceNumber::MAX`].
    pub fn advance(self, count: u64) -> Result<Self> {
        self.0
            .checked_add(count)
            .ok_or(Error::SequenceExhausted)
            .and_then(Self::new)
    }
}
