//! The internal key: a user key plus the version that wrote it.
//!
//! Encoded as the user key's bytes followed by an 8-byte big-endian trailer holding
//! `sequence << 8 | kind`. The order is user key ascending, then trailer DESCENDING, so the
//! newest version of a key comes first and a lookup at a snapshot stops at the first entry it
//! may see. Plain byte order cannot express this: `"a"` followed by a trailer would compare
//! against `"ab"` inside the trailer. So every sorted structure compares with
//! [`compare_internal`].

use std::cmp::Ordering;

use tessarikv_constants::{SEQUENCE_BITS, TRAILER_BYTES};

use crate::{Error, Result, SequenceNumber, ValueKind};

#[cfg(test)]
mod tests;

/// A version and its kind packed into one integer, so they order together.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Trailer(u64);

impl Trailer {
    /// Packs a sequence number and a kind.
    #[must_use]
    pub const fn new(sequence: SequenceNumber, kind: ValueKind) -> Self {
        Self((sequence.get() << 8) | kind_bits(kind.to_byte()))
    }

    /// The raw packed value.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// The sequence number in the high 56 bits.
    #[must_use]
    pub const fn sequence(self) -> SequenceNumber {
        // The shift leaves at most SEQUENCE_BITS bits, so the value is always in range.
        match SequenceNumber::new(self.0 >> (64 - SEQUENCE_BITS)) {
            Ok(sequence) => sequence,
            Err(_) => SequenceNumber::MAX,
        }
    }

    /// The kind in the low byte.
    ///
    /// # Errors
    ///
    /// [`Error::UnknownValueKind`] for a byte no kind uses.
    pub const fn kind(self) -> Result<ValueKind> {
        ValueKind::from_byte(self.0.to_be_bytes()[7])
    }
}

const fn kind_bits(kind: u8) -> u64 {
    u64::from_be_bytes([0, 0, 0, 0, 0, 0, 0, kind])
}

/// The trailer of a seek key: positions a cursor on the newest version visible at `snapshot`.
///
/// It carries the largest kind byte, so it sorts before every entry written at `snapshot`.
#[must_use]
pub const fn seek_trailer(snapshot: SequenceNumber) -> Trailer {
    Trailer((snapshot.get() << 8) | kind_bits(ValueKind::MAX_BYTE))
}

/// Appends the encoding of `user_key` at `trailer` to `out`.
pub fn encode_internal(user_key: &[u8], trailer: Trailer, out: &mut Vec<u8>) {
    out.reserve(user_key.len().saturating_add(TRAILER_BYTES));
    out.extend_from_slice(user_key);
    out.extend_from_slice(&trailer.0.to_be_bytes());
}

/// A borrowed, parsed internal key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InternalKeyRef<'a> {
    user_key: &'a [u8],
    trailer: Trailer,
}

impl<'a> InternalKeyRef<'a> {
    /// Splits an encoded internal key into its user key and trailer.
    ///
    /// # Errors
    ///
    /// [`Error::ShortInternalKey`] when `encoded` is shorter than a trailer.
    pub fn parse(encoded: &'a [u8]) -> Result<Self> {
        let split = encoded
            .len()
            .checked_sub(TRAILER_BYTES)
            .ok_or(Error::ShortInternalKey { len: encoded.len() })?;
        let (user_key, trailer) = encoded.split_at(split);
        let mut raw = [0_u8; TRAILER_BYTES];
        raw.copy_from_slice(trailer);
        Ok(Self {
            user_key,
            trailer: Trailer(u64::from_be_bytes(raw)),
        })
    }

    /// The user key.
    #[must_use]
    pub const fn user_key(&self) -> &'a [u8] {
        self.user_key
    }

    /// The trailer.
    #[must_use]
    pub const fn trailer(&self) -> Trailer {
        self.trailer
    }

    #[cfg(test)]
    pub(crate) const fn sentinel() -> InternalKeyRef<'static> {
        InternalKeyRef {
            user_key: &[],
            trailer: Trailer(0),
        }
    }
}

/// Orders two encoded internal keys: user key ascending, then trailer descending.
///
/// An encoding too short to hold a trailer orders as if its trailer were zero, after any real
/// version of the same bytes; such keys are never produced by [`encode_internal`].
#[must_use]
pub fn compare_internal(left: &[u8], right: &[u8]) -> Ordering {
    let (left_user, left_trailer) = split(left);
    let (right_user, right_trailer) = split(right);
    left_user
        .cmp(right_user)
        .then_with(|| right_trailer.cmp(&left_trailer))
}

fn split(encoded: &[u8]) -> (&[u8], u64) {
    match InternalKeyRef::parse(encoded) {
        Ok(key) => (key.user_key, key.trailer.0),
        Err(_) => (encoded, 0),
    }
}
