//! What a stored entry means.

use crate::{Error, Result};

/// The kind of an entry, stored in the low byte of its trailer.
///
/// A delete is an entry like any other: it hides older versions until compaction proves no
/// reader needs them and drops both.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ValueKind {
    /// The key is absent from this version on.
    Delete = 0,
    /// The key holds the entry's value.
    Value = 1,
    /// Every key in `[this key, the entry's value)` is absent from this version on.
    RangeDelete = 2,
    /// The key holds the entry's value until the time its expiry stamp names (see
    /// [`split_expiring`](crate::split_expiring)), and is absent from then on: an expired
    /// version hides the older ones like a delete.
    ExpiringValue = 3,
    /// The key holds a value kept in a blob file; the entry's value says where (see
    /// `tessarikv_format::BlobIndex`). Only table files hold it: writes never carry it.
    BlobIndex = 4,
}

impl ValueKind {
    /// The largest kind byte; a seek key uses it so it sorts before every entry of its version.
    pub const MAX_BYTE: u8 = 0xff;

    /// The byte stored in the trailer.
    #[must_use]
    pub const fn to_byte(self) -> u8 {
        match self {
            Self::Delete => 0,
            Self::Value => 1,
            Self::RangeDelete => 2,
            Self::ExpiringValue => 3,
            Self::BlobIndex => 4,
        }
    }

    /// The kind a stored byte names.
    ///
    /// # Errors
    ///
    /// [`Error::UnknownValueKind`] for a byte no kind uses.
    pub const fn from_byte(byte: u8) -> Result<Self> {
        match byte {
            0 => Ok(Self::Delete),
            1 => Ok(Self::Value),
            2 => Ok(Self::RangeDelete),
            3 => Ok(Self::ExpiringValue),
            4 => Ok(Self::BlobIndex),
            other => Err(Error::UnknownValueKind(other)),
        }
    }
}
