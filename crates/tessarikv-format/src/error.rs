//! Errors of the on-disk format codecs.
//!
//! Every decoder treats its input as hostile: a short or malformed input is one of these values,
//! never a panic and never an allocation sized by an unchecked length field.

/// Why bytes could not be decoded.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The input ended before a complete value.
    #[error("{what} truncated: needed {needed} bytes, {available} available")]
    Truncated {
        /// What was being decoded.
        what: &'static str,
        /// Bytes the value needs.
        needed: u64,
        /// Bytes the input held.
        available: u64,
    },

    /// The input is in a format version this build does not read.
    #[error("{what} is format version {version}, which this build does not read")]
    UnsupportedVersion {
        /// What was being decoded.
        what: &'static str,
        /// The version byte found.
        version: u8,
    },

    /// A value, or a length field describing one, is larger than the engine accepts.
    #[error("{what} of {size} bytes exceeds the limit of {limit} bytes")]
    TooLarge {
        /// What was too large.
        what: &'static str,
        /// Its size in bytes.
        size: u64,
        /// The largest size accepted.
        limit: u64,
    },

    /// The input breaks a rule of its format.
    #[error("{what} is malformed: {reason}")]
    Malformed {
        /// What was being decoded.
        what: &'static str,
        /// The rule it breaks.
        reason: &'static str,
    },

    /// Sealed bytes failed authentication: the key is not the one they were sealed with, or the
    /// bytes, their place or their file header were changed. One error for every cause, so a
    /// failure tells nothing about which.
    #[error("{what} failed authentication: wrong key, or the bytes were changed")]
    Unauthenticated {
        /// What was being opened.
        what: &'static str,
    },
}

/// Result of a codec operation.
pub type Result<T> = std::result::Result<T, Error>;
