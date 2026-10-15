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
}

/// Result of a codec operation.
pub type Result<T> = std::result::Result<T, Error>;
