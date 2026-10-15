//! Errors raised while decoding or constructing shared types.

/// What went wrong with a value of a shared type.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    /// A stored value-kind byte names no kind this build knows.
    #[error("unknown value kind {0:#04x}")]
    UnknownValueKind(u8),
    /// An encoded internal key is shorter than its trailer.
    #[error("internal key of {len} bytes is shorter than its 8-byte trailer")]
    ShortInternalKey {
        /// Length of the encoded key that was given.
        len: usize,
    },
    /// A sequence number would pass the largest value the trailer can hold.
    #[error("sequence number space exhausted")]
    SequenceExhausted,
}

/// Result of an operation on a shared type.
pub type Result<T> = std::result::Result<T, Error>;
