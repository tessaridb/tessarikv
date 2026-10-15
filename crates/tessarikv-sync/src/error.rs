//! Errors of the lock-free building blocks.

/// Why a buffer operation was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BufferError {
    /// The range read reaches past the published length.
    OutOfRange {
        /// The end of the requested range.
        requested_end: u64,
        /// How many bytes were published when the read looked.
        available: u64,
    },
    /// An append would take the length past `u64::MAX`, or a chunk could not be addressed.
    Overflow,
}

impl std::fmt::Display for BufferError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OutOfRange {
                requested_end,
                available,
            } => write!(
                f,
                "read to byte {requested_end} but only {available} bytes are published"
            ),
            Self::Overflow => write!(f, "buffer length overflow"),
        }
    }
}

impl std::error::Error for BufferError {}
