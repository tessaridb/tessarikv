//! The categories errors fall into.

/// What kind of failure an [`super::Error`] is, and so what a caller can do about it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ErrorCategory {
    /// Caller input or options out of range; fix the call.
    Validation,
    /// A file the operation needs is missing.
    NotFound,
    /// A precondition failed or a concurrent write won; re-read and retry.
    Conflict,
    /// Another holder owns the resource, or the engine is stalled; retry later.
    Unavailable,
    /// A bounded wait expired.
    Timeout,
    /// Stored bytes do not decode or verify. The data needs restoring, not retrying.
    Corruption,
    /// The operating system refused an operation.
    Io,
    /// The database stopped accepting writes after a durability failure; reads still work.
    ReadOnly,
    /// The handle is closed.
    ShutDown,
    /// The engine broke one of its own invariants.
    Internal,
}
