//! The engine's error type and its categories.
//!
//! Every failure is a typed variant carrying the context needed to act on it, belongs to one
//! [`ErrorCategory`], and has a stable [`Error::code`]. Callers branch on the category or the
//! code, never on the message. An absent KEY is never an error: reads return `Ok(None)`;
//! [`ErrorCategory::NotFound`] means a FILE the operation needs is missing.
//!
//! There is no `unauthorized` or `forbidden` category: an embedded engine has no principals.

use std::path::{Path, PathBuf};

#[cfg(test)]
mod tests;

/// What kind of failure an [`Error`] is, and so what a caller can do about it.
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

/// Everything an engine operation can fail with.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// A file-system call failed.
    #[error("{operation} {path}: {source}")]
    Io {
        /// What was being done.
        operation: &'static str,
        /// The path it was done to.
        path: PathBuf,
        /// The operating system's error.
        #[source]
        source: std::io::Error,
    },
    /// A file the operation needs does not exist.
    #[error("{path} does not exist")]
    NotFound {
        /// The missing path.
        path: PathBuf,
    },
    /// The database directory is already open.
    #[error("{path} is locked by another open database")]
    Locked {
        /// The lock file.
        path: PathBuf,
    },
    /// An option value is outside what the engine accepts.
    #[error("invalid option `{option}`: {reason}")]
    InvalidOptions {
        /// The option, as its field path (`keyspace.block_bytes`).
        option: &'static str,
        /// Why it is refused.
        reason: String,
    },
    /// Stored bytes do not decode or verify.
    ///
    /// The message names where, never the stored bytes themselves, so a log line cannot leak data.
    #[error("corruption in {what}{}{}: {detail}", path.as_ref().map(|path| format!(" of {}", path.display())).unwrap_or_default(), offset.map(|offset| format!(" at byte {offset}")).unwrap_or_default())]
    Corruption {
        /// What was being decoded (`block`, `log record`, `manifest`).
        what: &'static str,
        /// The file, when the bytes came from one.
        path: Option<PathBuf>,
        /// The byte offset in that file.
        offset: Option<u64>,
        /// What was wrong.
        detail: String,
    },
    /// A read reached past the end of a file: the file is shorter than its own metadata says.
    #[error("read past the end of {path}: wanted byte {requested_end}, file has {available}")]
    ShortRead {
        /// The file.
        path: PathBuf,
        /// End of the requested range.
        requested_end: u64,
        /// Bytes in the file.
        available: u64,
    },
}

/// Result of an engine operation.
pub type Result<T> = std::result::Result<T, Error>;

impl Error {
    /// The category a caller branches on.
    #[must_use]
    pub const fn category(&self) -> ErrorCategory {
        match self {
            Self::Io { .. } => ErrorCategory::Io,
            Self::NotFound { .. } => ErrorCategory::NotFound,
            Self::Locked { .. } => ErrorCategory::Unavailable,
            Self::InvalidOptions { .. } => ErrorCategory::Validation,
            Self::Corruption { .. } | Self::ShortRead { .. } => ErrorCategory::Corruption,
        }
    }

    /// A stable machine-readable code; codes are added, never changed.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::Io { .. } => "io",
            Self::NotFound { .. } => "not_found",
            Self::Locked { .. } => "locked",
            Self::InvalidOptions { .. } => "invalid_options",
            Self::Corruption { .. } => "corruption",
            Self::ShortRead { .. } => "short_read",
        }
    }

    /// Whether the same call may succeed if repeated unchanged.
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Io { source, .. } => matches!(
                source.kind(),
                std::io::ErrorKind::Interrupted | std::io::ErrorKind::WouldBlock
            ),
            Self::Locked { .. } => true,
            Self::NotFound { .. }
            | Self::InvalidOptions { .. }
            | Self::Corruption { .. }
            | Self::ShortRead { .. } => false,
        }
    }

    /// Maps an OS error, turning "not found" into [`Error::NotFound`].
    pub(crate) fn io(operation: &'static str, path: &Path, source: std::io::Error) -> Self {
        if source.kind() == std::io::ErrorKind::NotFound {
            return Self::NotFound {
                path: path.to_path_buf(),
            };
        }
        Self::Io {
            operation,
            path: path.to_path_buf(),
            source,
        }
    }
}
