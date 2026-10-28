//! The engine's error type and its categories.
//!
//! Every failure is a typed variant carrying the context needed to act on it, belongs to one
//! [`ErrorCategory`], and has a stable [`Error::code`]. Callers branch on the category or the
//! code, never on the message. An absent KEY is never an error: reads return `Ok(None)`;
//! [`ErrorCategory::NotFound`] means a FILE the operation needs is missing.
//!
//! There is no `unauthorized` or `forbidden` category: an embedded engine has no principals.

use std::path::{Path, PathBuf};

mod category;
#[cfg(test)]
mod tests;

pub use self::category::ErrorCategory;

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
    /// A batch or a read names a keyspace the database does not hold.
    #[error("unknown keyspace {keyspace}")]
    UnknownKeyspace {
        /// The keyspace id.
        keyspace: u32,
    },
    /// The operation needs something this version of the engine does not provide.
    #[error("not supported: {what}")]
    Unsupported {
        /// What was asked for.
        what: &'static str,
    },
    /// The database was written in a later store format than this build reads; it is refused
    /// before its tables or logs are read. Open it with the release that wrote it, or a later one.
    #[error("{path} is store format {found}; this build reads formats up to {newest}")]
    UnsupportedFormat {
        /// The manifest that records the format.
        path: PathBuf,
        /// The store format the database records.
        found: u32,
        /// The latest store format this build reads.
        newest: u32,
    },
    /// Every sequence number has been used; the database accepts no more writes.
    #[error("sequence numbers exhausted")]
    SequenceExhausted,
    /// An in-memory table refused an entry: an engine invariant was broken or its arena is
    /// exhausted.
    #[error("in-memory table refused an entry: {reason}")]
    Memtable {
        /// Why.
        reason: &'static str,
    },
    /// The write-ahead log stopped after a write or a sync failed. Writes acknowledged before the
    /// failure are as durable as they were acknowledged; nothing else is written until the
    /// database is reopened, which recovers from the log on storage.
    #[error("the write-ahead log stopped after a failed write or sync; reopen the database")]
    LogFailed,
    /// Writing a flushed table or its manifest record failed. Acknowledged writes stay in the
    /// write-ahead log; nothing else is written until the database is reopened, which recovers
    /// from the manifest and the log.
    #[error("a flush failed to write its table or the manifest; reopen the database")]
    FlushFailed,
    /// A compaction failed to read its tables, write its outputs or record them in the
    /// manifest. Every table it read is still in place and named by the manifest; nothing else
    /// is written until the database is reopened, which removes the outputs no edit named.
    #[error("a compaction failed to write its tables or the manifest; reopen the database")]
    CompactionFailed,
    /// The thread reading a table block stopped without an answer. Nothing was changed.
    #[error("a table block read stopped without an answer")]
    ReadFailed,
    /// The write asked not to wait while in-memory tables wait for flushes, and they do: the
    /// active table is full and no more tables may be frozen. Retry later.
    #[error("writes are stalled: {reason}")]
    WriteStalled {
        /// What the engine is waiting for.
        reason: &'static str,
    },
    /// Opening was told to fail when a database already exists, and one does.
    #[error("a database already exists at {path}")]
    AlreadyExists {
        /// The database's path.
        path: PathBuf,
    },
    /// The database was closed.
    #[error("the database is closed")]
    Closed,
    /// The database was opened read-only (see [`crate::Db::open_read_only`]): `what` would
    /// change it.
    #[error("{what} cannot run on a database opened read-only")]
    ReadOnly {
        /// The operation refused.
        what: &'static str,
    },
    /// A read met operands of keyspace `keyspace` that its merge operator did not combine: the
    /// open names no operator for it, or the operator refused them.
    #[error("the merge operands of keyspace {keyspace} could not be combined")]
    MergeFailed {
        /// The keyspace holding the operands.
        keyspace: u32,
    },
    /// An argument the operation cannot accept.
    #[error("invalid {what}: {reason}")]
    InvalidArgument {
        /// What was refused.
        what: &'static str,
        /// Why.
        reason: &'static str,
    },
    /// A transaction's commit was refused: a write committed after the transaction began
    /// touched a key it writes. Nothing of the transaction was written; running it again from
    /// [`crate::Db::begin`] may succeed.
    #[error("the transaction conflicts with a write committed after it began")]
    Conflict,
    /// The key the caller gave for `key_id` does not open the database's files.
    #[error("the key given for key id {key_id} does not open this database")]
    WrongKey {
        /// The id the database's files name.
        key_id: u32,
    },
    /// The database's files are encrypted with key `key_id`, and no key was given for it.
    #[error("the database is encrypted with key id {key_id}, and no key was given for it")]
    KeyNotFound {
        /// The id the database's files name.
        key_id: u32,
    },
    /// A key or value is larger than a batch entry can describe.
    #[error("{what} of {size} bytes exceeds the limit of {limit} bytes")]
    TooLarge {
        /// What was too large.
        what: &'static str,
        /// Its size in bytes.
        size: u64,
        /// The largest size accepted.
        limit: u64,
    },
}

/// Result of an engine operation.
pub type Result<T> = std::result::Result<T, Error>;

impl Error {
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
            Self::UnknownKeyspace { .. } => "unknown_keyspace",
            Self::Unsupported { .. } => "unsupported",
            Self::UnsupportedFormat { .. } => "unsupported_format",
            Self::SequenceExhausted => "sequence_exhausted",
            Self::Memtable { .. } => "memtable",
            Self::LogFailed => "log_failed",
            Self::FlushFailed => "flush_failed",
            Self::CompactionFailed => "compaction_failed",
            Self::ReadFailed => "read_failed",
            Self::WriteStalled { .. } => "write_stalled",
            Self::AlreadyExists { .. } => "already_exists",
            Self::Closed => "closed",
            Self::ReadOnly { .. } => "read_only",
            Self::MergeFailed { .. } => "merge_failed",
            Self::TooLarge { .. } => "too_large",
            Self::InvalidArgument { .. } => "invalid_argument",
            Self::Conflict => "conflict",
            Self::WrongKey { .. } => "wrong_key",
            Self::KeyNotFound { .. } => "key_not_found",
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
            Self::Locked { .. } | Self::WriteStalled { .. } | Self::Conflict => true,
            Self::NotFound { .. }
            | Self::InvalidOptions { .. }
            | Self::Corruption { .. }
            | Self::ShortRead { .. }
            | Self::UnknownKeyspace { .. }
            | Self::Unsupported { .. }
            | Self::UnsupportedFormat { .. }
            | Self::SequenceExhausted
            | Self::Memtable { .. }
            | Self::LogFailed
            | Self::FlushFailed
            | Self::CompactionFailed
            | Self::ReadFailed
            | Self::AlreadyExists { .. }
            | Self::Closed
            | Self::ReadOnly { .. }
            | Self::MergeFailed { .. }
            | Self::TooLarge { .. }
            | Self::InvalidArgument { .. }
            | Self::WrongKey { .. }
            | Self::KeyNotFound { .. } => false,
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
