//! What can stop an inspection.

use std::path::PathBuf;

/// Why an inspection stopped.
#[derive(Debug, thiserror::Error)]
pub enum InspectError {
    /// The command line asked for something this tool does not do.
    #[error("{0}\n\n{USAGE}")]
    Usage(String),
    /// A file could not be read.
    #[error("cannot read {path}: {source}")]
    Read {
        /// The file.
        path: PathBuf,
        /// The operating system's error.
        source: std::io::Error,
    },
    /// A file's bytes are not what its kind holds: damaged, encrypted, or of a later format.
    #[error("{path} is not a readable {what}: {detail}")]
    Unreadable {
        /// The file.
        path: PathBuf,
        /// What it should hold.
        what: &'static str,
        /// What was wrong.
        detail: String,
    },
    /// The engine refused the database.
    #[error(transparent)]
    Engine(#[from] tessarikv::Error),
    /// Writing the output failed.
    #[error("cannot write the output: {0}")]
    Output(#[from] std::io::Error),
}

/// The result of an inspection.
pub type Result<T> = std::result::Result<T, InspectError>;

/// How the tool is called.
pub const USAGE: &str = "usage: tessarikv-inspect <command> <path>
  manifest <database directory>   store format, keyspaces and table files per level
  table <table file>              the table's entries: key, sequence, kind, value
  scan <database directory>       the live keys and values of every keyspace, read-only";

/// Reads all of `path`.
pub fn read(path: &std::path::Path) -> Result<Vec<u8>> {
    std::fs::read(path).map_err(|source| InspectError::Read {
        path: path.to_path_buf(),
        source,
    })
}
