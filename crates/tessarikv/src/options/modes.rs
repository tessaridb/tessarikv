//! Where a database keeps its files and what opening it does with a damaged log.

/// Where the database keeps its files.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum StorageMode {
    /// The operating system's file system, at the path the database is opened with.
    Os,
    /// Process memory only: the same engine, nothing survives the process.
    Memory,
}

/// What opening a database does with a log whose end is damaged.
///
/// A crash can leave the last frame of the log half written; after a power loss everything not
/// yet synced may be missing or torn. Which response is right depends on whether something
/// outside this database can supply what a damaged log lost.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[non_exhaustive]
pub enum WalRecovery {
    /// Recover every batch up to the first damaged frame, discard the rest of the log, and open:
    /// the database is as it was at some moment before the crash, and every write acknowledged at
    /// the level that survives that crash is in it. The default.
    #[default]
    PointInTime,
    /// Refuse to open when any frame is damaged, leaving the files untouched for an operator.
    Absolute,
}
