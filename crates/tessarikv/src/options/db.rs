//! Database-wide options.

use tessarikv_constants as limits;

use super::bounds::check_range;
use super::keyspace::KeyspaceOptions;
use crate::{Error, Result};

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

/// Validated database-wide options. Built through [`DbOptions::builder`] or a preset.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DbOptions {
    create_if_missing: bool,
    error_if_exists: bool,
    storage: StorageMode,
    wal_segment_bytes: u64,
    wal_recovery: WalRecovery,
    block_cache_bytes: u64,
    background_threads: u32,
    compaction_bytes_per_second: u64,
    default_keyspace: KeyspaceOptions,
}

impl Default for DbOptions {
    fn default() -> Self {
        Self {
            create_if_missing: true,
            error_if_exists: false,
            storage: StorageMode::Os,
            wal_segment_bytes: limits::DEFAULT_WAL_SEGMENT_BYTES,
            wal_recovery: WalRecovery::PointInTime,
            block_cache_bytes: limits::DEFAULT_BLOCK_CACHE_BYTES,
            background_threads: limits::DEFAULT_BACKGROUND_THREADS,
            compaction_bytes_per_second: 0,
            default_keyspace: KeyspaceOptions::default(),
        }
    }
}

impl DbOptions {
    /// A builder starting from the defaults.
    pub fn builder() -> DbOptionsBuilder {
        Self::default().to_builder()
    }

    /// A builder starting from these options, to adjust a preset.
    pub fn to_builder(&self) -> DbOptionsBuilder {
        DbOptionsBuilder {
            options: self.clone(),
        }
    }

    /// A database held entirely in process memory.
    #[must_use]
    pub fn in_memory() -> Self {
        Self {
            storage: StorageMode::Memory,
            ..Self::default()
        }
    }

    /// Create the database when the path holds none (default `true`).
    #[must_use]
    pub const fn create_if_missing(&self) -> bool {
        self.create_if_missing
    }

    /// Refuse to open a database that already exists (default `false`).
    #[must_use]
    pub const fn error_if_exists(&self) -> bool {
        self.error_if_exists
    }

    /// Where the files live.
    #[must_use]
    pub const fn storage(&self) -> StorageMode {
        self.storage
    }

    /// Size at which a log segment is closed and the next one started, in bytes.
    #[must_use]
    pub const fn wal_segment_bytes(&self) -> u64 {
        self.wal_segment_bytes
    }

    /// What opening does with a damaged log (default [`WalRecovery::PointInTime`]).
    pub const fn wal_recovery(&self) -> WalRecovery {
        self.wal_recovery
    }

    /// Block cache capacity shared by every keyspace, in bytes; zero disables the cache.
    #[must_use]
    pub const fn block_cache_bytes(&self) -> u64 {
        self.block_cache_bytes
    }

    /// Options of the default keyspace: its in-memory table size and the tables it flushes.
    #[must_use]
    pub const fn default_keyspace(&self) -> &KeyspaceOptions {
        &self.default_keyspace
    }

    /// Threads running flushes and compactions.
    #[must_use]
    pub const fn background_threads(&self) -> u32 {
        self.background_threads
    }

    /// The rate compactions write their output tables at, in bytes per second, with up to a
    /// second's worth let through at once; zero (the default) does not pace them. Flushes are
    /// never paced: they free the in-memory tables writes wait for.
    #[must_use]
    pub const fn compaction_bytes_per_second(&self) -> u64 {
        self.compaction_bytes_per_second
    }
}

/// Builds a validated [`DbOptions`].
#[derive(Debug, Clone)]
#[must_use]
pub struct DbOptionsBuilder {
    options: DbOptions,
}

impl DbOptionsBuilder {
    /// Sets [`DbOptions::create_if_missing`].
    pub const fn create_if_missing(mut self, value: bool) -> Self {
        self.options.create_if_missing = value;
        self
    }

    /// Sets [`DbOptions::error_if_exists`].
    pub const fn error_if_exists(mut self, value: bool) -> Self {
        self.options.error_if_exists = value;
        self
    }

    /// Sets [`DbOptions::storage`].
    pub const fn storage(mut self, value: StorageMode) -> Self {
        self.options.storage = value;
        self
    }

    /// Sets [`DbOptions::wal_segment_bytes`].
    pub const fn wal_segment_bytes(mut self, value: u64) -> Self {
        self.options.wal_segment_bytes = value;
        self
    }

    /// Sets [`DbOptions::wal_recovery`].
    pub const fn wal_recovery(mut self, value: WalRecovery) -> Self {
        self.options.wal_recovery = value;
        self
    }

    /// Sets [`DbOptions::block_cache_bytes`].
    pub const fn block_cache_bytes(mut self, value: u64) -> Self {
        self.options.block_cache_bytes = value;
        self
    }

    /// Sets [`DbOptions::background_threads`].
    pub const fn background_threads(mut self, value: u32) -> Self {
        self.options.background_threads = value;
        self
    }

    /// Sets [`DbOptions::compaction_bytes_per_second`].
    pub const fn compaction_bytes_per_second(mut self, value: u64) -> Self {
        self.options.compaction_bytes_per_second = value;
        self
    }

    /// Sets [`DbOptions::default_keyspace`].
    pub fn default_keyspace(mut self, value: KeyspaceOptions) -> Self {
        self.options.default_keyspace = value;
        self
    }

    /// Validates every field and returns the options.
    ///
    /// # Errors
    ///
    /// [`crate::Error::InvalidOptions`] naming the first field outside its bounds, or
    /// `db.error_if_exists` when it is set without `create_if_missing` (no open could succeed).
    pub fn build(self) -> Result<DbOptions> {
        let options = self.options;
        if options.error_if_exists && !options.create_if_missing {
            return Err(Error::InvalidOptions {
                option: "db.error_if_exists",
                reason: "set without create_if_missing, no open could succeed".to_owned(),
            });
        }
        check_range(
            "db.wal_segment_bytes",
            options.wal_segment_bytes,
            limits::MIN_WAL_SEGMENT_BYTES,
            limits::MAX_WAL_SEGMENT_BYTES,
        )?;
        check_range(
            "db.background_threads",
            u64::from(options.background_threads),
            1,
            u64::from(limits::MAX_BACKGROUND_THREADS),
        )?;
        if options.compaction_bytes_per_second != 0 {
            check_range(
                "db.compaction_bytes_per_second",
                options.compaction_bytes_per_second,
                limits::MIN_COMPACTION_BYTES_PER_SECOND,
                limits::MAX_COMPACTION_BYTES_PER_SECOND,
            )?;
        }
        Ok(options)
    }
}
