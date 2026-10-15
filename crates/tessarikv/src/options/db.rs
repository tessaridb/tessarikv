//! Database-wide options.

use tessarikv_constants as limits;

use super::bounds::check_range;
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

/// Validated database-wide options. Built through [`DbOptions::builder`] or a preset.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DbOptions {
    create_if_missing: bool,
    error_if_exists: bool,
    storage: StorageMode,
    wal_segment_bytes: u64,
    block_cache_bytes: u64,
    background_threads: u32,
}

impl Default for DbOptions {
    fn default() -> Self {
        Self {
            create_if_missing: true,
            error_if_exists: false,
            storage: StorageMode::Os,
            wal_segment_bytes: limits::DEFAULT_WAL_SEGMENT_BYTES,
            block_cache_bytes: limits::DEFAULT_BLOCK_CACHE_BYTES,
            background_threads: limits::DEFAULT_BACKGROUND_THREADS,
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

    /// Block cache capacity shared by every keyspace, in bytes; zero disables the cache.
    #[must_use]
    pub const fn block_cache_bytes(&self) -> u64 {
        self.block_cache_bytes
    }

    /// Threads running flushes and compactions.
    #[must_use]
    pub const fn background_threads(&self) -> u32 {
        self.background_threads
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
        Ok(options)
    }
}
