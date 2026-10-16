//! Database-wide bounds on resources: in-memory tables and open table files.

use tessarikv_constants as limits;

use super::bounds::check_range;
use super::db::{DbOptions, DbOptionsBuilder};
use crate::Result;

impl DbOptions {
    /// The most bytes the newest in-memory tables of every keyspace may hold together; when
    /// they reach it they are frozen and flushed as if one of them were full. Zero (the
    /// default) sets no ceiling, leaving each keyspace to its own
    /// [`crate::KeyspaceOptions::memtable_bytes`]. In-memory tables then take at most this
    /// much for every epoch a keyspace lets wait for its flush, plus the newest one.
    #[must_use]
    pub const fn total_memtable_bytes(&self) -> u64 {
        self.total_memtable_bytes
    }

    /// The most table files kept open at once; zero (the default) keeps every table file open
    /// while it is read. With a bound, a read of a file let go opens it again, and the files
    /// open at once are at most the bound plus the reads in flight. Each table's index and
    /// filter stay in memory either way.
    #[must_use]
    pub const fn max_open_files(&self) -> u32 {
        self.max_open_files
    }
}

impl DbOptionsBuilder {
    /// Sets [`DbOptions::total_memtable_bytes`].
    pub const fn total_memtable_bytes(mut self, value: u64) -> Self {
        self.options.total_memtable_bytes = value;
        self
    }

    /// Sets [`DbOptions::max_open_files`].
    pub const fn max_open_files(mut self, value: u32) -> Self {
        self.options.max_open_files = value;
        self
    }
}

/// Refuses a ceiling below the smallest table a keyspace may have; zero sets none.
pub(super) fn check(options: &DbOptions) -> Result<()> {
    if options.total_memtable_bytes == 0 {
        return Ok(());
    }
    check_range(
        "db.total_memtable_bytes",
        options.total_memtable_bytes,
        limits::MIN_MEMTABLE_BYTES,
        u64::MAX,
    )
}
