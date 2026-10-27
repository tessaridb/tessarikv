//! Where a database reads a table block that is not in the block cache.

use super::db::{DbOptions, DbOptionsBuilder};

impl DbOptions {
    /// Whether a block missing from the cache is read on the task that asked for it, on the
    /// runtime worker running that task; default `true`. The read then costs one positional
    /// read and nothing more, and reads of many tasks keep a short tail.
    ///
    /// The worker cannot run its other tasks while the read waits: a block in the operating
    /// system's page cache is copied in microseconds, but one that must come from the device
    /// holds the worker for the device's latency. Set `false` to hand such reads to the runtime
    /// — a multi-thread runtime's worker then gives its other tasks to another thread first,
    /// and any other runtime reads on a blocking thread — at the cost of a thread hand-off per
    /// read. A read the storage promises not to block (a page-cache read on Linux, every read of
    /// memory) is made on the task either way.
    #[must_use]
    pub const fn block_reads_on_worker(&self) -> bool {
        self.block_reads_on_worker
    }
}

impl DbOptionsBuilder {
    /// Sets [`DbOptions::block_reads_on_worker`].
    pub const fn block_reads_on_worker(mut self, value: bool) -> Self {
        self.options.block_reads_on_worker = value;
        self
    }
}
