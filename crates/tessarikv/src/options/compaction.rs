//! How a keyspace's table files are compacted.

use tessarikv_constants as limits;

/// The compaction style of a keyspace.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum CompactionStyle {
    /// Level 0 merges into sorted levels sized from the bottom up: few tables per read and
    /// little space beyond the live data, paid for by rewriting data once per level. The default.
    #[default]
    Leveled,
    /// Tables stay in level 0 and are never merged; once they hold more than `max_table_bytes`,
    /// the oldest are dropped. For data that may expire by age, such as metrics or caches: data
    /// older than what fits is lost, and a key reads its newest kept value or nothing, never an
    /// older one.
    Fifo {
        /// Bytes of table files kept before the oldest go.
        max_table_bytes: u64,
    },
}

impl CompactionStyle {
    /// First in, first out, keeping the default number of bytes.
    #[must_use]
    pub const fn fifo() -> Self {
        Self::Fifo {
            max_table_bytes: limits::DEFAULT_FIFO_MAX_TABLE_BYTES,
        }
    }
}
