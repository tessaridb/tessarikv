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
    /// older one. Opening a keyspace that holds tables below level 0 (written in another style)
    /// with it is refused: it would never remove them.
    Fifo {
        /// Bytes of table files kept before the oldest go.
        max_table_bytes: u64,
    },
    /// Every level-0 table is a sorted run, and so is the bottom level; runs of similar size are
    /// merged, newest first, and everything is merged into the bottom once the runs above it
    /// hold `max_size_amplification_percent` of its size. Fewer rewrites than leveled, paid for
    /// in space (up to that bound beyond the live data, and twice the data during a full merge)
    /// and in tables per read.
    Universal {
        /// A run joins a merge when it is at most this many percent larger than the runs merged
        /// before it.
        size_ratio_percent: u32,
        /// Fewest runs a merge by size takes.
        min_merge: u32,
        /// Bytes above the bottom, in percent of the bottom's, at which everything is merged.
        max_size_amplification_percent: u32,
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

    /// Universal, with the default ratio, run count and amplification bound.
    #[must_use]
    pub const fn universal() -> Self {
        Self::Universal {
            size_ratio_percent: limits::DEFAULT_UNIVERSAL_SIZE_RATIO_PERCENT,
            min_merge: limits::DEFAULT_UNIVERSAL_MIN_MERGE,
            max_size_amplification_percent:
                limits::DEFAULT_UNIVERSAL_MAX_SIZE_AMPLIFICATION_PERCENT,
        }
    }
}
