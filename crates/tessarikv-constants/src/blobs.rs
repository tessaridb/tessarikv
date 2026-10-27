//! Bounds and defaults of the values a keyspace keeps apart from its tables, in blob files.

/// Smallest value size a keyspace may keep in blob files: a blob index takes 24 bytes in the
/// table, so a value much shorter than a few times that saves nothing by moving out.
pub const MIN_BLOB_THRESHOLD: u64 = 64;

/// Default share, in percent, of a keyspace's blob files — the oldest by number — whose values
/// a compaction moves into a new blob file as it passes them: a quarter, so a blob file's dead
/// values are reclaimed after about four rounds of compaction over its live ones.
pub const DEFAULT_BLOB_GC_AGE_CUTOFF_PERCENT: u32 = 25;
