//! Sizes and bounds of the write-ahead log and its writer.

/// Default size at which a log segment is closed and a new one started (64 MiB).
pub const DEFAULT_WAL_SEGMENT_BYTES: u64 = 64 * 1024 * 1024;

/// Smallest log segment (1 MiB): a smaller one rolls over on every few commits.
pub const MIN_WAL_SEGMENT_BYTES: u64 = 1024 * 1024;

/// Largest log segment (4 GiB): an offset inside one segment fits in 32 bits.
pub const MAX_WAL_SEGMENT_BYTES: u64 = 4 * 1024 * 1024 * 1024;

/// Committed batches that may wait in the queue to the log writer; a writer that finds it full
/// waits for room. Also the most the log writer takes from the queue for one write.
pub const LOG_QUEUE_ENTRIES: usize = 1024;

/// Syncs of the log that may run at once: a writer that asks for durability while a sync is
/// running gets a sync of its own instead of waiting for that one to finish. Their results are
/// still taken in the order they started.
pub const LOG_SYNCS_IN_FLIGHT: usize = 4;
