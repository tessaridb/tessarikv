//! Constants shared by every TessariKV crate.
//!
//! A number with a meaning lives here once, with its unit and the reason it has the value it
//! has. Defaults that a user may change are the starting values of the options in the
//! `tessarikv` crate; limits are hard bounds the format or the engine relies on.

/// How many bits of an internal key's trailer hold the sequence number.
///
/// The trailer is one `u64`: the sequence number in the high 56 bits and the value kind in the
/// low 8, so a version and its kind compare as one integer.
pub const SEQUENCE_BITS: u32 = 56;

/// The largest sequence number the trailer can hold: 2^56 - 1.
///
/// At ten million writes a second this lasts about 228 years, which is why the engine refuses a
/// write past it instead of wrapping.
pub const MAX_SEQUENCE: u64 = (1 << SEQUENCE_BITS) - 1;

/// Bytes in an internal key's trailer.
pub const TRAILER_BYTES: usize = 8;

/// Bytes in one chunk of an in-memory file (64 KiB).
///
/// Large enough that a table block or a log block never spans more than two chunks, small enough
/// that an empty in-memory file costs little.
pub const MEMORY_FILE_CHUNK_BYTES: usize = 64 * 1024;

/// Batches below this many bytes have their log checksum taken in one pass after their sequence
/// number is known (32 KiB); larger ones combine the batch's checksum, taken before, with the
/// sequence number's.
///
/// Combining costs about as much as one pass over 48 KB (~3 µs against ~15 GB/s of hardware
/// CRC-32C, measured on Apple M1), so below this a pass is cheaper; above it the combine keeps
/// the time between claiming sequence numbers and handing them to the log bounded.
pub const DIRECT_CHECKSUM_BYTES: usize = 32 * 1024;

/// Bytes of a table being built that are handed to storage at once (1 MiB).
///
/// A table is written as it is built, so a flush or a compaction holds this much of it in memory
/// at most, besides the block being filled and the table's index and filter, whatever the table's
/// size; large enough that writing it costs few system calls.
pub const TABLE_WRITE_CHUNK_BYTES: usize = 1024 * 1024;

/// Default capacity of one memtable before it is frozen and flushed (64 MiB).
pub const DEFAULT_MEMTABLE_BYTES: u64 = 64 * 1024 * 1024;

/// Smallest memtable the engine accepts (64 KiB): below this a flush per handful of writes costs more than it saves.
pub const MIN_MEMTABLE_BYTES: u64 = 64 * 1024;

/// Largest memtable the engine accepts (4 GiB): an offset inside one memtable's arena fits in 32 bits.
pub const MAX_MEMTABLE_BYTES: u64 = 4 * 1024 * 1024 * 1024;

/// Default number of frozen memtables that may wait for a flush before writes slow down.
pub const DEFAULT_MAX_IMMUTABLE_MEMTABLES: u32 = 2;

/// Largest number of frozen memtables allowed to wait for a flush; each one holds its full size in memory.
pub const MAX_IMMUTABLE_MEMTABLES: u32 = 64;

/// Default target size of a table data block before compression (4 KiB).
pub const DEFAULT_BLOCK_BYTES: u64 = 4 * 1024;

/// Smallest table data block (256 bytes): smaller blocks make the index larger than the data it locates.
pub const MIN_BLOCK_BYTES: u64 = 256;

/// Largest table data block (16 MiB): a read decodes a whole block, so this bounds one read's work.
pub const MAX_BLOCK_BYTES: u64 = 16 * 1024 * 1024;

/// Default number of keys between two restart points in a data block.
///
/// A seek binary-searches the restart points and scans at most this many keys forward.
pub const DEFAULT_BLOCK_RESTART_INTERVAL: u32 = 16;

/// Largest restart interval: a seek scans at most this many keys after its binary search.
pub const MAX_BLOCK_RESTART_INTERVAL: u32 = 1024;

/// Default bloom filter bits per key: about a 1 % false-positive rate.
pub const DEFAULT_BLOOM_BITS_PER_KEY: u32 = 10;

/// Largest bloom filter budget (64 bits per key): beyond it the false-positive rate no longer improves measurably.
///
/// Zero bits per key means no filter.
pub const MAX_BLOOM_BITS_PER_KEY: u32 = 64;

/// The highest Deflate level a keyspace may compress its tables with.
pub const MAX_DEFLATE_LEVEL: u8 = 10;

/// Default number of level-0 tables that starts a compaction of level 0.
pub const DEFAULT_LEVEL0_COMPACTION_TRIGGER: u32 = 4;

/// Largest level-0 compaction trigger: a point read consults every level-0 table, so more would make reads pay for it.
pub const MAX_LEVEL0_COMPACTION_TRIGGER: u32 = 1024;

/// Default number of level-0 tables past which writes are slowed down.
pub const DEFAULT_LEVEL0_SLOWDOWN_TRIGGER: u32 = 20;

/// Default number of level-0 tables at which writes wait for a compaction.
pub const DEFAULT_LEVEL0_STOP_TRIGGER: u32 = 36;

/// Default number of compactions a keyspace runs at once.
pub const DEFAULT_MAX_COMPACTIONS: u32 = 4;

/// Largest number of compactions a keyspace runs at once.
pub const MAX_COMPACTIONS: u32 = 64;

/// Default bytes a keyspace compacted first in, first out keeps before dropping its oldest tables.
pub const DEFAULT_FIFO_MAX_TABLE_BYTES: u64 = 1 << 30;

/// Smallest bytes a first-in-first-out keyspace keeps.
pub const MIN_FIFO_MAX_TABLE_BYTES: u64 = 1 << 20;

/// Largest bytes a first-in-first-out keyspace keeps.
pub const MAX_FIFO_MAX_TABLE_BYTES: u64 = 1 << 50;

/// Default size ratio, in percent, within which universal compaction merges sorted runs.
pub const DEFAULT_UNIVERSAL_SIZE_RATIO_PERCENT: u32 = 1;

/// Largest universal size ratio, in percent.
pub const MAX_UNIVERSAL_SIZE_RATIO_PERCENT: u32 = 1000;

/// Default fewest sorted runs a universal compaction merges by size.
pub const DEFAULT_UNIVERSAL_MIN_MERGE: u32 = 2;

/// Largest fewest-runs setting of universal compaction.
pub const MAX_UNIVERSAL_MIN_MERGE: u32 = 64;

/// Default space amplification, in percent of the oldest run, at which universal compaction
/// merges everything.
pub const DEFAULT_UNIVERSAL_MAX_SIZE_AMPLIFICATION_PERCENT: u32 = 200;

/// Smallest universal space-amplification bound, in percent.
pub const MIN_UNIVERSAL_MAX_SIZE_AMPLIFICATION_PERCENT: u32 = 10;

/// Largest universal space-amplification bound, in percent.
pub const MAX_UNIVERSAL_MAX_SIZE_AMPLIFICATION_PERCENT: u32 = 10_000;

/// Default bytes compaction may owe before writes slow down.
pub const DEFAULT_SOFT_PENDING_COMPACTION_BYTES: u64 = 64 << 30;

/// Default bytes compaction may owe before writes stop.
pub const DEFAULT_HARD_PENDING_COMPACTION_BYTES: u64 = 256 << 30;

/// Smallest pending-compaction limit.
pub const MIN_PENDING_COMPACTION_BYTES: u64 = 1 << 20;

/// Largest pending-compaction limit.
pub const MAX_PENDING_COMPACTION_BYTES: u64 = 1 << 50;

/// Default number of parts a compaction is split into, merged side by side.
pub const DEFAULT_MAX_SUBCOMPACTIONS: u32 = 4;

/// Largest number of parts a compaction is split into.
pub const MAX_SUBCOMPACTIONS: u32 = 64;

/// Largest level-0 slowdown or stop trigger.
pub const MAX_LEVEL0_STALL_TRIGGER: u32 = 4096;

/// Default target size of the first level below level 0 (256 MiB).
pub const DEFAULT_LEVEL_BASE_BYTES: u64 = 256 * 1024 * 1024;

/// Smallest target size of the first level below level 0 (64 KiB), the smallest memtable.
pub const MIN_LEVEL_BASE_BYTES: u64 = 64 * 1024;

/// Largest target size of the first level below level 0 (1 TiB).
pub const MAX_LEVEL_BASE_BYTES: u64 = 1024 * 1024 * 1024 * 1024;

/// Default factor by which each level's target size exceeds the one above it.
pub const DEFAULT_LEVEL_SIZE_MULTIPLIER: u32 = 10;

/// Smallest level size multiplier: below 2 the levels stop growing.
pub const MIN_LEVEL_SIZE_MULTIPLIER: u32 = 2;

/// Largest level size multiplier: beyond it one compaction rewrites far more than it moves down.
pub const MAX_LEVEL_SIZE_MULTIPLIER: u32 = 100;

/// Default size at which a compaction closes one output table and starts the next (64 MiB).
pub const DEFAULT_TARGET_FILE_BYTES: u64 = 64 * 1024 * 1024;

/// Smallest compaction output table (4 KiB): one data block.
pub const MIN_TARGET_FILE_BYTES: u64 = 4 * 1024;

/// Largest compaction output table (4 GiB).
pub const MAX_TARGET_FILE_BYTES: u64 = 4 * 1024 * 1024 * 1024;

/// Default block cache capacity (512 MiB).
pub const DEFAULT_BLOCK_CACHE_BYTES: u64 = 512 * 1024 * 1024;

/// Default size at which a log segment is closed and a new one started (64 MiB).
pub const DEFAULT_WAL_SEGMENT_BYTES: u64 = 64 * 1024 * 1024;

/// Smallest log segment (1 MiB): a smaller one rolls over on every few commits.
pub const MIN_WAL_SEGMENT_BYTES: u64 = 1024 * 1024;

/// Largest log segment (4 GiB): an offset inside one segment fits in 32 bits.
pub const MAX_WAL_SEGMENT_BYTES: u64 = 4 * 1024 * 1024 * 1024;

/// Levels of a keyspace's table tree: level 0 takes flushed tables, levels 1 to 6 compacted ones.
pub const LEVELS: usize = 7;

/// Default number of background threads for flush and compaction.
pub const DEFAULT_BACKGROUND_THREADS: u32 = 4;

/// Largest number of background threads the engine starts.
pub const MAX_BACKGROUND_THREADS: u32 = 1024;

/// Smallest compaction write rate; zero means compaction writes are not paced.
pub const MIN_COMPACTION_BYTES_PER_SECOND: u64 = 1024 * 1024;

/// Largest compaction write rate.
pub const MAX_COMPACTION_BYTES_PER_SECOND: u64 = 1 << 40;

/// Default rate of writes while level 0 is past its slowdown trigger.
pub const DEFAULT_DELAYED_WRITE_BYTES_PER_SECOND: u64 = 16 * 1024 * 1024;

/// Smallest rate of writes while level 0 is past its slowdown trigger.
pub const MIN_DELAYED_WRITE_BYTES_PER_SECOND: u64 = 1024;

/// Largest rate of writes while level 0 is past its slowdown trigger.
pub const MAX_DELAYED_WRITE_BYTES_PER_SECOND: u64 = 1 << 40;

/// The largest key the engine accepts (8 MiB).
///
/// Keys are held in index blocks and compared on every seek; a key this large is already a
/// design problem upstream, and the bound keeps a length field from describing nonsense.
pub const MAX_KEY_BYTES: usize = 8 * 1024 * 1024;

/// The largest value the engine accepts (1 GiB).
pub const MAX_VALUE_BYTES: usize = 1024 * 1024 * 1024;

/// The largest encoded write batch the engine accepts (2 GiB).
///
/// It bounds what one atomic commit may carry and what a decoder of a logged batch will accept.
pub const MAX_BATCH_BYTES: usize = 2 * 1024 * 1024 * 1024;

/// The largest payload one log frame carries: a batch of [`MAX_BATCH_BYTES`] and the first
/// sequence number it was committed at.
///
/// A decoder refuses a frame whose length field claims more, before it looks at the bytes.
pub const MAX_LOG_PAYLOAD_BYTES: usize = MAX_BATCH_BYTES + 8;

/// Slots in the commit sequencer's ring: how many sequence numbers a writer may run ahead of
/// the oldest commit still being applied before its completion waits.
pub const COMMIT_RING_SLOTS: usize = 4096;

/// Buckets of the table that remembers, per hashed key, the newest commit that wrote it, which
/// a committing transaction checks for writes after its snapshot (8 bytes each). Keys sharing a
/// bucket may refuse a commit that wrote neither; more buckets make that rarer.
pub const CONFLICT_BUCKETS: usize = 1 << 16;

/// The largest number of keyspaces one database may hold.
///
/// A keyspace id is a `u32` on disk; this bound is what the engine sizes per-keyspace state for.
pub const MAX_KEYSPACES: u32 = 4096;

/// Bytes in one chunk of a memtable's arena (128 KiB).
///
/// An allocation larger than a quarter chunk gets its own allocation, so the bytes wasted at a
/// chunk's end are at most a quarter chunk per crossing.
pub const ARENA_CHUNK_BYTES: usize = 128 * 1024;

/// Tallest tower in a memtable skiplist: with branching 4, enough for 4^12 = 16.7 million
/// entries before searches lengthen.
pub const SKIPLIST_MAX_HEIGHT: usize = 12;

/// Each skiplist level holds about one in this many of the level below.
pub const SKIPLIST_BRANCHING: u32 = 4;

/// Committed batches that may wait in the queue to the log writer; a writer that finds it full
/// waits for room. Also the most the log writer takes from the queue for one write.
pub const LOG_QUEUE_ENTRIES: usize = 1024;
