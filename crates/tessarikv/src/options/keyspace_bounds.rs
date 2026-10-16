//! The bounds every keyspace option is checked against, and how the level-0 triggers relate.

use tessarikv_constants as limits;

use super::KeyspaceOptions;
use super::bounds::check_range;
use crate::Result;

/// Checks every field of `options` against its bounds.
///
/// # Errors
///
/// [`crate::Error::InvalidOptions`] naming the first field outside its bounds.
pub(super) fn validate(options: &KeyspaceOptions) -> Result<()> {
    check_range(
        "keyspace.memtable_bytes",
        options.memtable_bytes(),
        limits::MIN_MEMTABLE_BYTES,
        limits::MAX_MEMTABLE_BYTES,
    )?;
    check_range(
        "keyspace.max_immutable_memtables",
        u64::from(options.max_immutable_memtables()),
        1,
        u64::from(limits::MAX_IMMUTABLE_MEMTABLES),
    )?;
    check_range(
        "keyspace.block_bytes",
        options.block_bytes(),
        limits::MIN_BLOCK_BYTES,
        limits::MAX_BLOCK_BYTES,
    )?;
    check_range(
        "keyspace.block_restart_interval",
        u64::from(options.block_restart_interval()),
        1,
        u64::from(limits::MAX_BLOCK_RESTART_INTERVAL),
    )?;
    check_range(
        "keyspace.bloom_bits_per_key",
        u64::from(options.bloom_bits_per_key()),
        0,
        u64::from(limits::MAX_BLOOM_BITS_PER_KEY),
    )?;
    check_range(
        "keyspace.level0_compaction_trigger",
        u64::from(options.level0_compaction_trigger()),
        1,
        u64::from(limits::MAX_LEVEL0_COMPACTION_TRIGGER),
    )?;
    check_range(
        "keyspace.level0_slowdown_trigger",
        u64::from(options.level0_slowdown_trigger()),
        u64::from(options.level0_compaction_trigger()),
        u64::from(limits::MAX_LEVEL0_STALL_TRIGGER),
    )?;
    check_range(
        "keyspace.level0_stop_trigger",
        u64::from(options.level0_stop_trigger()),
        u64::from(options.level0_slowdown_trigger()),
        u64::from(limits::MAX_LEVEL0_STALL_TRIGGER),
    )?;
    check_range(
        "keyspace.max_compactions",
        u64::from(options.max_compactions()),
        1,
        u64::from(limits::MAX_COMPACTIONS),
    )?;
    check_range(
        "keyspace.max_subcompactions",
        u64::from(options.max_subcompactions()),
        1,
        u64::from(limits::MAX_SUBCOMPACTIONS),
    )?;
    check_range(
        "keyspace.level_base_bytes",
        options.level_base_bytes(),
        limits::MIN_LEVEL_BASE_BYTES,
        limits::MAX_LEVEL_BASE_BYTES,
    )?;
    check_range(
        "keyspace.level_size_multiplier",
        u64::from(options.level_size_multiplier()),
        u64::from(limits::MIN_LEVEL_SIZE_MULTIPLIER),
        u64::from(limits::MAX_LEVEL_SIZE_MULTIPLIER),
    )?;
    check_range(
        "keyspace.target_file_bytes",
        options.target_file_bytes(),
        limits::MIN_TARGET_FILE_BYTES,
        limits::MAX_TARGET_FILE_BYTES,
    )?;
    Ok(())
}
