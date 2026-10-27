//! The bounds every keyspace option is checked against, and how the level-0 triggers relate.

use tessarikv_constants as limits;
use tessarikv_format::Compression;

use super::compaction::CompactionStyle;

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
    if let Some(length) = options.prefix_length() {
        check_range(
            "keyspace.prefix_length",
            u64::from(length),
            1,
            u64::from(limits::MAX_PREFIX_LENGTH),
        )?;
    }
    if let Some(bytes) = options.blob_threshold() {
        check_range(
            "keyspace.blob_threshold",
            bytes,
            limits::MIN_BLOB_THRESHOLD,
            u64::try_from(limits::MAX_VALUE_BYTES).unwrap_or(u64::MAX),
        )?;
    }
    check_range(
        "keyspace.blob_gc_age_cutoff_percent",
        u64::from(options.blob_gc_age_cutoff_percent()),
        0,
        100,
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
    match options.compaction() {
        CompactionStyle::Fifo { max_table_bytes } => check_range(
            "keyspace.compaction.max_table_bytes",
            max_table_bytes,
            limits::MIN_FIFO_MAX_TABLE_BYTES,
            limits::MAX_FIFO_MAX_TABLE_BYTES,
        )?,
        CompactionStyle::Universal {
            size_ratio_percent,
            min_merge,
            max_size_amplification_percent,
        } => {
            check_range(
                "keyspace.compaction.size_ratio_percent",
                u64::from(size_ratio_percent),
                0,
                u64::from(limits::MAX_UNIVERSAL_SIZE_RATIO_PERCENT),
            )?;
            check_range(
                "keyspace.compaction.min_merge",
                u64::from(min_merge),
                2,
                u64::from(limits::MAX_UNIVERSAL_MIN_MERGE),
            )?;
            check_range(
                "keyspace.compaction.max_size_amplification_percent",
                u64::from(max_size_amplification_percent),
                u64::from(limits::MIN_UNIVERSAL_MAX_SIZE_AMPLIFICATION_PERCENT),
                u64::from(limits::MAX_UNIVERSAL_MAX_SIZE_AMPLIFICATION_PERCENT),
            )?;
        }
        CompactionStyle::Leveled => {}
    }
    check_range(
        "keyspace.hard_pending_compaction_bytes",
        options.hard_pending_compaction_bytes(),
        limits::MIN_PENDING_COMPACTION_BYTES,
        limits::MAX_PENDING_COMPACTION_BYTES,
    )?;
    check_range(
        "keyspace.soft_pending_compaction_bytes",
        options.soft_pending_compaction_bytes(),
        limits::MIN_PENDING_COMPACTION_BYTES,
        options.hard_pending_compaction_bytes(),
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
    check_codec("keyspace.compression.level", options.compression())?;
    if let Some(bottom) = options.bottommost_compression() {
        check_codec("keyspace.bottommost_compression.level", bottom)?;
    }
    Ok(())
}

/// Checks the level of a Deflate codec; the other codecs have no setting to check.
fn check_codec(option: &'static str, codec: Compression) -> Result<()> {
    match codec {
        Compression::Deflate { level } => check_range(
            option,
            u64::from(level),
            0,
            u64::from(limits::MAX_DEFLATE_LEVEL),
        ),
        _ => Ok(()),
    }
}
