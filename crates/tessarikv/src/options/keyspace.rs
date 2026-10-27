//! Options for one keyspace: its memtables and the tables it writes.

use std::sync::Arc;

use tessarikv_constants::{self as limits, LEVELS};
use tessarikv_format::Compression;

use super::compaction::CompactionStyle;
use super::compaction_filter::{CompactionFilter, FilterHandle};
use super::keyspace_builder::KeyspaceOptionsBuilder;

/// Validated options of one keyspace. Built through [`KeyspaceOptions::builder`] or a preset.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyspaceOptions {
    pub(super) memtable_bytes: u64,
    pub(super) max_immutable_memtables: u32,
    pub(super) block_bytes: u64,
    pub(super) block_restart_interval: u32,
    pub(super) bloom_bits_per_key: u32,
    pub(super) level0_compaction_trigger: u32,
    pub(super) level0_slowdown_trigger: u32,
    pub(super) level0_stop_trigger: u32,
    pub(super) max_compactions: u32,
    pub(super) max_subcompactions: u32,
    pub(super) disable_auto_compactions: bool,
    pub(super) soft_pending_compaction_bytes: u64,
    pub(super) hard_pending_compaction_bytes: u64,
    pub(super) compaction: CompactionStyle,
    pub(super) level_base_bytes: u64,
    pub(super) level_size_multiplier: u32,
    pub(super) target_file_bytes: u64,
    pub(super) compression: Compression,
    pub(super) bottommost_compression: Option<Compression>,
    pub(super) compaction_filter: Option<FilterHandle>,
    pub(super) prefix_length: Option<u32>,
    pub(super) blob_threshold: Option<u64>,
    pub(super) blob_gc_age_cutoff_percent: u32,
}

impl Default for KeyspaceOptions {
    fn default() -> Self {
        Self {
            memtable_bytes: limits::DEFAULT_MEMTABLE_BYTES,
            max_immutable_memtables: limits::DEFAULT_MAX_IMMUTABLE_MEMTABLES,
            block_bytes: limits::DEFAULT_BLOCK_BYTES,
            block_restart_interval: limits::DEFAULT_BLOCK_RESTART_INTERVAL,
            bloom_bits_per_key: limits::DEFAULT_BLOOM_BITS_PER_KEY,
            level0_compaction_trigger: limits::DEFAULT_LEVEL0_COMPACTION_TRIGGER,
            level0_slowdown_trigger: limits::DEFAULT_LEVEL0_SLOWDOWN_TRIGGER,
            level0_stop_trigger: limits::DEFAULT_LEVEL0_STOP_TRIGGER,
            max_compactions: limits::DEFAULT_MAX_COMPACTIONS,
            max_subcompactions: limits::DEFAULT_MAX_SUBCOMPACTIONS,
            disable_auto_compactions: false,
            soft_pending_compaction_bytes: limits::DEFAULT_SOFT_PENDING_COMPACTION_BYTES,
            hard_pending_compaction_bytes: limits::DEFAULT_HARD_PENDING_COMPACTION_BYTES,
            compaction: CompactionStyle::Leveled,
            level_base_bytes: limits::DEFAULT_LEVEL_BASE_BYTES,
            level_size_multiplier: limits::DEFAULT_LEVEL_SIZE_MULTIPLIER,
            target_file_bytes: limits::DEFAULT_TARGET_FILE_BYTES,
            compression: Compression::Lz4,
            bottommost_compression: None,
            compaction_filter: None,
            prefix_length: None,
            blob_threshold: None,
            blob_gc_age_cutoff_percent: limits::DEFAULT_BLOB_GC_AGE_CUTOFF_PERCENT,
        }
    }
}

impl KeyspaceOptions {
    /// A builder starting from the defaults.
    pub fn builder() -> KeyspaceOptionsBuilder {
        Self::default().to_builder()
    }

    /// A builder starting from these options, to adjust a preset.
    pub fn to_builder(&self) -> KeyspaceOptionsBuilder {
        KeyspaceOptionsBuilder {
            options: self.clone(),
        }
    }

    /// Mostly point reads: a larger bloom filter (16 bits per key, about 0.1 % false positives)
    /// so a read of an absent key touches almost no table.
    #[must_use]
    pub fn point_lookup() -> Self {
        Self {
            bloom_bits_per_key: 16,
            ..Self::default()
        }
    }

    /// Mostly writes: 256 MiB memtables and up to four waiting for a flush, so bursts are absorbed
    /// in memory and flushes write fewer, larger tables.
    #[must_use]
    pub fn write_heavy() -> Self {
        Self {
            memtable_bytes: 256 * 1024 * 1024,
            max_immutable_memtables: 4,
            ..Self::default()
        }
    }

    /// One large load: 512 MiB memtables, up to six waiting, and 64 KiB blocks that compress and
    /// scan well.
    #[must_use]
    pub fn bulk_load() -> Self {
        Self {
            memtable_bytes: 512 * 1024 * 1024,
            max_immutable_memtables: 6,
            block_bytes: 64 * 1024,
            ..Self::default()
        }
    }

    /// Bytes a memtable holds before it is frozen and queued for flush.
    #[must_use]
    pub const fn memtable_bytes(&self) -> u64 {
        self.memtable_bytes
    }

    /// Frozen memtables allowed to wait for a flush before writes slow down.
    #[must_use]
    pub const fn max_immutable_memtables(&self) -> u32 {
        self.max_immutable_memtables
    }

    /// Target size of a table data block before compression, in bytes.
    #[must_use]
    pub const fn block_bytes(&self) -> u64 {
        self.block_bytes
    }

    /// Keys between two restart points in a data block.
    #[must_use]
    pub const fn block_restart_interval(&self) -> u32 {
        self.block_restart_interval
    }

    /// Bloom filter bits per key; zero writes no filter.
    #[must_use]
    pub const fn bloom_bits_per_key(&self) -> u32 {
        self.bloom_bits_per_key
    }

    /// Level-0 tables that start a compaction of level 0 into the levels below.
    #[must_use]
    pub const fn level0_compaction_trigger(&self) -> u32 {
        self.level0_compaction_trigger
    }

    /// Level-0 tables at which each write is delayed a little, so compaction catches up before
    /// writes must stop. At least the compaction trigger.
    #[must_use]
    pub const fn level0_slowdown_trigger(&self) -> u32 {
        self.level0_slowdown_trigger
    }

    /// Level-0 tables at which writes wait until a compaction takes level 0 back under it. At
    /// least the slowdown trigger.
    #[must_use]
    pub const fn level0_stop_trigger(&self) -> u32 {
        self.level0_stop_trigger
    }

    /// Compactions the keyspace runs at once. Each takes tables and a key range no other
    /// running compaction touches; level 0 is compacted by one at a time.
    #[must_use]
    pub const fn max_compactions(&self) -> u32 {
        self.max_compactions
    }

    /// Parts one compaction is split into at most, each a key range merged on its own thread;
    /// a compaction whose input tables all start at one key is not split.
    #[must_use]
    pub const fn max_subcompactions(&self) -> u32 {
        self.max_subcompactions
    }

    /// The filter compaction asks which values to keep (default none); see
    /// [`CompactionFilter`] for what it is shown.
    #[must_use]
    pub fn compaction_filter(&self) -> Option<&Arc<dyn CompactionFilter>> {
        self.compaction_filter.as_ref().map(|handle| &handle.0)
    }

    /// Whether the engine never starts a compaction of this keyspace by itself (default
    /// `false`), for bulk loads: [`crate::Db::compact_range`] still runs, and the level-0
    /// slowdown and stop triggers still hold writes, so a load that passes them needs a manual
    /// compaction to go on.
    #[must_use]
    pub const fn disable_auto_compactions(&self) -> bool {
        self.disable_auto_compactions
    }

    /// Bytes compaction may owe (estimated after every install) before writes are paced as
    /// past the level-0 slowdown trigger.
    #[must_use]
    pub const fn soft_pending_compaction_bytes(&self) -> u64 {
        self.soft_pending_compaction_bytes
    }

    /// Bytes compaction may owe before writes wait for a compaction, as at the level-0 stop
    /// trigger.
    #[must_use]
    pub const fn hard_pending_compaction_bytes(&self) -> u64 {
        self.hard_pending_compaction_bytes
    }

    /// How the keyspace's table files are compacted; the level and stall options above apply
    /// to [`CompactionStyle::Leveled`].
    #[must_use]
    pub const fn compaction(&self) -> CompactionStyle {
        self.compaction
    }

    /// Target size of the first level below level 0 that holds data, in bytes. Targets are
    /// computed from the bottom level up, so data first goes to the bottom level and the levels
    /// above it fill as it grows.
    #[must_use]
    pub const fn level_base_bytes(&self) -> u64 {
        self.level_base_bytes
    }

    /// Factor by which each level's target size exceeds the one above it.
    #[must_use]
    pub const fn level_size_multiplier(&self) -> u32 {
        self.level_size_multiplier
    }

    /// Size at which a compaction closes one output table and starts the next, in bytes. A table
    /// may exceed it by the versions of one key: a key's versions never span two tables.
    #[must_use]
    pub const fn target_file_bytes(&self) -> u64 {
        self.target_file_bytes
    }

    /// How tables are compressed, except those written to the bottom level when
    /// [`Self::bottommost_compression`] names another codec. A block is stored compressed only
    /// when that saves at least an eighth of it.
    #[must_use]
    pub const fn compression(&self) -> Compression {
        self.compression
    }

    /// How tables written to the bottom level are compressed, where most of a keyspace's data
    /// ends up and is rewritten least often: a slower codec with a better ratio pays off there.
    /// `None` compresses them like the other levels.
    #[must_use]
    pub const fn bottommost_compression(&self) -> Option<Compression> {
        self.bottommost_compression
    }

    /// The codec of a table written to `level`.
    pub(crate) fn compression_at(&self, level: usize) -> Compression {
        match self.bottommost_compression {
            Some(bottom) if level.saturating_add(1) == LEVELS => bottom,
            _ => self.compression,
        }
    }
}
