//! Options for one keyspace: its memtables and the tables it writes.

use tessarikv_constants as limits;

use super::bounds::check_range;
use crate::Result;

/// Validated options of one keyspace. Built through [`KeyspaceOptions::builder`] or a preset.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyspaceOptions {
    memtable_bytes: u64,
    max_immutable_memtables: u32,
    block_bytes: u64,
    block_restart_interval: u32,
    bloom_bits_per_key: u32,
    level0_compaction_trigger: u32,
    level_base_bytes: u64,
    level_size_multiplier: u32,
    target_file_bytes: u64,
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
            level_base_bytes: limits::DEFAULT_LEVEL_BASE_BYTES,
            level_size_multiplier: limits::DEFAULT_LEVEL_SIZE_MULTIPLIER,
            target_file_bytes: limits::DEFAULT_TARGET_FILE_BYTES,
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
}

/// Builds a validated [`KeyspaceOptions`].
#[derive(Debug, Clone)]
#[must_use]
pub struct KeyspaceOptionsBuilder {
    options: KeyspaceOptions,
}

impl KeyspaceOptionsBuilder {
    /// Sets [`KeyspaceOptions::memtable_bytes`].
    pub const fn memtable_bytes(mut self, value: u64) -> Self {
        self.options.memtable_bytes = value;
        self
    }

    /// Sets [`KeyspaceOptions::max_immutable_memtables`].
    pub const fn max_immutable_memtables(mut self, value: u32) -> Self {
        self.options.max_immutable_memtables = value;
        self
    }

    /// Sets [`KeyspaceOptions::block_bytes`].
    pub const fn block_bytes(mut self, value: u64) -> Self {
        self.options.block_bytes = value;
        self
    }

    /// Sets [`KeyspaceOptions::block_restart_interval`].
    pub const fn block_restart_interval(mut self, value: u32) -> Self {
        self.options.block_restart_interval = value;
        self
    }

    /// Sets [`KeyspaceOptions::bloom_bits_per_key`].
    pub const fn bloom_bits_per_key(mut self, value: u32) -> Self {
        self.options.bloom_bits_per_key = value;
        self
    }

    /// Sets [`KeyspaceOptions::level0_compaction_trigger`].
    pub const fn level0_compaction_trigger(mut self, value: u32) -> Self {
        self.options.level0_compaction_trigger = value;
        self
    }

    /// Sets [`KeyspaceOptions::level_base_bytes`].
    pub const fn level_base_bytes(mut self, value: u64) -> Self {
        self.options.level_base_bytes = value;
        self
    }

    /// Sets [`KeyspaceOptions::level_size_multiplier`].
    pub const fn level_size_multiplier(mut self, value: u32) -> Self {
        self.options.level_size_multiplier = value;
        self
    }

    /// Sets [`KeyspaceOptions::target_file_bytes`].
    pub const fn target_file_bytes(mut self, value: u64) -> Self {
        self.options.target_file_bytes = value;
        self
    }

    /// Validates every field and returns the options.
    ///
    /// # Errors
    ///
    /// [`crate::Error::InvalidOptions`] naming the first field outside its bounds.
    pub fn build(self) -> Result<KeyspaceOptions> {
        let options = self.options;
        check_range(
            "keyspace.memtable_bytes",
            options.memtable_bytes,
            limits::MIN_MEMTABLE_BYTES,
            limits::MAX_MEMTABLE_BYTES,
        )?;
        check_range(
            "keyspace.max_immutable_memtables",
            u64::from(options.max_immutable_memtables),
            1,
            u64::from(limits::MAX_IMMUTABLE_MEMTABLES),
        )?;
        check_range(
            "keyspace.block_bytes",
            options.block_bytes,
            limits::MIN_BLOCK_BYTES,
            limits::MAX_BLOCK_BYTES,
        )?;
        check_range(
            "keyspace.block_restart_interval",
            u64::from(options.block_restart_interval),
            1,
            u64::from(limits::MAX_BLOCK_RESTART_INTERVAL),
        )?;
        check_range(
            "keyspace.bloom_bits_per_key",
            u64::from(options.bloom_bits_per_key),
            0,
            u64::from(limits::MAX_BLOOM_BITS_PER_KEY),
        )?;
        check_range(
            "keyspace.level0_compaction_trigger",
            u64::from(options.level0_compaction_trigger),
            1,
            u64::from(limits::MAX_LEVEL0_COMPACTION_TRIGGER),
        )?;
        check_range(
            "keyspace.level_base_bytes",
            options.level_base_bytes,
            limits::MIN_LEVEL_BASE_BYTES,
            limits::MAX_LEVEL_BASE_BYTES,
        )?;
        check_range(
            "keyspace.level_size_multiplier",
            u64::from(options.level_size_multiplier),
            u64::from(limits::MIN_LEVEL_SIZE_MULTIPLIER),
            u64::from(limits::MAX_LEVEL_SIZE_MULTIPLIER),
        )?;
        check_range(
            "keyspace.target_file_bytes",
            options.target_file_bytes,
            limits::MIN_TARGET_FILE_BYTES,
            limits::MAX_TARGET_FILE_BYTES,
        )?;
        Ok(options)
    }
}
