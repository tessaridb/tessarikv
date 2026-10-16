//! The builder of [`KeyspaceOptions`]: every field settable, checked once by `build()`.

use super::keyspace::KeyspaceOptions;
use super::keyspace_bounds::validate;
use crate::Result;

/// Builds a validated [`KeyspaceOptions`].
#[derive(Debug, Clone)]
#[must_use]
pub struct KeyspaceOptionsBuilder {
    pub(super) options: KeyspaceOptions,
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

    /// Sets [`KeyspaceOptions::level0_slowdown_trigger`].
    pub const fn level0_slowdown_trigger(mut self, value: u32) -> Self {
        self.options.level0_slowdown_trigger = value;
        self
    }

    /// Sets [`KeyspaceOptions::level0_stop_trigger`].
    pub const fn level0_stop_trigger(mut self, value: u32) -> Self {
        self.options.level0_stop_trigger = value;
        self
    }

    /// Sets [`KeyspaceOptions::max_compactions`].
    pub const fn max_compactions(mut self, value: u32) -> Self {
        self.options.max_compactions = value;
        self
    }

    /// Sets [`KeyspaceOptions::max_subcompactions`].
    pub const fn max_subcompactions(mut self, value: u32) -> Self {
        self.options.max_subcompactions = value;
        self
    }

    /// Sets [`KeyspaceOptions::soft_pending_compaction_bytes`].
    pub const fn soft_pending_compaction_bytes(mut self, value: u64) -> Self {
        self.options.soft_pending_compaction_bytes = value;
        self
    }

    /// Sets [`KeyspaceOptions::hard_pending_compaction_bytes`].
    pub const fn hard_pending_compaction_bytes(mut self, value: u64) -> Self {
        self.options.hard_pending_compaction_bytes = value;
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
        validate(&self.options)?;
        Ok(self.options)
    }
}
