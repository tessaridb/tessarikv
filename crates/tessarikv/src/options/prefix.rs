//! The key prefix a keyspace's tables filter by.

use super::keyspace::KeyspaceOptions;
use super::keyspace_builder::KeyspaceOptionsBuilder;

impl KeyspaceOptions {
    /// Length in bytes of the key prefixes the keyspace's tables keep a second filter for, at
    /// most [`tessarikv_constants::MAX_PREFIX_LENGTH`]; default `None`, no prefix filter.
    ///
    /// With a length set, a scan or an iterator whose bounds lie inside one prefix of that
    /// length — as [`crate::ReadOptions::with_prefix`] sets them for a prefix at least that
    /// long — passes over every table file that holds no key with that prefix, without reading
    /// its blocks. Reads whose bounds span more than one prefix, or none, read as without it,
    /// so no read returns fewer keys. Keys shorter than the length have no prefix and are
    /// recorded in no prefix filter. Each table records the length it was written with and is
    /// consulted only for that length: after a change, tables written before it are read as
    /// though they had no prefix filter until compaction rewrites them. Point reads keep using
    /// the whole-key filter. The filter costs about `bloom_bits_per_key` bits per distinct
    /// prefix in each table.
    #[must_use]
    pub const fn prefix_length(&self) -> Option<u32> {
        self.prefix_length
    }
}

impl KeyspaceOptionsBuilder {
    /// Sets [`KeyspaceOptions::prefix_length`]; `build()` refuses a length of zero or above
    /// [`tessarikv_constants::MAX_PREFIX_LENGTH`].
    pub const fn prefix_length(mut self, length: u32) -> Self {
        self.options.prefix_length = Some(length);
        self
    }
}
