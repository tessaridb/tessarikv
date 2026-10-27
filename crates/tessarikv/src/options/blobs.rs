//! Large values a keyspace keeps apart from its tables, in blob files.

use super::keyspace::KeyspaceOptions;
use super::keyspace_builder::KeyspaceOptionsBuilder;

impl KeyspaceOptions {
    /// Size in bytes from which a value is kept in a blob file instead of in the table that
    /// indexes it; default `None`, every value in its table. At least
    /// [`tessarikv_constants::MIN_BLOB_THRESHOLD`] and at most
    /// [`tessarikv_constants::MAX_VALUE_BYTES`].
    ///
    /// Values move out when tables are written — by a flush or a compaction — never on the
    /// write path: the log and the in-memory tables keep the value. The table then holds a
    /// 24-byte index to the value, so a compaction moves the index and leaves the value where
    /// it is, which is what saves its rewrites. A read of a value in a blob file costs one more
    /// read, and blob files are not kept in the block cache. Expiring puts and deletes stay in
    /// the table. A blob file is removed once no table refers to it; tables written before a
    /// change keep their values where they are until compaction rewrites them, and a read finds
    /// a value wherever it is.
    #[must_use]
    pub const fn blob_threshold(&self) -> Option<u64> {
        self.blob_threshold
    }

    /// Share in percent, 0 to 100, of the keyspace's blob files — the oldest, by number — whose
    /// values a compaction moves into a new blob file as it passes them; default
    /// [`tessarikv_constants::DEFAULT_BLOB_GC_AGE_CUTOFF_PERCENT`]. A blob file whose values
    /// were all moved or deleted is removed, which is how the space of overwritten values comes
    /// back; 0 moves none, and a file then lives as long as one of its values does.
    #[must_use]
    pub const fn blob_gc_age_cutoff_percent(&self) -> u32 {
        self.blob_gc_age_cutoff_percent
    }
}

impl KeyspaceOptionsBuilder {
    /// Sets [`KeyspaceOptions::blob_threshold`]; `build()` refuses a size outside its bounds.
    pub const fn blob_threshold(mut self, bytes: u64) -> Self {
        self.options.blob_threshold = Some(bytes);
        self
    }

    /// Sets [`KeyspaceOptions::blob_gc_age_cutoff_percent`]; `build()` refuses one above 100.
    pub const fn blob_gc_age_cutoff_percent(mut self, percent: u32) -> Self {
        self.options.blob_gc_age_cutoff_percent = percent;
        self
    }
}
