//! Options for one read.

/// How one read or iterator behaves.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct ReadOptions {
    /// Verify the checksum of every block read from a table (default `true`).
    ///
    /// Turning it off trades corruption detection for a little CPU; a corrupt block then decodes
    /// to an error or to wrong data instead of a [`crate::ErrorCategory::Corruption`] error.
    pub verify_checksums: bool,
    /// Keep the blocks this read loads in the block cache (default `true`).
    ///
    /// A one-off scan sets it to `false` so it does not evict the working set.
    pub fill_cache: bool,
}

impl Default for ReadOptions {
    fn default() -> Self {
        Self {
            verify_checksums: true,
            fill_cache: true,
        }
    }
}
