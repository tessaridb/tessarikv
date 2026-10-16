//! How large the manifest grows before it is rewritten.

use tessarikv_constants as limits;

use super::bounds::check_range;
use super::db::{DbOptions, DbOptionsBuilder};
use crate::Result;

impl DbOptions {
    /// Bytes past which the manifest, the log of changes to the set of table files, is replaced
    /// by a new one holding the current set as one change (default 64 MiB). The manifest is read
    /// whole on every open, so this bounds how long an open spends reading it.
    #[must_use]
    pub const fn max_manifest_bytes(&self) -> u64 {
        self.max_manifest_bytes
    }
}

impl DbOptionsBuilder {
    /// Sets [`DbOptions::max_manifest_bytes`].
    pub const fn max_manifest_bytes(mut self, value: u64) -> Self {
        self.options.max_manifest_bytes = value;
        self
    }
}

/// Refuses a manifest limit outside its bounds.
pub(super) fn check(options: &DbOptions) -> Result<()> {
    check_range(
        "db.max_manifest_bytes",
        options.max_manifest_bytes,
        limits::MIN_MAX_MANIFEST_BYTES,
        limits::MAX_MAX_MANIFEST_BYTES,
    )
}
