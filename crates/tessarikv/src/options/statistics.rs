//! Whether a database keeps statistics.

use super::db::{DbOptions, DbOptionsBuilder};

impl DbOptions {
    /// Whether the database keeps statistics, read with [`crate::Db::statistics`]; default
    /// `false`. Each counted event then costs one uncontended atomic add, and a timed operation
    /// two clock reads.
    #[must_use]
    pub const fn statistics(&self) -> bool {
        self.statistics
    }
}

impl DbOptionsBuilder {
    /// Sets [`DbOptions::statistics`].
    pub const fn statistics(mut self, value: bool) -> Self {
        self.options.statistics = value;
        self
    }
}
