//! What the installer reports beside its signals: what compaction owes each keyspace, and the
//! events it tells a listener about.

use std::sync::Arc;

use super::FlushSignals;
use crate::options::Reporter;

impl FlushSignals {
    /// Reports pressure changes and the installer's failure to `events`, when there are any.
    pub(in crate::db) fn with_events(mut self, events: Option<Arc<Reporter>>) -> Self {
        self.events = events;
        self
    }

    /// Publishes what compaction owes each keyspace, by keyspace id.
    pub(in crate::db) fn set_owed(&self, owed: Vec<(u32, u64)>) {
        self.owed.store(Arc::new(owed));
    }

    /// What compaction owed `keyspace` at the installer's last step.
    pub(in crate::db) fn owed(&self, keyspace: u32) -> u64 {
        self.owed
            .load()
            .iter()
            .find(|(id, _)| *id == keyspace)
            .map_or(0, |(_, owed)| *owed)
    }
}
