//! Opening a database in memory: nothing touches storage and no write outlives the process.

use std::sync::Arc;

use arc_swap::ArcSwap;
use tessarikv_constants::COMMIT_RING_SLOTS;
use tessarikv_sync::{Sequencer, SnapshotRegistry};

use super::view::View;
use super::{Db, commits, keyspaces};
use crate::DbOptions;
use crate::statistics::Statistics;

impl Db {
    /// A database with no log, no flushes and no lock, holding every batch in memory.
    pub(super) fn open_in_memory(options: &DbOptions) -> Self {
        Self {
            sequencer: Arc::new(Sequencer::new(0, COMMIT_RING_SLOTS)),
            visibility: Arc::new(tokio::sync::Notify::new()),
            commits: commits::Commits::new(options.serializable_transactions()),
            view: Arc::new(ArcSwap::from_pointee({
                // In memory the named keyspaces take ids in the order given.
                let named = options.keyspaces().iter().zip(1_u32..);
                let (tables, names) = keyspaces::tables_for(
                    options,
                    named.map(|((name, _), id)| (id, name.as_str())),
                );
                View::recovered(tables, names, Vec::new())
            })),
            snapshots: Arc::new(SnapshotRegistry::new()),
            #[cfg(test)]
            switching: std::sync::atomic::AtomicBool::new(false),
            #[cfg(test)]
            limiter: None,
            log: None,
            installer: None,
            statistics: options.statistics().then(|| Arc::new(Statistics::new())),
            access: super::open::Access::Memory,
        }
    }
}
