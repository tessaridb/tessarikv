//! The engine's configuration surface.
//!
//! Database-wide and per-keyspace options are validated values: they are built only through a
//! builder whose `build()` checks every field against the bounds in `tessarikv-constants`, so an
//! options value that exists is one the engine accepts. Per-call options ([`ReadOptions`],
//! [`WriteOptions`]) have no invalid combinations and are plain structs.

mod block_reads;
mod bounds;
mod compaction;
mod compaction_filter;
mod db;
mod encryption;
mod events;
mod keyspace;
mod keyspace_bounds;
mod keyspace_builder;
mod keyspaces;
mod manifest;
mod memory;
mod modes;
mod read;
mod statistics;
mod transaction;
mod write;

#[cfg(test)]
mod compaction_tests;
#[cfg(test)]
mod events_tests;
#[cfg(test)]
mod keyspace_tests;
#[cfg(test)]
mod tests;

pub use compaction::CompactionStyle;
pub use compaction_filter::{CompactionFilter, FilterDecision};
pub use db::{DbOptions, DbOptionsBuilder};
pub use encryption::{Encryption, KeyProvider};
pub(crate) use events::Reporter;
pub use events::{CompactionCompleted, EventListener, FlushCompleted, StallCondition};
pub use keyspace::KeyspaceOptions;
pub use keyspace_builder::KeyspaceOptionsBuilder;
pub(crate) use keyspaces::DEFAULT_NAME;
pub use modes::{StorageMode, WalRecovery};
pub use read::ReadOptions;
pub use transaction::{Isolation, TransactionOptions};
pub use write::{Durability, WriteOptions};
