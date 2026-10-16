//! The engine's configuration surface.
//!
//! Database-wide and per-keyspace options are validated values: they are built only through a
//! builder whose `build()` checks every field against the bounds in `tessarikv-constants`, so an
//! options value that exists is one the engine accepts. Per-call options ([`ReadOptions`],
//! [`WriteOptions`]) have no invalid combinations and are plain structs.

mod bounds;
mod compaction;
mod db;
mod keyspace;
mod keyspace_bounds;
mod keyspace_builder;
mod keyspaces;
mod read;
mod transaction;
mod write;

#[cfg(test)]
mod compaction_tests;
#[cfg(test)]
mod keyspace_tests;
#[cfg(test)]
mod tests;

pub use compaction::CompactionStyle;
pub use db::{DbOptions, DbOptionsBuilder, StorageMode, WalRecovery};
pub use keyspace::KeyspaceOptions;
pub use keyspace_builder::KeyspaceOptionsBuilder;
pub use read::ReadOptions;
pub use transaction::{Isolation, TransactionOptions};
pub use write::{Durability, WriteOptions};
