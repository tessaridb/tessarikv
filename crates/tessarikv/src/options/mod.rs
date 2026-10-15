//! The engine's configuration surface.
//!
//! Database-wide and per-keyspace options are validated values: they are built only through a
//! builder whose `build()` checks every field against the bounds in `tessarikv-constants`, so an
//! options value that exists is one the engine accepts. Per-call options ([`ReadOptions`],
//! [`WriteOptions`]) have no invalid combinations and are plain structs.

mod bounds;
mod db;
mod keyspace;
mod read;
mod write;

#[cfg(test)]
mod tests;

pub use db::{DbOptions, DbOptionsBuilder, StorageMode};
pub use keyspace::{KeyspaceOptions, KeyspaceOptionsBuilder};
pub use read::ReadOptions;
pub use write::{Durability, WriteOptions};
