//! TessariKV: a pure-Rust, lock-free-first LSM key-value storage engine.

// Production code is held to the workspace lints; a test states its own expectations.
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::as_conversions,
        clippy::arithmetic_side_effects,
        clippy::panic
    )
)]

mod error;
mod options;
mod storage;

pub use error::{Error, ErrorCategory, Result};
pub use options::{
    DbOptions, DbOptionsBuilder, Durability, KeyspaceOptions, KeyspaceOptionsBuilder, ReadOptions,
    StorageMode, WriteOptions,
};
pub use storage::{MemoryStorage, OsStorage, ReadableFile, Storage, StorageLock, WritableFile};
