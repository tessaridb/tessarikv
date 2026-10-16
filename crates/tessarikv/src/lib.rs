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

mod db;
mod error;
mod log;
mod memtable;
mod options;
mod sealing;
mod statistics;
mod storage;
mod tombstones;
mod transaction;
mod version;

pub use db::{Db, DbIterator, ScanPage, ScanRange, Snapshot};
pub use error::{Error, ErrorCategory, Result};
pub use options::{
    CompactionStyle, DbOptions, DbOptionsBuilder, Durability, Encryption, Isolation, KeyProvider,
    KeyspaceOptions, KeyspaceOptionsBuilder, ReadOptions, StorageMode, TransactionOptions,
    WalRecovery, WriteOptions,
};
pub use statistics::{Histogram, HistogramSnapshot, StatisticsSnapshot, Ticker};
pub use storage::{MemoryStorage, OsStorage, ReadableFile, Storage, StorageLock, WritableFile};
pub use tessarikv_format::{BatchEntry, Cipher, Compression, ROOT_KEY_BYTES, RootKey, WriteBatch};
pub use tessarikv_types::{KeyspaceId, SequenceNumber};
pub use transaction::{Savepoint, Transaction};
