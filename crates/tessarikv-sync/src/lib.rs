//! Lock-free and wait-free building blocks of the TessariKV storage engine.
//!
//! Every structure here documents, on its type, its sequential specification, the
//! linearization point and progress guarantee of each operation, its memory-ordering
//! argument and how memory is reclaimed. Each is model-checked with `loom`
//! (`RUSTFLAGS="--cfg loom" cargo test -p tessarikv-sync --release`).

// Production code is held to the workspace lints; a test states its own expectations.
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::as_conversions,
        clippy::arithmetic_side_effects,
        clippy::panic,
        clippy::cast_possible_truncation
    )
)]

mod append_buffer;
mod arena;
mod clock_cache;
mod error;
mod sequencer;
mod shim;
mod skiplist;
mod slot_directory;
mod snapshots;

pub use append_buffer::{AppendReader, AppendWriter, append_buffer};
pub use clock_cache::{Admission, ClockCache};
pub use error::{ArenaError, BufferError};
pub use sequencer::{Claim, Sequencer, Switch, next_epoch};
pub use skiplist::{BytewiseComparator, Cursor, CursorPosition, Insert, KeyComparator, SkipList};
pub use snapshots::{Horizon, Registration, SnapshotRegistry};
