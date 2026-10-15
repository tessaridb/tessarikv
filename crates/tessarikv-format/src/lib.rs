//! On-disk formats of TessariKV: log records and table blocks.
//!
//! Every format here carries a version and a checksum, and every decoder treats its input as
//! untrusted: a malformed input is an error, never a panic or an unbounded allocation.

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

mod batch;
mod error;
mod log;

pub use batch::{BatchEntry, WriteBatch};
pub use error::{Error, Result};
pub use log::{
    EndKind, FRAME_HEADER_BYTES, FrameReader, LogEnd, LogRead, encode_frame, frame_bytes,
};
