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
mod manifest;
mod table;
mod varint;

pub use batch::{BatchEntry, WriteBatch};
pub use error::{Error, Result};
pub use log::{
    EndKind, FRAME_HEADER_BYTES, FrameReader, LogEnd, LogRead, concat_checksum, encode_frame,
    frame_bytes, frame_header, payload_checksum,
};
pub use manifest::{ManifestEdit, TableMeta, TableRef};
pub use table::block::{Block, BlockBuilder, BlockIter, Compare};
pub use table::filter::{BloomBuilder, BloomFilter};
pub use table::{Compression, FOOTER_BYTES, Table, TableBuilder, TableIter, TableOptions};
