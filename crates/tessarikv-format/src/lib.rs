//! On-disk formats of TessariKV: log records and table blocks.
//!
//! Filled by the log and table subgoals; every format here carries a version and a checksum.

mod error;

pub use error::{Error, Result};
