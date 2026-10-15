//! On-disk formats of TessariKV: log records and table blocks.
//!
//! Every format here carries a version and a checksum, and every decoder treats its input as
//! untrusted: a malformed input is an error, never a panic or an unbounded allocation.

mod error;

pub use error::{Error, Result};
