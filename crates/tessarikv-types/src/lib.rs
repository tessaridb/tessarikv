//! The value types every TessariKV crate shares.
//!
//! Each kind of number the engine counts (a sequence number, a keyspace, a file) is its own
//! type, so one cannot be passed where another is meant. The internal key, the unit every sorted
//! structure in the engine orders by, is defined here once.

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

mod error;
mod ids;
mod internal_key;
mod sequence;
mod value_kind;

pub use error::{Error, Result};
pub use ids::{FileNumber, KeyspaceId};
pub use internal_key::{InternalKeyRef, Trailer, compare_internal, encode_internal, seek_trailer};
pub use sequence::SequenceNumber;
pub use value_kind::ValueKind;
