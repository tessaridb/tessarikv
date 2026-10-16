//! Tables: immutable sorted files of keys, searched without reading them whole.
//!
//! ```text
//! [data block]*  [filter block]  [index block]  [properties block]  [footer]
//! ```
//!
//! Data blocks hold the entries in key order, each closed once it reaches the target size. The
//! index block has one entry per data block: the block's last key, mapped to where the block
//! is. The filter answers "certainly absent" for keys the table does not hold, so a lookup that
//! misses usually reads no data block at all. The properties block records facts about the table
//! (its entry count), and the footer locates the other three and names the format version.
//! Every block is sealed with a checksum verified on every read (see the handle module docs).
//!
//! A table is written once by [`TableBuilder`] and never changed. [`Table`] opens it from its
//! bytes, or through reads of a file it keeps only the metadata of; data blocks then come from a
//! [`BlockSource`], which may make a walk wait for a block (see the source module). Any number of
//! [`TableIter`]s may walk a table at once.

pub mod block;
pub mod filter;

mod builder;
mod codec;
mod handle;
mod iter;
mod reader;
mod source;

#[cfg(test)]
mod codec_tests;
#[cfg(test)]
mod offset_tests;
#[cfg(test)]
mod source_tests;
#[cfg(test)]
mod tests;

pub use builder::TableBuilder;
pub use codec::Compression;
pub use handle::{BlockHandle, FOOTER_BYTES};
pub use iter::TableIter;
pub use reader::Table;
pub use source::{BlockSource, OpenError, unseal_block};

/// How a table is laid out.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TableOptions {
    /// A data block is closed once its entries take this many bytes.
    pub block_bytes: usize,
    /// Entries between restart points in a data block.
    pub restart_interval: usize,
    /// Bits of filter per key; about one absent key in a hundred passes at ten.
    pub bloom_bits_per_key: u32,
    /// How data blocks are stored.
    pub compression: Compression,
}
