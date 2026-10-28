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

mod blob;
mod builder;
mod codec;
mod handle;
mod iter;
mod prefix;
mod reader;
mod source;
mod unseal;

#[cfg(test)]
mod blob_tests;
#[cfg(test)]
mod codec_tests;
#[cfg(test)]
mod hostile_tests;
#[cfg(test)]
mod offset_tests;
#[cfg(test)]
mod prefix_tests;
#[cfg(test)]
mod sealed_tests;
#[cfg(test)]
mod source_tests;
#[cfg(test)]
mod tests;

pub use blob::{BlobBuilder, BlobIndex, open_blob};
pub use builder::TableBuilder;
pub use codec::Compression;
pub use filter::FilterPolicy;
pub use handle::{BlockHandle, FOOTER_BYTES};
pub use iter::TableIter;
pub use reader::Table;
pub use source::{BlockSource, OpenError, unseal_block, unseal_block_owned};

/// How a table is laid out.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TableOptions {
    /// A data block is closed once its entries take this many bytes. Readers refuse a block
    /// holding more than `MAX_BLOCK_BYTES` plus one largest entry, so keep it at or below
    /// `tessarikv_constants::MAX_BLOCK_BYTES`.
    pub block_bytes: usize,
    /// Entries between restart points in a data block.
    pub restart_interval: usize,
    /// Bits of filter per key; about one absent key in a hundred passes a bloom filter at ten.
    pub bloom_bits_per_key: u32,
    /// The kind of whole-key filter the table keeps.
    pub filter: FilterPolicy,
    /// How data blocks are stored.
    pub compression: Compression,
    /// Length of the key prefixes a second filter records, at most
    /// `tessarikv_constants::MAX_PREFIX_LENGTH`; `None` writes no prefix filter. The filter lets
    /// a read whose bounds lie inside one prefix pass over a table that holds no key with it
    /// (see [`Table::may_hold_prefix_range`]).
    pub prefix_length: Option<u32>,
}
