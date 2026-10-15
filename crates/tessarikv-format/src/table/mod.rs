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
//! A table is written once by [`TableBuilder`] and never changed; [`Table`] reads it from its
//! bytes, and any number of [`TableIter`]s may walk it at once.

pub mod block;
pub mod filter;

mod builder;
mod handle;
mod reader;

#[cfg(test)]
mod tests;

pub use builder::TableBuilder;
pub use handle::FOOTER_BYTES;
pub use reader::{Table, TableIter};

/// How a block's bytes are stored.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[non_exhaustive]
pub enum Compression {
    /// As they are.
    #[default]
    None,
    /// Compressed with LZ4, unless that saves less than an eighth.
    Lz4,
}

impl Compression {
    fn byte(self) -> u8 {
        match self {
            Self::None => 0,
            Self::Lz4 => 1,
        }
    }

    fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            0 => Some(Self::None),
            1 => Some(Self::Lz4),
            _ => None,
        }
    }
}

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
