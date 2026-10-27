//! Where blocks are in a table file, how each is sealed, and the footer that finds them.
//!
//! Every block is stored as `[payload][compression: u8][crc32c: u32]`, the checksum covering the
//! payload and the compression byte; it is verified on every read. A compressed payload is
//! `[uncompressed length: varint][codec body]` (see the codec module for how each is decoded).
//!
//! The footer is the last [`FOOTER_BYTES`] of the file:
//!
//! ```text
//! [filter handle: 16][index handle: 16][properties handle: 16][format version: u32]
//! [crc32c of the 52 bytes before it: u32][magic: u64]
//! ```
//!
//! A handle is `[offset: u64][size: u64]`, the size including the block's 5-byte seal.
//!
//! A sealed file (see the crate's `FileSealer`) starts with its seal header, and each block,
//! seal included, and the footer are encrypted as the unit at their offset: a handle's size then
//! counts the authentication tag, and the footer is the last [`FOOTER_BYTES`] plus a tag.

use super::filter::Probing;
use super::{Compression, codec};
use crate::error::{Error, Result};
use crate::seal::FileSealer;

/// Bytes of the footer.
pub const FOOTER_BYTES: usize = 64;

/// The table format this build writes and reads.
pub(super) const FORMAT_VERSION: u32 = 3;

/// The second version, whose filters step through a line by double hashing; still read.
const FORMAT_VERSION_2: u32 = 2;

/// The first version, also without range deletes; still read.
const FORMAT_VERSION_1: u32 = 1;

/// Identifies a table file.
const MAGIC: u64 = 0x5445_5353_4B56_5442;

/// Bytes after a block's payload: the compression byte and the checksum.
pub(super) const SEAL_BYTES: usize = 5;

/// Bytes of one handle.
const HANDLE_BYTES: usize = 16;

/// Where the footer's checksum starts.
const FOOTER_CHECKSUM_AT: usize = 52;

/// Where a block is in a table file: its offset and its size, seal included. A reader that
/// holds the file reads exactly these bytes and passes them to [`super::unseal_block`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct BlockHandle {
    pub(super) offset: u64,
    pub(super) size: u64,
}

impl BlockHandle {
    /// Where the block starts in the file.
    pub fn offset(self) -> u64 {
        self.offset
    }

    /// Bytes of the block in the file, its seal included.
    pub fn size(self) -> u64 {
        self.size
    }

    /// Where the block ends in the file, checked against a file of `len` bytes.
    pub(super) fn end_within(self, len: u64) -> Result<u64> {
        self.offset
            .checked_add(self.size)
            .filter(|end| *end <= len)
            .ok_or(Error::Malformed {
                what: "table block",
                reason: "handle points past the file",
            })
    }

    pub(super) fn encode(self) -> [u8; HANDLE_BYTES] {
        let mut bytes = [0; HANDLE_BYTES];
        bytes[..8].copy_from_slice(&self.offset.to_le_bytes());
        bytes[8..].copy_from_slice(&self.size.to_le_bytes());
        bytes
    }

    pub(super) fn decode(bytes: &[u8]) -> Result<Self> {
        let field = |range: std::ops::Range<usize>| {
            bytes
                .get(range)
                .and_then(|field| field.try_into().ok())
                .map(u64::from_le_bytes)
                .ok_or(Error::Malformed {
                    what: "block handle",
                    reason: "shorter than 16 bytes",
                })
        };
        Ok(Self {
            offset: field(0..8)?,
            size: field(8..16)?,
        })
    }
}

/// The handles the footer records.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Footer {
    pub(super) filter: BlockHandle,
    pub(super) index: BlockHandle,
    pub(super) properties: BlockHandle,
    /// How the filter picks a key's bits, which the format version decides.
    pub(super) probing: Probing,
}

impl Footer {
    /// The footer's bytes, under the version its filter's probing belongs to.
    pub(super) fn encode(self) -> [u8; FOOTER_BYTES] {
        let version = match self.probing {
            Probing::Remixed => FORMAT_VERSION,
            Probing::Stepped => FORMAT_VERSION_2,
        };
        let mut bytes = [0; FOOTER_BYTES];
        bytes[0..16].copy_from_slice(&self.filter.encode());
        bytes[16..32].copy_from_slice(&self.index.encode());
        bytes[32..48].copy_from_slice(&self.properties.encode());
        bytes[48..52].copy_from_slice(&version.to_le_bytes());
        let checksum = crc32c::crc32c(&bytes[..FOOTER_CHECKSUM_AT]);
        bytes[52..56].copy_from_slice(&checksum.to_le_bytes());
        bytes[56..64].copy_from_slice(&MAGIC.to_le_bytes());
        bytes
    }

    /// Reads the footer at the end of `file`.
    pub(super) fn decode(file: &[u8]) -> Result<Self> {
        let Some(start) = file.len().checked_sub(FOOTER_BYTES) else {
            return Err(Error::Truncated {
                what: "table",
                needed: 64,
                available: u64::try_from(file.len()).unwrap_or(u64::MAX),
            });
        };
        let bytes = file.get(start..).unwrap_or_default();
        let u32_at = |at: usize| {
            bytes
                .get(at..at.saturating_add(4))
                .and_then(|field| field.try_into().ok())
                .map(u32::from_le_bytes)
                .unwrap_or_default()
        };
        let magic = bytes
            .get(56..64)
            .and_then(|field| field.try_into().ok())
            .map(u64::from_le_bytes);
        if magic != Some(MAGIC) {
            return Err(Error::Malformed {
                what: "table",
                reason: "no table footer at the end of the file",
            });
        }
        let checksum = crc32c::crc32c(bytes.get(..FOOTER_CHECKSUM_AT).unwrap_or_default());
        // A fuzzing build skips the comparison so mutated input reaches the decoders behind it.
        if !cfg!(fuzzing) && checksum != u32_at(FOOTER_CHECKSUM_AT) {
            return Err(Error::Malformed {
                what: "table footer",
                reason: "checksum mismatch",
            });
        }
        let version = u32_at(48);
        let probing = match version {
            FORMAT_VERSION => Probing::Remixed,
            FORMAT_VERSION_2 | FORMAT_VERSION_1 => Probing::Stepped,
            _ => {
                return Err(match u8::try_from(version) {
                    Ok(version) => Error::UnsupportedVersion {
                        what: "table",
                        version,
                    },
                    Err(_) => Error::Malformed {
                        what: "table",
                        reason: "format version out of range",
                    },
                });
            }
        };
        Ok(Self {
            filter: BlockHandle::decode(bytes.get(0..16).unwrap_or_default())?,
            index: BlockHandle::decode(bytes.get(16..32).unwrap_or_default())?,
            properties: BlockHandle::decode(bytes.get(32..48).unwrap_or_default())?,
            probing,
        })
    }
}

/// Appends `payload` to `out` as a sealed block, compressed when that saves at least an
/// eighth, and encrypted at its offset when the file has a `sealer`; returns its handle.
pub(super) fn write_block(
    out: &mut Vec<u8>,
    base: u64,
    payload: &[u8],
    compression: Compression,
    sealer: Option<&FileSealer>,
) -> Result<BlockHandle> {
    let start = out.len();
    let offset = base.saturating_add(u64::try_from(start).unwrap_or(u64::MAX));
    let compressed = codec::compress(compression, payload);
    let (stored, kind) = match &compressed {
        Some((sealed, byte)) => (sealed.as_slice(), *byte),
        None => (payload, 0),
    };
    out.extend_from_slice(stored);
    out.push(kind);
    let checksum = crc32c::crc32c_append(crc32c::crc32c(stored), &[kind]);
    out.extend_from_slice(&checksum.to_le_bytes());
    if let Some(sealer) = sealer {
        let plain = out.split_off(start);
        out.extend_from_slice(&sealer.seal(offset, &plain)?);
    }
    let end = base.saturating_add(u64::try_from(out.len()).unwrap_or(u64::MAX));
    Ok(BlockHandle {
        offset,
        size: end.saturating_sub(offset),
    })
}

/// The verified, uncompressed payload of the block at `handle` in `file`.
pub(super) fn read_block(
    file: &[u8],
    handle: BlockHandle,
    sealer: Option<&FileSealer>,
) -> Result<Vec<u8>> {
    let len = u64::try_from(file.len()).unwrap_or(u64::MAX);
    let end = handle.end_within(len)?;
    let sealed = usize::try_from(handle.offset)
        .ok()
        .zip(usize::try_from(end).ok())
        .and_then(|(start, end)| file.get(start..end))
        .ok_or(Error::Malformed {
            what: "table block",
            reason: "handle points past the file",
        })?;
    super::unseal::unseal(sealed, handle.offset, sealer)
}
