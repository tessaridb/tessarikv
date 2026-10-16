//! Where blocks are in a table file, how each is sealed, and the footer that finds them.
//!
//! Every block is stored as `[payload][compression: u8][crc32c: u32]`, the checksum covering the
//! payload and the compression byte; it is verified on every read. A compressed payload is
//! `[uncompressed length: varint][LZ4 block]`, and its length is checked against what LZ4 can
//! produce from that many bytes before anything is allocated.
//!
//! The footer is the last [`FOOTER_BYTES`] of the file:
//!
//! ```text
//! [filter handle: 16][index handle: 16][properties handle: 16][format version: u32]
//! [crc32c of the 52 bytes before it: u32][magic: u64]
//! ```
//!
//! A handle is `[offset: u64][size: u64]`, the size including the block's 5-byte seal.

use super::Compression;
use crate::error::{Error, Result};
use crate::varint;

/// Bytes of the footer.
pub const FOOTER_BYTES: usize = 64;

/// The table format this build writes and reads.
pub(super) const FORMAT_VERSION: u32 = 2;

/// The first version, without range deletes; still read.
const FORMAT_VERSION_1: u32 = 1;

/// Identifies a table file.
const MAGIC: u64 = 0x5445_5353_4B56_5442;

/// Bytes after a block's payload: the compression byte and the checksum.
const SEAL_BYTES: usize = 5;

/// Bytes of one handle.
const HANDLE_BYTES: usize = 16;

/// Where the footer's checksum starts.
const FOOTER_CHECKSUM_AT: usize = 52;

/// The most bytes LZ4 can expand one input byte into.
const LZ4_MAX_RATIO: usize = 255;

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
}

impl Footer {
    pub(super) fn encode(self) -> [u8; FOOTER_BYTES] {
        let mut bytes = [0; FOOTER_BYTES];
        bytes[0..16].copy_from_slice(&self.filter.encode());
        bytes[16..32].copy_from_slice(&self.index.encode());
        bytes[32..48].copy_from_slice(&self.properties.encode());
        bytes[48..52].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
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
        if checksum != u32_at(FOOTER_CHECKSUM_AT) {
            return Err(Error::Malformed {
                what: "table footer",
                reason: "checksum mismatch",
            });
        }
        let version = u32_at(48);
        if version != FORMAT_VERSION && version != FORMAT_VERSION_1 {
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
        Ok(Self {
            filter: BlockHandle::decode(bytes.get(0..16).unwrap_or_default())?,
            index: BlockHandle::decode(bytes.get(16..32).unwrap_or_default())?,
            properties: BlockHandle::decode(bytes.get(32..48).unwrap_or_default())?,
        })
    }
}

/// Appends `payload` to `out` as a sealed block, compressed when that saves at least an
/// eighth; returns its handle.
pub(super) fn write_block(
    out: &mut Vec<u8>,
    base: u64,
    payload: &[u8],
    compression: Compression,
) -> BlockHandle {
    let offset = base.saturating_add(u64::try_from(out.len()).unwrap_or(u64::MAX));
    let compressed = match compression {
        Compression::Lz4 => {
            let mut sealed = Vec::new();
            varint::put_u32(
                &mut sealed,
                u32::try_from(payload.len()).unwrap_or(u32::MAX),
            );
            sealed.extend_from_slice(&lz4_flex::block::compress(payload));
            let saved = payload.len().saturating_sub(payload.len() / 8);
            (sealed.len() < saved && u32::try_from(payload.len()).is_ok()).then_some(sealed)
        }
        Compression::None => None,
    };
    let (stored, kind) = match &compressed {
        Some(sealed) => (sealed.as_slice(), Compression::Lz4.byte()),
        None => (payload, Compression::None.byte()),
    };
    out.extend_from_slice(stored);
    out.push(kind);
    let checksum = crc32c::crc32c_append(crc32c::crc32c(stored), &[kind]);
    out.extend_from_slice(&checksum.to_le_bytes());
    let end = base.saturating_add(u64::try_from(out.len()).unwrap_or(u64::MAX));
    BlockHandle {
        offset,
        size: end.saturating_sub(offset),
    }
}

/// The verified, uncompressed payload of the block at `handle` in `file`.
pub(super) fn read_block(file: &[u8], handle: BlockHandle) -> Result<Vec<u8>> {
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
    unseal(sealed)
}

/// The verified, uncompressed payload of a sealed block: its stored bytes and its seal.
pub(super) fn unseal(sealed: &[u8]) -> Result<Vec<u8>> {
    let corrupt = |reason| Error::Malformed {
        what: "table block",
        reason,
    };
    let stored_len = sealed
        .len()
        .checked_sub(SEAL_BYTES)
        .ok_or(corrupt("shorter than its seal"))?;
    let (stored, seal) = sealed.split_at(stored_len);
    let kind = *seal.first().ok_or(corrupt("seal"))?;
    let expected = seal
        .get(1..)
        .and_then(|field| field.try_into().ok())
        .map(u32::from_le_bytes)
        .ok_or(corrupt("seal"))?;
    if crc32c::crc32c_append(crc32c::crc32c(stored), &[kind]) != expected {
        return Err(corrupt("checksum mismatch"));
    }
    match Compression::from_byte(kind).ok_or(corrupt("unknown compression"))? {
        Compression::None => Ok(stored.to_vec()),
        Compression::Lz4 => {
            let (length, used) = varint::get_u32(stored).ok_or(corrupt("compressed length"))?;
            let body = stored.get(used..).ok_or(corrupt("compressed length"))?;
            let length = usize::try_from(length).map_err(|_| corrupt("compressed length"))?;
            if length > body.len().saturating_mul(LZ4_MAX_RATIO).saturating_add(16) {
                return Err(corrupt("claims more than its bytes can expand to"));
            }
            lz4_flex::block::decompress(body, length).map_err(|_| corrupt("does not decompress"))
        }
    }
}
