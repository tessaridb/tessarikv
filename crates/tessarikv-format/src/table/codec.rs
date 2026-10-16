//! Block codecs: how a data block's bytes are shrunk on the way to disk and restored on the way
//! back.
//!
//! A compressed block stores the length of its payload as a varint, then the codec's own bytes;
//! the block's seal records which codec wrote it, so a table may hold blocks of several codecs
//! and is read whatever the options of the reader. A block is stored compressed only when that
//! saves at least an eighth of its payload; otherwise it is stored as it is, and reading it costs
//! nothing extra.
//!
//! Reading treats the stored bytes as untrusted even after the seal's checksum has passed: a
//! claimed length larger than the body could expand to is refused before anything is reserved,
//! the codecs whose expansion is unbounded in practice decode into a buffer that grows only as
//! output appears, and a body that decodes to any length but the claimed one is refused.

use std::io::Read;

use ruzstd::decoding::StreamingDecoder;
use ruzstd::encoding::CompressionLevel;

use crate::varint;

/// How a table stores its data blocks.
///
/// Every codec here is decoded by any reader: the choice affects only how new blocks are
/// written.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[non_exhaustive]
pub enum Compression {
    /// As they are.
    #[default]
    None,
    /// LZ4: the fastest to write and read, a moderate ratio.
    Lz4,
    /// Snappy: close to LZ4 in speed and ratio.
    Snappy,
    /// Zstandard at its fastest level: a better ratio than LZ4 for more time spent writing,
    /// still fast to read.
    Zstd,
    /// Deflate (the zlib algorithm, without its wrapper) at `level`, 0 to 10: the slowest to
    /// write at high levels, a good ratio. Levels above 10 compress as 10; level 0 stores
    /// blocks as they are.
    Deflate {
        /// From 0 (store) to 10 (smallest).
        level: u8,
    },
}

/// The codec byte of a block stored as it is.
const NONE: u8 = 0;
const LZ4: u8 = 1;
const SNAPPY: u8 = 2;
const ZSTD: u8 = 3;
const DEFLATE: u8 = 4;

/// The most bytes LZ4 can expand one input byte into.
const LZ4_MAX_RATIO: usize = 255;

/// The most bytes Snappy can expand one input byte into: a three-byte copy of 64 bytes.
const SNAPPY_MAX_RATIO: usize = 22;

/// Zstandard output reserved per body byte before decoding; the buffer grows past it only as
/// output appears.
const ZSTD_RESERVE_RATIO: usize = 8;

/// `payload` compressed by `compression` with its length in front, and the codec byte to seal it
/// with; `None` when the codec stores blocks as they are or saves less than an eighth.
pub(super) fn compress(compression: Compression, payload: &[u8]) -> Option<(Vec<u8>, u8)> {
    let length = u32::try_from(payload.len()).ok()?;
    if payload.is_empty() {
        return None;
    }
    let mut sealed = Vec::new();
    varint::put_u32(&mut sealed, length);
    let byte = match compression {
        Compression::None => return None,
        Compression::Lz4 => {
            sealed.extend_from_slice(&lz4_flex::block::compress(payload));
            LZ4
        }
        Compression::Snappy => {
            let body = snap::raw::Encoder::new().compress_vec(payload).ok()?;
            sealed.extend_from_slice(&body);
            SNAPPY
        }
        Compression::Zstd => {
            ruzstd::encoding::compress(payload, &mut sealed, CompressionLevel::Fastest);
            ZSTD
        }
        Compression::Deflate { level } => {
            sealed.extend_from_slice(&miniz_oxide::deflate::compress_to_vec(payload, level));
            DEFLATE
        }
    };
    let saved = payload.len().saturating_sub(payload.len() / 8);
    (sealed.len() < saved).then_some((sealed, byte))
}

/// The payload of a block stored by codec `byte`, or why it cannot be restored.
pub(super) fn decompress(byte: u8, stored: &[u8]) -> Result<Vec<u8>, &'static str> {
    if byte == NONE {
        return Ok(stored.to_vec());
    }
    if !matches!(byte, LZ4 | SNAPPY | ZSTD | DEFLATE) {
        return Err("unknown compression");
    }
    let (length, used) = varint::get_u32(stored).ok_or("compressed length")?;
    let body = stored.get(used..).ok_or("compressed length")?;
    let length = usize::try_from(length).map_err(|_| "compressed length")?;
    let payload = match byte {
        LZ4 => {
            bounded(length, body, LZ4_MAX_RATIO)?;
            lz4_flex::block::decompress(body, length).map_err(|_| "does not decompress")?
        }
        SNAPPY => {
            bounded(length, body, SNAPPY_MAX_RATIO)?;
            if snap::raw::decompress_len(body) != Ok(length) {
                return Err("decompresses to another length");
            }
            snap::raw::Decoder::new()
                .decompress_vec(body)
                .map_err(|_| "does not decompress")?
        }
        ZSTD => {
            let reserve = length.min(body.len().saturating_mul(ZSTD_RESERVE_RATIO));
            let mut payload = Vec::with_capacity(reserve);
            let decoder = StreamingDecoder::new(body).map_err(|_| "does not decompress")?;
            let limit = u64::try_from(length).map_or(u64::MAX, |limit| limit.saturating_add(1));
            decoder
                .take(limit)
                .read_to_end(&mut payload)
                .map_err(|_| "does not decompress")?;
            payload
        }
        _ => miniz_oxide::inflate::decompress_to_vec_with_limit(body, length)
            .map_err(|_| "does not decompress")?,
    };
    if payload.len() == length {
        Ok(payload)
    } else {
        Err("decompresses to another length")
    }
}

/// Refuses a claimed `length` that `body` could not expand to at `ratio` output bytes per byte.
fn bounded(length: usize, body: &[u8], ratio: usize) -> Result<(), &'static str> {
    if length > body.len().saturating_mul(ratio).saturating_add(16) {
        Err("claims more than its bytes can expand to")
    } else {
        Ok(())
    }
}
