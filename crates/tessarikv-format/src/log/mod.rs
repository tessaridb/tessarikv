//! Log frames: the unit the write-ahead log is made of.
//!
//! A frame is a 16-byte header followed by its payload:
//!
//! ```text
//! [checksum: u32][payload_len: u32][log_number: u64][payload: payload_len bytes]
//! ```
//!
//! All integers are little-endian. The checksum is CRC-32C over `payload_len`, `log_number` and
//! the payload. Frames are packed back to back with no alignment, so a frame's size depends only
//! on its payload: a writer knows how many bytes to reserve before it knows where they land.
//!
//! A log file is read as the longest prefix of valid frames. What follows that prefix tells how
//! the log ended (see [`EndKind`]): zero bytes or the end of the file are a clean end, a frame
//! that fails its checksum or runs past the input is a torn tail, and a valid frame carrying
//! another log number is a leftover from a reused file. Nothing after the first invalid frame
//! is ever read, so a damaged frame can never be mistaken for the start of the next one.

mod reader;

#[cfg(test)]
mod tests;

use tessarikv_constants::MAX_LOG_PAYLOAD_BYTES;

use crate::error::{Error, Result};

pub use reader::{EndKind, FrameReader, LogEnd, LogRead};

/// Bytes of a frame header.
pub const FRAME_HEADER_BYTES: usize = 16;

// The length field is a u32, so the largest payload must fit in one.
const _: () = assert!(matches!(
    MAX_LOG_PAYLOAD_BYTES.checked_shr(32),
    None | Some(0)
));

/// Bytes of a frame carrying `payload_len` bytes of payload.
///
/// # Errors
///
/// [`Error::TooLarge`] for a payload longer than the largest a frame carries.
pub fn frame_bytes(payload_len: usize) -> Result<usize> {
    if payload_len > MAX_LOG_PAYLOAD_BYTES {
        return Err(too_large(payload_len));
    }
    payload_len
        .checked_add(FRAME_HEADER_BYTES)
        .ok_or_else(|| too_large(payload_len))
}

/// Writes one frame of `payload` for log `log_number` into `destination`, which must be exactly
/// [`frame_bytes`]`(payload.len())` long.
///
/// # Errors
///
/// [`Error::TooLarge`] for an oversized payload; [`Error::Malformed`] for a destination of
/// another length.
pub fn encode_frame(log_number: u64, payload: &[u8], destination: &mut [u8]) -> Result<()> {
    if destination.len() != frame_bytes(payload.len())? {
        return Err(Error::Malformed {
            what: "log frame",
            reason: "destination is not exactly one frame long",
        });
    }
    let payload_len = u32::try_from(payload.len()).map_err(|_| too_large(payload.len()))?;
    let (header, body) = destination.split_at_mut(FRAME_HEADER_BYTES);
    header[4..8].copy_from_slice(&payload_len.to_le_bytes());
    header[8..16].copy_from_slice(&log_number.to_le_bytes());
    body.copy_from_slice(payload);
    let checksum = checksum(&header[4..16], body);
    header[0..4].copy_from_slice(&checksum.to_le_bytes());
    Ok(())
}

/// CRC-32C over the header fields after the checksum, then the payload.
fn checksum(fields: &[u8], payload: &[u8]) -> u32 {
    crc32c::crc32c_append(crc32c::crc32c(fields), payload)
}

/// The error for a payload no frame can carry.
fn too_large(payload_len: usize) -> Error {
    Error::TooLarge {
        what: "log frame payload",
        size: u64::try_from(payload_len).unwrap_or(u64::MAX),
        limit: u64::try_from(MAX_LOG_PAYLOAD_BYTES).unwrap_or(u64::MAX),
    }
}
