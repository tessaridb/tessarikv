//! A frame ready to be logged, as a writer hands it to the log writer task.

use tessarikv_format::{FRAME_HEADER_BYTES, frame_bytes};

use crate::{Error, Result};

/// A frame ready to be logged: header space, then the payload, with the payload's checksum.
pub(crate) struct Frame {
    pub(super) bytes: Vec<u8>,
    pub(super) checksum: u32,
}

impl Frame {
    /// A frame from `bytes`: [`FRAME_HEADER_BYTES`] of header space, which the log writer
    /// fills in, followed by the payload, whose
    /// [`payload_checksum`](tessarikv_format::payload_checksum) is `checksum`.
    ///
    /// # Errors
    ///
    /// [`Error::Unsupported`] for a payload larger than a frame carries.
    pub(crate) fn with_checksum(bytes: Vec<u8>, checksum: u32) -> Result<Self> {
        let payload_len =
            bytes
                .len()
                .checked_sub(FRAME_HEADER_BYTES)
                .ok_or(Error::Unsupported {
                    what: "a log frame without room for its header",
                })?;
        frame_bytes(payload_len).map_err(|_| Error::Unsupported {
            what: "a log frame larger than the frame limit",
        })?;
        Ok(Self { bytes, checksum })
    }

    /// A frame from `bytes`, as [`Frame::with_checksum`], computing the checksum.
    #[cfg(test)]
    pub(crate) fn new(bytes: Vec<u8>) -> Result<Self> {
        let checksum =
            tessarikv_format::payload_checksum(bytes.get(FRAME_HEADER_BYTES..).unwrap_or_default());
        Self::with_checksum(bytes, checksum)
    }
}
