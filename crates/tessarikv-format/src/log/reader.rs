//! Reading a log: the longest prefix of valid frames, and how it ended.

use tessarikv_constants::MAX_LOG_PAYLOAD_BYTES;

use super::{FRAME_HEADER_BYTES, checksum};

/// How a log's valid prefix ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EndKind {
    /// The input ended, or zero bytes followed: nothing more was written.
    Clean,
    /// A frame was cut short or failed its checksum: a write that did not complete.
    Torn,
    /// A valid frame of another log followed: bytes left from an earlier use of the file.
    Foreign,
}

/// Where and how a log's valid prefix ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogEnd {
    /// Bytes of valid frames before the end.
    pub offset: u64,
    /// What was found at `offset`.
    pub kind: EndKind,
}

/// One step of reading a log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogRead<'a> {
    /// The payload of the next valid frame.
    Frame(&'a [u8]),
    /// The valid prefix ended; every later call returns the same end.
    End(LogEnd),
}

/// Reads the frames of one log, in order, from its bytes.
#[derive(Debug, Clone)]
pub struct FrameReader<'a> {
    bytes: &'a [u8],
    log_number: u64,
    offset: usize,
}

impl<'a> FrameReader<'a> {
    /// A reader of `bytes`, accepting only frames written for log `log_number`.
    pub fn new(bytes: &'a [u8], log_number: u64) -> Self {
        Self {
            bytes,
            log_number,
            offset: 0,
        }
    }

    /// The next frame, or how the valid prefix ended.
    pub fn next_frame(&mut self) -> LogRead<'a> {
        match self.frame_at(self.offset) {
            Ok((payload, next)) => {
                self.offset = next;
                LogRead::Frame(payload)
            }
            Err(kind) => LogRead::End(LogEnd {
                offset: u64::try_from(self.offset).unwrap_or(u64::MAX),
                kind,
            }),
        }
    }

    /// The frame starting at `start` and the offset after it, or why there is none.
    fn frame_at(&self, start: usize) -> Result<(&'a [u8], usize), EndKind> {
        let rest = self.bytes.get(start..).unwrap_or_default();
        let Some(header) = rest.get(..FRAME_HEADER_BYTES) else {
            return Err(if rest.iter().all(|byte| *byte == 0) {
                EndKind::Clean
            } else {
                EndKind::Torn
            });
        };
        if header.iter().all(|byte| *byte == 0) {
            return Err(EndKind::Clean);
        }
        let stored = u32::from_le_bytes(field(header, 0)?);
        let payload_len =
            usize::try_from(u32::from_le_bytes(field(header, 4)?)).map_err(|_| EndKind::Torn)?;
        let log_number = u64::from_le_bytes(field(header, 8)?);
        // The length is checked against the limit before it is used to address anything.
        if payload_len > MAX_LOG_PAYLOAD_BYTES {
            return Err(EndKind::Torn);
        }
        let payload = rest
            .get(FRAME_HEADER_BYTES..)
            .and_then(|body| body.get(..payload_len))
            .ok_or(EndKind::Torn)?;
        let fields = header.get(4..).ok_or(EndKind::Torn)?;
        if checksum(fields, payload) != stored {
            return Err(EndKind::Torn);
        }
        if log_number != self.log_number {
            return Err(EndKind::Foreign);
        }
        let next = start
            .checked_add(FRAME_HEADER_BYTES)
            .and_then(|end| end.checked_add(payload_len))
            .ok_or(EndKind::Torn)?;
        Ok((payload, next))
    }
}

/// The `N` header bytes at `at`.
fn field<const N: usize>(header: &[u8], at: usize) -> Result<[u8; N], EndKind> {
    header
        .get(at..)
        .and_then(|rest| rest.get(..N))
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or(EndKind::Torn)
}
