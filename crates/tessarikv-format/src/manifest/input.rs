//! Reading the fields of a manifest edit, every length checked against the bytes left.

use crate::error::{Error, Result};
use crate::varint::{get_u32, get_u64, put_u32};

pub(super) fn put_key(out: &mut Vec<u8>, key: &[u8]) {
    // Keys longer than a u32 cannot be built: batch entries are bounded far below it.
    put_u32(out, u32::try_from(key.len()).unwrap_or(u32::MAX));
    out.extend_from_slice(key);
}

pub(super) fn set_once<T>(slot: &mut Option<T>, value: T) -> Result<()> {
    if slot.replace(value).is_some() {
        return Err(Error::Malformed {
            what: "manifest edit",
            reason: "a field given twice",
        });
    }
    Ok(())
}

/// The bytes of an edit not yet decoded.
pub(super) struct Input<'a> {
    pub(super) bytes: &'a [u8],
}

impl<'a> Input<'a> {
    pub(super) fn byte(&mut self, what: &'static str) -> Result<u8> {
        let (&first, rest) = self.bytes.split_first().ok_or(Error::Truncated {
            what,
            needed: 1,
            available: 0,
        })?;
        self.bytes = rest;
        Ok(first)
    }

    pub(super) fn u32(&mut self) -> Result<u32> {
        let (value, used) = get_u32(self.bytes).ok_or_else(|| self.bad_varint(5))?;
        self.advance(used);
        Ok(value)
    }

    pub(super) fn u64(&mut self) -> Result<u64> {
        let (value, used) = get_u64(self.bytes).ok_or_else(|| self.bad_varint(10))?;
        self.advance(used);
        Ok(value)
    }

    pub(super) fn key(&mut self) -> Result<Vec<u8>> {
        let len = usize::try_from(self.u32()?).unwrap_or(usize::MAX);
        let (key, rest) = self
            .bytes
            .split_at_checked(len)
            .ok_or_else(|| Error::Truncated {
                what: "manifest edit key",
                needed: u64::try_from(len).unwrap_or(u64::MAX),
                available: u64::try_from(self.bytes.len()).unwrap_or(u64::MAX),
            })?;
        self.bytes = rest;
        Ok(key.to_vec())
    }

    /// Whether every byte has been read.
    pub(super) fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    /// Steps over a skippable field: its length, then that many bytes.
    pub(super) fn skip(&mut self) -> Result<()> {
        let len = usize::try_from(self.u32()?).unwrap_or(usize::MAX);
        self.bytes = self.bytes.get(len..).ok_or_else(|| Error::Truncated {
            what: "manifest edit skippable field",
            needed: u64::try_from(len).unwrap_or(u64::MAX),
            available: u64::try_from(self.bytes.len()).unwrap_or(u64::MAX),
        })?;
        Ok(())
    }

    fn advance(&mut self, used: usize) {
        self.bytes = self.bytes.get(used..).unwrap_or_default();
    }

    /// Why an integer of at most `max_bytes` bytes did not decode.
    fn bad_varint(&self, max_bytes: usize) -> Error {
        if self.bytes.len() >= max_bytes {
            return Error::Malformed {
                what: "manifest edit integer",
                reason: "longer than its type",
            };
        }
        Error::Truncated {
            what: "manifest edit integer",
            needed: 1,
            available: u64::try_from(self.bytes.len()).unwrap_or(u64::MAX),
        }
    }
}
