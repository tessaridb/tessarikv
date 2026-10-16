//! The clear start of a sealed file: magic, version, cipher, root key id and salt.

use super::{Cipher, HEADER_BYTES};
use crate::{Error, Result};

const MAGIC: [u8; 4] = *b"TKVS";
const VERSION: u8 = 1;
pub(super) const SALT_BYTES: usize = 32;

/// The clear start of a sealed file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SealHeader {
    /// The cipher every unit of the file is sealed with.
    pub cipher: Cipher,
    /// The id of the root key the file's key is derived from.
    pub key_id: u32,
    /// Random bytes making the file's key its own.
    pub salt: [u8; SALT_BYTES],
}

impl SealHeader {
    /// A header for a new file, its salt from the operating system's random source.
    ///
    /// # Errors
    ///
    /// [`Error::Malformed`] when the random source fails.
    pub fn generate(cipher: Cipher, key_id: u32) -> Result<Self> {
        let mut salt = [0; SALT_BYTES];
        getrandom::fill(&mut salt).map_err(|_| Error::Malformed {
            what: "seal header",
            reason: "the operating system's random source failed",
        })?;
        Ok(Self {
            cipher,
            key_id,
            salt,
        })
    }

    /// The header's bytes.
    #[must_use]
    pub fn encode(&self) -> [u8; HEADER_BYTES] {
        let mut bytes = [0; HEADER_BYTES];
        let (magic, rest) = bytes.split_at_mut(MAGIC.len());
        magic.copy_from_slice(&MAGIC);
        let (fields, salt) = rest.split_at_mut(6);
        fields.copy_from_slice(&[VERSION, self.cipher.code(), 0, 0, 0, 0]);
        if let Some(key_id) = fields.get_mut(2..6) {
            key_id.copy_from_slice(&self.key_id.to_le_bytes());
        }
        salt.copy_from_slice(&self.salt);
        bytes
    }

    /// Reads a header from the start of `bytes`.
    ///
    /// # Errors
    ///
    /// [`Error::Truncated`], [`Error::UnsupportedVersion`], or [`Error::Malformed`] for a wrong
    /// magic or an unknown cipher.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        const WHAT: &str = "seal header";
        let header: &[u8; HEADER_BYTES] = bytes
            .get(..HEADER_BYTES)
            .and_then(|header| header.try_into().ok())
            .ok_or(Error::Truncated {
                what: WHAT,
                needed: u64::try_from(HEADER_BYTES).unwrap_or(u64::MAX),
                available: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
            })?;
        let (magic, rest) = header.split_at(MAGIC.len());
        if magic != MAGIC {
            return Err(Error::Malformed {
                what: WHAT,
                reason: "not a sealed file",
            });
        }
        let (fields, salt) = rest.split_at(6);
        let [version, cipher, a, b, c, d] =
            <[u8; 6]>::try_from(fields).map_err(|_| Error::Malformed {
                what: WHAT,
                reason: "short fields",
            })?;
        if version != VERSION {
            return Err(Error::UnsupportedVersion {
                what: WHAT,
                version,
            });
        }
        let cipher = Cipher::from_code(cipher).ok_or(Error::Malformed {
            what: WHAT,
            reason: "unknown cipher",
        })?;
        Ok(Self {
            cipher,
            key_id: u32::from_le_bytes([a, b, c, d]),
            salt: <[u8; SALT_BYTES]>::try_from(salt).map_err(|_| Error::Malformed {
                what: WHAT,
                reason: "short salt",
            })?,
        })
    }
}
