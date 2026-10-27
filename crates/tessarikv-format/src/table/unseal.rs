//! Opening a sealed block: decrypting it at its offset when the file is encrypted, verifying its
//! checksum, and restoring its payload.

use super::codec;
use super::handle::SEAL_BYTES;
use crate::error::{Error, Result};
use crate::seal::FileSealer;

/// The verified, uncompressed payload of a sealed block: its stored bytes and its seal, first
/// decrypted as the unit at `at` when the file has a `sealer`.
pub(super) fn unseal(sealed: &[u8], at: u64, sealer: Option<&FileSealer>) -> Result<Vec<u8>> {
    match sealer {
        Some(sealer) => unseal_plain(sealer.open(at, sealed)?),
        None => {
            let (stored, kind) = verified(sealed)?;
            codec::decompress(kind, stored).map_err(corrupt)
        }
    }
}

/// As [`unseal`], taking the sealed block as a buffer of its own: a block stored as it is
/// becomes the payload in place, with no second allocation or copy.
pub(super) fn unseal_owned(
    sealed: Vec<u8>,
    at: u64,
    sealer: Option<&FileSealer>,
) -> Result<Vec<u8>> {
    match sealer {
        Some(sealer) => unseal_plain(sealer.open(at, &sealed)?),
        None => unseal_plain(sealed),
    }
}

/// The payload of `plain`, a block's stored bytes and seal after any decryption.
fn unseal_plain(mut plain: Vec<u8>) -> Result<Vec<u8>> {
    let (stored, kind) = verified(&plain)?;
    if codec::is_stored(kind) {
        let len = stored.len();
        plain.truncate(len);
        return Ok(plain);
    }
    codec::decompress(kind, stored).map_err(corrupt)
}

/// The stored bytes of `sealed` and the codec byte of its seal, once its checksum is verified.
fn verified(sealed: &[u8]) -> Result<(&[u8], u8)> {
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
    // A fuzzing build skips the comparison so mutated input reaches the decompressors.
    if !cfg!(fuzzing) && crc32c::crc32c_append(crc32c::crc32c(stored), &[kind]) != expected {
        return Err(corrupt("checksum mismatch"));
    }
    Ok((stored, kind))
}

fn corrupt(reason: &'static str) -> Error {
    Error::Malformed {
        what: "table block",
        reason,
    }
}
