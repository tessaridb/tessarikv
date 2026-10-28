//! Sealing: authenticated encryption of the units the engine writes whole.
//!
//! A sealed file starts with a [`SealHeader`] in the clear: the cipher, the id of the caller's
//! root key, and 32 random bytes of salt. Its key is derived from the root key with HKDF-SHA256,
//! salted by that header and labelled by the file's kind, so every file has its own key. Each
//! unit the engine writes once and never rewrites (a table block, a log frame, a manifest record)
//! is sealed with that key under the AEAD the header names, its nonce the unit's position in
//! the file and its associated data the header, the kind and the position: a unit opens only
//! with its key, in its file, at its place. A file is written once, front to back, and a unit is
//! never rewritten in place, so a nonce never repeats under a key and nothing about it needs
//! storing; a file cut short by a crash is not appended to again.
//!
//! Opening returns the plaintext or one error, [`Error::Unauthenticated`], for every cause.
//! What stays visible: names, sizes and the lengths of units.

mod header;
#[cfg(test)]
mod tests;

pub use header::SealHeader;

use aes_gcm::Aes256Gcm;
use aes_gcm::aead::{Aead, AeadInOut, KeyInit, Payload};
use chacha20poly1305::ChaCha20Poly1305;
use hkdf::Hkdf;
use sha2::Sha256;
use zeroize::Zeroizing;

use crate::{Error, Result};

/// Bytes of a [`SealHeader`].
pub const HEADER_BYTES: usize = 42;
/// Bytes a seal adds to a unit: the authentication tag.
pub const TAG_BYTES: usize = 16;
/// Bytes of a root key.
pub const ROOT_KEY_BYTES: usize = 32;

/// The largest unit either cipher seals: 2^36 - 32 bytes, the AES-GCM bound.
const MAX_UNIT_BYTES: u64 = (1 << 36) - 32;

const LABEL: &[u8] = b"tessarikv/sealed-file/v1/";

/// The authenticated encryption a file is sealed with.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Cipher {
    /// AES-256-GCM: the choice where the processor has AES instructions.
    Aes256Gcm,
    /// ChaCha20-Poly1305: constant-time in software, the choice elsewhere.
    ChaCha20Poly1305,
}

impl Cipher {
    /// AES-256-GCM where this processor has AES instructions, ChaCha20-Poly1305 otherwise.
    #[must_use]
    pub fn preferred() -> Self {
        if aes_instructions() {
            Self::Aes256Gcm
        } else {
            Self::ChaCha20Poly1305
        }
    }

    const fn code(self) -> u8 {
        match self {
            Self::Aes256Gcm => 1,
            Self::ChaCha20Poly1305 => 2,
        }
    }

    const fn from_code(code: u8) -> Option<Self> {
        match code {
            1 => Some(Self::Aes256Gcm),
            2 => Some(Self::ChaCha20Poly1305),
            _ => None,
        }
    }
}

#[cfg(target_arch = "x86_64")]
fn aes_instructions() -> bool {
    std::arch::is_x86_feature_detected!("aes")
}

#[cfg(target_arch = "aarch64")]
fn aes_instructions() -> bool {
    std::arch::is_aarch64_feature_detected!("aes")
}

#[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
fn aes_instructions() -> bool {
    false
}

/// What a sealed file holds, part of its key's label so no two kinds share a key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum FileKind {
    /// A table file.
    Table,
    /// A write-ahead log segment.
    Log,
    /// A manifest.
    Manifest,
    /// A record of the options a database was opened with.
    Options,
    /// The file an open checks its key against.
    KeyCheck,
    /// A blob file of large values.
    Blob,
}

impl FileKind {
    const fn label(self) -> &'static [u8] {
        match self {
            Self::Table => b"table",
            Self::Log => b"log",
            Self::Manifest => b"manifest",
            Self::Options => b"options",
            Self::KeyCheck => b"key-check",
            Self::Blob => b"blob",
        }
    }
}

/// A caller's root key; the bytes are wiped when it is dropped.
pub struct RootKey(Zeroizing<[u8; ROOT_KEY_BYTES]>);

impl RootKey {
    /// The root key `bytes`.
    #[must_use]
    pub fn new(bytes: [u8; ROOT_KEY_BYTES]) -> Self {
        Self(Zeroizing::new(bytes))
    }
}

impl std::fmt::Debug for RootKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RootKey(..)")
    }
}

/// The key of one sealed file, ready to seal and open its units.
pub struct FileSealer {
    aead: SealingAead,
    /// The header and kind, the start of every unit's associated data.
    context: Vec<u8>,
}

enum SealingAead {
    Aes(Box<Aes256Gcm>),
    ChaCha(Box<ChaCha20Poly1305>),
}

impl FileSealer {
    /// The key of the file `header` starts, of `kind`, derived from `root`.
    #[must_use]
    pub fn new(root: &RootKey, header: &SealHeader, kind: FileKind) -> Self {
        let encoded = header.encode();
        let mut label = Vec::with_capacity(LABEL.len().saturating_add(kind.label().len()));
        label.extend_from_slice(LABEL);
        label.extend_from_slice(kind.label());
        let mut key = Zeroizing::new([0_u8; 32]);
        // A 32-byte output is far below HKDF-SHA256's limit of 255 blocks, so expanding cannot fail.
        let _ = Hkdf::<Sha256>::new(Some(&encoded), root.0.as_slice())
            .expand(&label, key.as_mut_slice());
        let aead = match header.cipher {
            Cipher::Aes256Gcm => SealingAead::Aes(Box::new(Aes256Gcm::new((&*key).into()))),
            Cipher::ChaCha20Poly1305 => {
                SealingAead::ChaCha(Box::new(ChaCha20Poly1305::new((&*key).into())))
            }
        };
        let mut context = Vec::with_capacity(HEADER_BYTES.saturating_add(label.len()));
        context.extend_from_slice(&encoded);
        context.extend_from_slice(&label);
        Self { aead, context }
    }

    /// `plaintext` sealed as the unit at `position`: its ciphertext then its tag.
    ///
    /// The caller seals at most one unit at each position of a file; a second, different unit
    /// at the same position would reuse the nonce under the file's key.
    ///
    /// # Errors
    ///
    /// [`Error::TooLarge`] for a plaintext past the cipher's limit of 64 GiB.
    pub fn seal(&self, position: u64, plaintext: &[u8]) -> Result<Vec<u8>> {
        let aad = self.aad(position);
        let nonce = nonce(position);
        let payload = Payload {
            msg: plaintext,
            aad: &aad,
        };
        match &self.aead {
            SealingAead::Aes(aead) => aead.encrypt((&nonce).into(), payload),
            SealingAead::ChaCha(aead) => aead.encrypt((&nonce).into(), payload),
        }
        .map_err(|_| too_large(plaintext.len()))
    }

    /// `buffer` sealed in place as the unit at `position`, exactly as [`FileSealer::seal`]
    /// seals it, without a copy: `buffer` then holds the ciphertext, and the returned tag
    /// follows it in the sealed unit.
    ///
    /// # Errors
    ///
    /// [`Error::TooLarge`] as for [`FileSealer::seal`].
    pub fn seal_in_place(&self, position: u64, buffer: &mut [u8]) -> Result<[u8; TAG_BYTES]> {
        let aad = self.aad(position);
        let nonce = nonce(position);
        let len = buffer.len();
        let tag = match &self.aead {
            SealingAead::Aes(aead) => {
                aead.encrypt_inout_detached((&nonce).into(), &aad, buffer.into())
            }
            SealingAead::ChaCha(aead) => {
                aead.encrypt_inout_detached((&nonce).into(), &aad, buffer.into())
            }
        }
        .map_err(|_| too_large(len))?;
        let mut bytes = [0; TAG_BYTES];
        bytes.copy_from_slice(&tag);
        Ok(bytes)
    }

    /// The plaintext of the unit sealed at `position`.
    ///
    /// # Errors
    ///
    /// [`Error::Unauthenticated`] when the unit was sealed with another key, in another file or
    /// place, or was changed: one error for every cause, and no plaintext.
    pub fn open(&self, position: u64, sealed: &[u8]) -> Result<Vec<u8>> {
        let aad = self.aad(position);
        let nonce = nonce(position);
        let payload = Payload {
            msg: sealed,
            aad: &aad,
        };
        match &self.aead {
            SealingAead::Aes(aead) => aead.decrypt((&nonce).into(), payload),
            SealingAead::ChaCha(aead) => aead.decrypt((&nonce).into(), payload),
        }
        .map_err(|_| Error::Unauthenticated {
            what: "sealed unit",
        })
    }

    fn aad(&self, position: u64) -> Vec<u8> {
        let mut aad = Vec::with_capacity(self.context.len().saturating_add(8));
        aad.extend_from_slice(&self.context);
        aad.extend_from_slice(&position.to_le_bytes());
        aad
    }
}

impl std::fmt::Debug for FileSealer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("FileSealer(..)")
    }
}

fn too_large(len: usize) -> Error {
    Error::TooLarge {
        what: "sealed unit",
        size: u64::try_from(len).unwrap_or(u64::MAX),
        limit: MAX_UNIT_BYTES,
    }
}

/// The nonce of the unit at `position`: unique within the file, whose key is its own.
fn nonce(position: u64) -> [u8; 12] {
    let mut nonce = [0; 12];
    if let Some(start) = nonce.get_mut(..8) {
        start.copy_from_slice(&position.to_le_bytes());
    }
    nonce
}
