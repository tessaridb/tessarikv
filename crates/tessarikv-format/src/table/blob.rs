//! Blob files: large values kept apart from the tables that index them.
//!
//! A blob file is a run of blocks, one per value, each written and sealed as a table block is
//! (the keyspace's compression, a CRC-32C seal, and when the store is encrypted, AEAD with the
//! block's offset as the nonce). It has no index or footer of its own: a value is reached only
//! through the [`BlobIndex`] a table entry holds, and a table is verified before its entries are
//! read. A reader still checks the handle against the file's length before reading, and the seal
//! before using the bytes.

use std::sync::Arc;

use super::Compression;
use super::block::Block;
use super::builder::BLOB_FILES_PROPERTY;
use super::handle::{BlockHandle, write_block};
use super::unseal::unseal_owned;
use crate::error::{Error, Result};
use crate::seal::{FileSealer, SealHeader};

/// Where a value is: the blob file's number and the value's block in it. Encoded as the file
/// number and the handle's offset and size, each a little-endian u64.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlobIndex {
    /// The blob file's number.
    pub file: u64,
    /// The value's block in the file.
    pub handle: BlockHandle,
}

impl BlobIndex {
    /// Bytes of an encoded index.
    pub const BYTES: usize = 24;

    /// The index as a table entry stores it.
    #[must_use]
    pub fn encode(&self) -> [u8; Self::BYTES] {
        let mut bytes = [0; Self::BYTES];
        bytes[..8].copy_from_slice(&self.file.to_le_bytes());
        bytes[8..].copy_from_slice(&self.handle.encode());
        bytes
    }

    /// Decodes an index a table entry stores.
    ///
    /// # Errors
    ///
    /// [`Error::Malformed`] when `bytes` is not an encoded index.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let malformed = Error::Malformed {
            what: "blob index",
            reason: "not 24 bytes",
        };
        if bytes.len() != Self::BYTES {
            return Err(malformed);
        }
        let file = bytes
            .get(..8)
            .and_then(|field| field.try_into().ok())
            .map(u64::from_le_bytes)
            .ok_or(malformed)?;
        Ok(Self {
            file,
            handle: BlockHandle::decode(bytes.get(8..).unwrap_or_default())?,
        })
    }
}

/// Writes the values of one blob file, a value at a time.
#[derive(Debug)]
pub struct BlobBuilder {
    file: u64,
    compression: Compression,
    /// Finished bytes not yet taken; they follow the `drained` bytes taken before.
    out: Vec<u8>,
    drained: u64,
    values: u64,
    sealer: Option<Arc<FileSealer>>,
}

impl BlobBuilder {
    /// An empty blob file numbered `file`, its values stored with `compression`.
    #[must_use]
    pub fn new(file: u64, compression: Compression) -> Self {
        Self {
            file,
            compression,
            out: Vec::new(),
            drained: 0,
            values: 0,
            sealer: None,
        }
    }

    /// An empty sealed blob file, as [`BlobBuilder::new`]: the file starts with `header`, and
    /// each value is encrypted by `sealer`, the key `header` derives for a blob file.
    #[must_use]
    pub fn sealed(
        file: u64,
        compression: Compression,
        sealer: Arc<FileSealer>,
        header: &SealHeader,
    ) -> Self {
        let mut builder = Self::new(file, compression);
        builder.out.extend_from_slice(&header.encode());
        builder.sealer = Some(sealer);
        builder
    }

    /// Appends `value` and returns where it is.
    ///
    /// # Errors
    ///
    /// [`Error::TooLarge`] when the value outgrows the format's length fields, and the sealer's
    /// error when sealing fails.
    pub fn add(&mut self, value: &[u8]) -> Result<BlobIndex> {
        let handle = write_block(
            &mut self.out,
            self.drained,
            value,
            self.compression,
            self.sealer.as_deref(),
        )?;
        self.values = self.values.saturating_add(1);
        Ok(BlobIndex {
            file: self.file,
            handle,
        })
    }

    /// Values added so far.
    #[must_use]
    pub fn values(&self) -> u64 {
        self.values
    }

    /// The blob file's number.
    #[must_use]
    pub fn file(&self) -> u64 {
        self.file
    }

    /// Bytes the file holds so far, taken or not.
    #[must_use]
    pub fn size(&self) -> u64 {
        self.drained
            .saturating_add(u64::try_from(self.out.len()).unwrap_or(u64::MAX))
    }

    /// The bytes finished and not yet taken, which continue the file after those taken before.
    pub fn take_finished(&mut self) -> Vec<u8> {
        let taken = std::mem::take(&mut self.out);
        self.drained = self
            .drained
            .saturating_add(u64::try_from(taken.len()).unwrap_or(u64::MAX));
        taken
    }

    /// The rest of the file.
    #[must_use]
    pub fn finish(self) -> Vec<u8> {
        self.out
    }
}

/// The value in `sealed`, the bytes of the block `handle` names, read from its blob file and
/// opened with the file's `sealer` when the file is encrypted.
///
/// # Errors
///
/// [`Error::Malformed`] for a block whose checksum or codec does not hold, and
/// [`Error::Unauthenticated`] for an encrypted block that fails authentication.
pub fn open_blob(
    sealed: Vec<u8>,
    handle: BlockHandle,
    sealer: Option<&FileSealer>,
) -> Result<Vec<u8>> {
    unseal_owned(sealed, handle.offset(), sealer)
}

/// The blob files the properties block `payload` names; none when it names none.
pub(super) fn blob_files_of(payload: Vec<u8>) -> Result<Arc<[u64]>> {
    let properties = Block::new(payload, |left, right| left.cmp(right))?;
    let mut property = properties.iter();
    property.seek(BLOB_FILES_PROPERTY);
    if !(property.valid() && property.key() == BLOB_FILES_PROPERTY) {
        property.status()?;
        return Ok(Arc::from([]));
    }
    let bytes = property.value();
    if bytes.len() % 8 != 0 {
        return Err(Error::Malformed {
            what: "table properties",
            reason: "blob file list is not whole numbers",
        });
    }
    let files: Vec<u64> = bytes
        .as_chunks::<8>()
        .0
        .iter()
        .map(|chunk| u64::from_le_bytes(*chunk))
        .collect();
    if files.windows(2).any(|pair| pair.first() >= pair.get(1)) {
        return Err(Error::Malformed {
            what: "table properties",
            reason: "blob file list is not increasing",
        });
    }
    Ok(files.into())
}
