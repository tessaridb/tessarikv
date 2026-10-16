//! Encryption at rest: the key of each file the engine writes or reads.
//!
//! A file is encrypted under a key of its own, derived from a caller's root key and the random
//! salt of the seal header it starts with; the header names the root key's id, so a file always
//! opens with the key it was written under. New files use the id the options name.

use std::path::Path;
use std::sync::Arc;

use tessarikv_format::{Cipher, FileKind, FileSealer, RootKey, SealHeader};

use crate::options::Encryption;
use crate::{Error, Result};

/// How the engine encrypts the files it writes and opens the ones it reads.
#[derive(Debug)]
pub(crate) struct Sealing {
    encryption: Encryption,
    cipher: Cipher,
}

impl Sealing {
    /// Sealing under `encryption`, with the cipher this processor runs fastest.
    pub(crate) fn new(encryption: &Encryption) -> Self {
        Self {
            encryption: encryption.clone(),
            cipher: Cipher::preferred(),
        }
    }

    /// The header and key of a new file of `kind`, under the current key id.
    pub(crate) fn new_file(&self, kind: FileKind) -> Result<(SealHeader, Arc<FileSealer>)> {
        let key_id = self.encryption.key_id();
        let header = SealHeader::generate(self.cipher, key_id).map_err(|_| Error::Unsupported {
            what: "encrypting a file without the operating system's random source",
        })?;
        let root = self.root_key(key_id)?;
        Ok((header, Arc::new(FileSealer::new(&root, &header, kind))))
    }

    /// The key of the file of `kind` at `path`, which starts with `header`.
    pub(crate) fn open_file(
        &self,
        header: &[u8],
        kind: FileKind,
        path: &Path,
    ) -> Result<Arc<FileSealer>> {
        let header = SealHeader::decode(header).map_err(|error| corruption(path, &error))?;
        let root = self.root_key(header.key_id)?;
        Ok(Arc::new(FileSealer::new(&root, &header, kind)))
    }

    fn root_key(&self, key_id: u32) -> Result<RootKey> {
        self.encryption
            .provider()
            .root_key(key_id)
            .ok_or(Error::KeyNotFound { key_id })
    }
}

/// A sealed file that cannot be read as one.
pub(crate) fn corruption(path: &Path, error: &tessarikv_format::Error) -> Error {
    Error::Corruption {
        what: "encrypted file",
        path: Some(path.to_path_buf()),
        offset: None,
        detail: error.to_string(),
    }
}
