//! Encryption at rest in the engine: the key of each file, and the key check every open makes.
//!
//! A file is encrypted under a key of its own, derived from a caller's root key and the random
//! salt of the seal header it starts with; the header names the root key's id, so a file always
//! opens with the key it was written under. New files use the id the options name.
//!
//! An encrypted database holds a key-check file, written when the database is created: a header
//! and one sealed unit. Every open reads it first, so a wrong or missing key is refused before
//! any other file is read, and a database written without encryption is never mistaken for an
//! encrypted one, or the other way round.

use std::path::Path;
use std::sync::Arc;

use tessarikv_format::{Cipher, FileKind, FileSealer, RootKey, SEAL_HEADER_BYTES, SealHeader};

use crate::options::Encryption;
use crate::storage::{Storage, read_all};
use crate::{Error, Result};

/// The key-check file's name.
const KEY_CHECK: &str = "KEYCHECK";
/// The key-check file's name while it is being written.
const KEY_CHECK_WRITING: &str = "KEYCHECK.writing";
/// What the key-check file seals.
const KEY_CHECK_TEXT: &[u8] = b"tessarikv key check";

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

/// Checks the key of the database in `dir` against its key-check file, writing that file when
/// an encrypted database is created; `existing` tells whether the database already holds data.
/// Runs on a blocking thread, before any other file is read.
///
/// # Errors
///
/// [`Error::WrongKey`] for a key that does not open the database, [`Error::KeyNotFound`] when no
/// key is given for the database's key id, and [`Error::Unsupported`] for encryption asked of a
/// database written without it.
pub(super) fn check_key(
    storage: &dyn Storage,
    dir: &Path,
    sealing: Option<&Sealing>,
    existing: bool,
) -> Result<()> {
    let path = dir.join(KEY_CHECK);
    if !storage.exists(&path)? {
        return match sealing {
            Some(_) if existing => Err(Error::Unsupported {
                what: "encrypting a database written without encryption",
            }),
            Some(sealing) => write_key_check(storage, dir, sealing),
            None => Ok(()),
        };
    }
    let bytes = read_all(storage, &path, "key check")?;
    let header = SealHeader::decode(&bytes).map_err(|error| corruption(&path, &error))?;
    let key_id = header.key_id;
    let Some(sealing) = sealing else {
        return Err(Error::KeyNotFound { key_id });
    };
    let sealer = sealing.open_file(&bytes, FileKind::KeyCheck, &path)?;
    let sealed = bytes.get(SEAL_HEADER_BYTES..).unwrap_or_default();
    match sealer.open(position(), sealed) {
        Ok(text) if text == KEY_CHECK_TEXT => Ok(()),
        _ => Err(Error::WrongKey { key_id }),
    }
}

/// Writes the key-check file beside its name, syncs it, then renames it into place.
fn write_key_check(storage: &dyn Storage, dir: &Path, sealing: &Sealing) -> Result<()> {
    let (header, sealer) = sealing.new_file(FileKind::KeyCheck)?;
    let mut bytes = header.encode().to_vec();
    let sealed = sealer
        .seal(position(), KEY_CHECK_TEXT)
        .map_err(|error| corruption(&dir.join(KEY_CHECK), &error))?;
    bytes.extend_from_slice(&sealed);
    let writing = dir.join(KEY_CHECK_WRITING);
    let mut file = storage.create(&writing)?;
    file.append(&bytes)?;
    file.sync()?;
    drop(file);
    storage.rename(&writing, &dir.join(KEY_CHECK))?;
    storage.sync_dir(dir)
}

/// Where the key-check file's sealed unit starts: right after its header.
fn position() -> u64 {
    u64::try_from(SEAL_HEADER_BYTES).unwrap_or(u64::MAX)
}

fn corruption(path: &Path, error: &tessarikv_format::Error) -> Error {
    Error::Corruption {
        what: "encrypted file",
        path: Some(path.to_path_buf()),
        offset: None,
        detail: error.to_string(),
    }
}
