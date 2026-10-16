//! The key check every open of an encrypted database makes before it reads anything else.
//!
//! An encrypted database holds a key-check file, written when the database is created: a seal
//! header and one sealed unit. Every open reads it first, so a wrong or missing key is refused
//! before any other file is read, and a database written without encryption is never mistaken
//! for an encrypted one, or the other way round. After a key rotation the file is rewritten under
//! the current key, so a retired key is needed only while some data file names it.

use std::path::Path;

use tessarikv_format::{FileKind, SEAL_HEADER_BYTES, SealHeader};

use crate::sealing::{Sealing, corruption};
use crate::storage::{Storage, read_all};
use crate::{Error, Result};

/// The key-check file's name.
pub(super) const KEY_CHECK: &str = "KEYCHECK";
/// The key-check file's name while it is being written.
pub(super) const KEY_CHECK_WRITING: &str = "KEYCHECK.writing";
/// What the key-check file seals.
const KEY_CHECK_TEXT: &[u8] = b"tessarikv key check";

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
        // A file checked under an older key is written again under the current one, so a
        // retired key is needed only while a data file still names it.
        Ok(text) if text == KEY_CHECK_TEXT && key_id != sealing.key_id() => {
            write_key_check(storage, dir, sealing)
        }
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
