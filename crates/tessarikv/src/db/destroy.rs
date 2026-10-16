//! Destroying a database: removing every file the engine wrote in its directory.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::Db;
use super::key_check::{KEY_CHECK, KEY_CHECK_WRITING};
use super::open::{IDENTITY, LOCK};
use super::options_file::PREFIX as OPTIONS_PREFIX;
use crate::storage::{OsStorage, Storage};
use crate::version::manifest::{CURRENT, CURRENT_TEMPORARY};
use crate::{Error, Result};

impl Db {
    /// Removes the database at `path` on the operating system's file system: every file the
    /// engine wrote there, and nothing else. The directory itself stays, with any file the
    /// engine did not write. A missing database is nothing to remove.
    ///
    /// The log goes first, then the file naming the current manifest, then the rest, and the
    /// identity file last, so a crash part way leaves no database that opens with part of the
    /// old data; destroying again removes what is left.
    ///
    /// # Errors
    ///
    /// [`Error::Locked`] while the database is open, here or in another process, and
    /// [`Error::Io`] when storage fails.
    pub async fn destroy(path: impl AsRef<Path>) -> Result<()> {
        Self::destroy_with_storage(path, Arc::new(OsStorage)).await
    }

    /// Removes the database at `path` on `storage`, as [`Db::destroy`] does on the operating
    /// system's file system.
    ///
    /// # Errors
    ///
    /// As [`Db::destroy`].
    pub async fn destroy_with_storage(
        path: impl AsRef<Path>,
        storage: Arc<dyn Storage>,
    ) -> Result<()> {
        let dir = path.as_ref().to_path_buf();
        tokio::task::spawn_blocking(move || destroy(storage.as_ref(), &dir))
            .await
            .map_err(|_| Error::Unsupported {
                what: "destroying a database after its thread panicked",
            })?
    }
}

/// Removes the engine's files in `dir` under its lock, in the order [`Db::destroy`] states. Runs
/// on a blocking thread.
fn destroy(storage: &dyn Storage, dir: &Path) -> Result<()> {
    let names = match storage.list(dir) {
        Ok(names) => names,
        Err(Error::NotFound { .. }) => return Ok(()),
        Err(error) => return Err(error),
    };
    let lock = storage.lock(&dir.join(LOCK))?;
    let owned: Vec<&String> = names.iter().filter(|name| written_here(name)).collect();
    let stage = |name: &str| -> u8 {
        if name.ends_with(".log") {
            0
        } else if name == CURRENT || name == CURRENT_TEMPORARY {
            1
        } else if name == IDENTITY {
            3
        } else {
            2
        }
    };
    let mut ordered = owned;
    ordered.sort_by_key(|name| stage(name));
    for name in ordered {
        remove(storage, dir.join(name))?;
    }
    // A held lock keeps its file from removal on some systems.
    drop(lock);
    remove(storage, dir.join(LOCK))?;
    storage.sync_dir(dir)
}

/// Whether the engine writes a file named `name`.
fn written_here(name: &str) -> bool {
    name.ends_with(".sst")
        || name.ends_with(".log")
        || name.starts_with("MANIFEST-")
        || name.starts_with(OPTIONS_PREFIX)
        || [
            CURRENT,
            CURRENT_TEMPORARY,
            IDENTITY,
            KEY_CHECK,
            KEY_CHECK_WRITING,
        ]
        .contains(&name)
}

/// Removes `path`; one already gone is not an error.
fn remove(storage: &dyn Storage, path: PathBuf) -> Result<()> {
    match storage.remove(&path) {
        Ok(()) | Err(Error::NotFound { .. }) => Ok(()),
        Err(error) => Err(error),
    }
}
