//! Where the engine's files live: the operating system's file system, or memory.
//!
//! The engine reaches files only through [`Storage`], so the same engine code runs on disk and
//! entirely in memory (an in-memory database is the engine on [`MemoryStorage`]). A backend is
//! chosen at open time, which is why the trait is used as an object.
//!
//! Every path the engine passes is inside its database directory; backends do not interpret
//! paths beyond that.

mod memory;
mod os;

#[cfg(test)]
mod tests;

use std::path::Path;
use std::sync::Arc;

pub use memory::MemoryStorage;
pub use os::OsStorage;

use crate::{Error, Result};

/// A file system the engine can keep a database in.
pub trait Storage: Send + Sync + std::fmt::Debug {
    /// Creates `path`, replacing any file there, and returns its only writer.
    ///
    /// Readers already open on a replaced file keep reading the old contents.
    ///
    /// # Errors
    ///
    /// The backend's I/O failure.
    fn create(&self, path: &Path) -> Result<Box<dyn WritableFile>>;

    /// Opens `path` for concurrent positional reads.
    ///
    /// # Errors
    ///
    /// [`crate::Error::NotFound`] when absent; the backend's I/O failure otherwise.
    fn open(&self, path: &Path) -> Result<Arc<dyn ReadableFile>>;

    /// Whether `path` names a file.
    ///
    /// # Errors
    ///
    /// The backend's I/O failure.
    fn exists(&self, path: &Path) -> Result<bool>;

    /// Removes the file at `path`.
    ///
    /// # Errors
    ///
    /// [`crate::Error::NotFound`] when absent; the backend's I/O failure otherwise.
    fn remove(&self, path: &Path) -> Result<()>;

    /// Moves `from` to `to`, replacing `to` atomically: a reader of `to` sees the old file or
    /// the new one, never neither.
    ///
    /// # Errors
    ///
    /// [`crate::Error::NotFound`] when `from` is absent; the backend's I/O failure otherwise.
    fn rename(&self, from: &Path, to: &Path) -> Result<()>;

    /// The names of the files directly inside `dir`, in no particular order.
    ///
    /// # Errors
    ///
    /// The backend's I/O failure.
    fn list(&self, dir: &Path) -> Result<Vec<String>>;

    /// Creates `dir` and its parents.
    ///
    /// # Errors
    ///
    /// The backend's I/O failure.
    fn create_dir_all(&self, dir: &Path) -> Result<()>;

    /// Makes the directory entries of `dir` (creations, renames, removals) durable.
    ///
    /// # Errors
    ///
    /// The backend's I/O failure.
    fn sync_dir(&self, dir: &Path) -> Result<()>;

    /// Takes the exclusive lock at `path`, held until the guard is dropped.
    ///
    /// # Errors
    ///
    /// [`crate::Error::Locked`] when another holder has it.
    fn lock(&self, path: &Path) -> Result<Box<dyn StorageLock>>;
}

/// The single writer of one file.
pub trait WritableFile: Send {
    /// Appends `data` at the end of the file.
    ///
    /// # Errors
    ///
    /// The backend's I/O failure.
    fn append(&mut self, data: &[u8]) -> Result<()>;

    /// Makes everything appended so far durable against power loss.
    ///
    /// # Errors
    ///
    /// The backend's I/O failure. After a failed sync the data's durability is unknown: the
    /// caller must treat the file as unsynced and recover, never retry and continue.
    fn sync(&mut self) -> Result<()>;

    /// Bytes appended so far.
    fn len(&self) -> u64;

    /// Whether nothing has been appended.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// A file read by position from any number of threads at once.
pub trait ReadableFile: Send + Sync {
    /// Fills `buf` from `offset`.
    ///
    /// # Errors
    ///
    /// [`crate::Error::ShortRead`] when the file ends first; the backend's I/O failure otherwise.
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<()>;

    /// The file's current length.
    ///
    /// # Errors
    ///
    /// The backend's I/O failure.
    fn len(&self) -> Result<u64>;

    /// Whether the file holds no bytes.
    ///
    /// # Errors
    ///
    /// The backend's I/O failure.
    fn is_empty(&self) -> Result<bool> {
        Ok(self.len()? == 0)
    }
}

/// An exclusive lock on a database directory, released on drop.
pub trait StorageLock: Send + Sync + std::fmt::Debug {}

/// Every byte of the file at `path`, which holds `what`.
///
/// # Errors
///
/// [`Error::Corruption`] when the file is larger than memory can address, and whatever opening
/// or reading the file returns.
pub(crate) fn read_all(storage: &dyn Storage, path: &Path, what: &'static str) -> Result<Vec<u8>> {
    let file = storage.open(path)?;
    let len = usize::try_from(file.len()?).map_err(|_| Error::Corruption {
        what,
        path: Some(path.to_path_buf()),
        offset: Some(0),
        detail: "the file is larger than memory can address".to_owned(),
    })?;
    let mut bytes = vec![0; len];
    file.read_at(0, &mut bytes)?;
    Ok(bytes)
}
