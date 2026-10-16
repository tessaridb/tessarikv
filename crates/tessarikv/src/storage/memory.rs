//! Files kept entirely in memory: the backend of an in-memory database.
//!
//! The directory is a lock-free map (`papaya`) from path to the file's published bytes; each
//! file is a single-writer, many-reader [`tessarikv_sync::AppendWriter`] buffer, so a read
//! never waits for the writer appending to the same file. Replacing a path leaves readers of the
//! old file on the old bytes, as an unlinked file behaves on disk.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use tessarikv_constants::MEMORY_FILE_CHUNK_BYTES;
use tessarikv_sync::{AppendReader, AppendWriter, BufferError, append_buffer};

use super::{ReadableFile, Storage, StorageLock, WritableFile};
use crate::{Error, Result};

/// A file system in memory. Cheap to clone; clones share the same files.
#[derive(Clone, Default)]
pub struct MemoryStorage {
    inner: Arc<Inner>,
}

#[derive(Default)]
struct Inner {
    files: papaya::HashMap<PathBuf, AppendReader>,
    locks: papaya::HashSet<PathBuf>,
}

impl std::fmt::Debug for MemoryStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryStorage")
            .field("files", &self.inner.files.len())
            .finish()
    }
}

impl MemoryStorage {
    /// An empty in-memory file system.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

fn buffer_error(path: &Path, error: BufferError) -> Error {
    match error {
        BufferError::OutOfRange {
            requested_end,
            available,
        } => Error::ShortRead {
            path: path.to_path_buf(),
            requested_end,
            available,
        },
        BufferError::Overflow => Error::Io {
            operation: "append",
            path: path.to_path_buf(),
            source: std::io::Error::other(error),
        },
    }
}

impl Storage for MemoryStorage {
    fn create(&self, path: &Path) -> Result<Box<dyn WritableFile>> {
        let (writer, reader) =
            append_buffer(MEMORY_FILE_CHUNK_BYTES).map_err(|error| buffer_error(path, error))?;
        self.inner.files.pin().insert(path.to_path_buf(), reader);
        Ok(Box::new(MemoryWritable {
            writer,
            path: path.to_path_buf(),
        }))
    }

    fn open(&self, path: &Path) -> Result<Arc<dyn ReadableFile>> {
        let reader = self
            .inner
            .files
            .pin()
            .get(path)
            .cloned()
            .ok_or_else(|| Error::NotFound {
                path: path.to_path_buf(),
            })?;
        Ok(Arc::new(MemoryReadable {
            reader,
            path: path.to_path_buf(),
        }))
    }

    fn exists(&self, path: &Path) -> Result<bool> {
        Ok(self.inner.files.pin().contains_key(path))
    }

    fn remove(&self, path: &Path) -> Result<()> {
        self.inner
            .files
            .pin()
            .remove(path)
            .map(|_| ())
            .ok_or_else(|| Error::NotFound {
                path: path.to_path_buf(),
            })
    }

    fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        let files = self.inner.files.pin();
        let reader = files.get(from).cloned().ok_or_else(|| Error::NotFound {
            path: from.to_path_buf(),
        })?;
        // `to` is replaced in one insert, so a reader of `to` sees the old file or the new one.
        files.insert(to.to_path_buf(), reader);
        if from != to {
            files.remove(from);
        }
        Ok(())
    }

    fn list(&self, dir: &Path) -> Result<Vec<String>> {
        Ok(self
            .inner
            .files
            .pin()
            .keys()
            .filter(|path| path.parent() == Some(dir))
            .filter_map(|path| path.file_name()?.to_str().map(str::to_owned))
            .collect())
    }

    fn create_dir_all(&self, _dir: &Path) -> Result<()> {
        Ok(())
    }

    fn sync_dir(&self, _dir: &Path) -> Result<()> {
        Ok(())
    }

    fn lock(&self, path: &Path) -> Result<Box<dyn StorageLock>> {
        if self.inner.locks.pin().insert(path.to_path_buf()) {
            Ok(Box::new(MemoryLock {
                inner: Arc::clone(&self.inner),
                path: path.to_path_buf(),
            }))
        } else {
            Err(Error::Locked {
                path: path.to_path_buf(),
            })
        }
    }
}

struct MemoryWritable {
    writer: AppendWriter,
    path: PathBuf,
}

impl WritableFile for MemoryWritable {
    fn append(&mut self, data: &[u8]) -> Result<()> {
        self.writer
            .append(data)
            .map_err(|error| buffer_error(&self.path, error))
    }

    /// Memory has nothing to flush: an appended byte is already as durable as memory gets.
    fn sync(&mut self) -> Result<()> {
        Ok(())
    }

    fn len(&self) -> u64 {
        self.writer.len()
    }
}

struct MemoryReadable {
    reader: AppendReader,
    path: PathBuf,
}

impl ReadableFile for MemoryReadable {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<()> {
        self.reader
            .read_at(offset, buf)
            .map_err(|error| buffer_error(&self.path, error))
    }

    /// Memory never waits: the read is always made here.
    fn try_read_at(&self, offset: u64, buf: &mut [u8]) -> Result<bool> {
        self.read_at(offset, buf).map(|()| true)
    }

    fn len(&self) -> Result<u64> {
        Ok(self.reader.len())
    }
}

struct MemoryLock {
    inner: Arc<Inner>,
    path: PathBuf,
}

impl std::fmt::Debug for MemoryLock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryLock")
            .field("path", &self.path)
            .finish()
    }
}

impl StorageLock for MemoryLock {}

impl Drop for MemoryLock {
    fn drop(&mut self) {
        self.inner.locks.pin().remove(&self.path);
    }
}
