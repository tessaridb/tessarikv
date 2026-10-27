//! The operating system's file system.
//!
//! Platform differences live in `flush` (durable data and directory flushes) and
//! `positional` (positional reads); everything else is portable `std`.

use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::{FileSyncer, ReadableFile, Storage, StorageLock, WritableFile};
use flush::{SYNCS_OVERLAP, full_sync, sync_directory};
use positional::read_exact_at;

mod flush;
mod positional;
use crate::{Error, Result};

/// Files on the local file system.
///
/// `sync` is a real flush to stable storage: `fdatasync` on Linux and `F_FULLFSYNC` on Apple
/// platforms, where a plain `fsync` leaves the data in the drive's cache.
#[derive(Debug, Default, Clone, Copy)]
pub struct OsStorage;

impl Storage for OsStorage {
    fn create(&self, path: &Path) -> Result<Box<dyn WritableFile>> {
        let file = File::create(path).map_err(|error| Error::io("create", path, error))?;
        Ok(Box::new(OsWritable {
            file: Arc::new(OsSyncer {
                file,
                path: path.to_path_buf(),
            }),
            len: 0,
        }))
    }

    fn open(&self, path: &Path) -> Result<Arc<dyn ReadableFile>> {
        let file = File::open(path).map_err(|error| Error::io("open", path, error))?;
        Ok(Arc::new(OsReadable {
            file,
            path: path.to_path_buf(),
        }))
    }

    fn exists(&self, path: &Path) -> Result<bool> {
        path.try_exists()
            .map_err(|error| Error::io("stat", path, error))
    }

    fn remove(&self, path: &Path) -> Result<()> {
        fs::remove_file(path).map_err(|error| Error::io("remove", path, error))
    }

    fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        fs::rename(from, to).map_err(|error| Error::io("rename", from, error))
    }

    fn link(&self, from: &Path, to: &Path) -> bool {
        // Any failure (another file system, no support) leaves the copy to the caller, which
        // reports a real I/O fault when it hits one.
        fs::hard_link(from, to).is_ok()
    }

    fn list(&self, dir: &Path) -> Result<Vec<String>> {
        let mut names = Vec::new();
        for entry in fs::read_dir(dir).map_err(|error| Error::io("list", dir, error))? {
            let entry = entry.map_err(|error| Error::io("list", dir, error))?;
            let is_file = entry
                .file_type()
                .map_err(|error| Error::io("stat", &entry.path(), error))?
                .is_file();
            if is_file && let Some(name) = entry.file_name().to_str() {
                names.push(name.to_owned());
            }
        }
        Ok(names)
    }

    fn create_dir_all(&self, dir: &Path) -> Result<()> {
        fs::create_dir_all(dir).map_err(|error| Error::io("create directory", dir, error))
    }

    fn sync_dir(&self, dir: &Path) -> Result<()> {
        sync_directory(dir)
    }

    fn lock(&self, path: &Path) -> Result<Box<dyn StorageLock>> {
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(path)
            .map_err(|error| Error::io("open lock", path, error))?;
        match file.try_lock() {
            Ok(()) => Ok(Box::new(OsLock { _file: file })),
            Err(fs::TryLockError::WouldBlock) => Err(Error::Locked {
                path: path.to_path_buf(),
            }),
            Err(fs::TryLockError::Error(error)) => Err(Error::io("lock", path, error)),
        }
    }
}

/// The writer's open file, shared with the syncers it hands out: every sync goes through the same
/// open file description as the appends, so a failed write-back is reported to one of them.
struct OsSyncer {
    file: File,
    path: PathBuf,
}

impl FileSyncer for OsSyncer {
    fn sync(&self) -> Result<()> {
        full_sync(&self.file).map_err(|error| Error::io("sync", &self.path, error))
    }
}

struct OsWritable {
    file: Arc<OsSyncer>,
    len: u64,
}

impl WritableFile for OsWritable {
    fn append(&mut self, data: &[u8]) -> Result<()> {
        (&self.file.file)
            .write_all(data)
            .map_err(|error| Error::io("write", &self.file.path, error))?;
        // A slice length always fits a u64 on supported targets; saturate rather than wrap.
        let added = u64::try_from(data.len()).unwrap_or(u64::MAX);
        self.len = self.len.saturating_add(added);
        Ok(())
    }

    fn sync(&mut self) -> Result<()> {
        self.file.sync()
    }

    fn len(&self) -> u64 {
        self.len
    }

    fn syncer(&self) -> Option<Arc<dyn FileSyncer>> {
        let syncer: Arc<dyn FileSyncer> = self.file.clone();
        SYNCS_OVERLAP.then_some(syncer)
    }
}

struct OsReadable {
    file: File,
    path: PathBuf,
}

impl ReadableFile for OsReadable {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<()> {
        match read_exact_at(&self.file, offset, buf) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => {
                let wanted = u64::try_from(buf.len()).unwrap_or(u64::MAX);
                Err(Error::ShortRead {
                    path: self.path.clone(),
                    requested_end: offset.saturating_add(wanted),
                    available: self.len()?,
                })
            }
            Err(error) => Err(Error::io("read", &self.path, error)),
        }
    }

    #[cfg(any(target_vendor = "apple", target_os = "linux", target_os = "android"))]
    fn read_vec_at(&self, offset: u64, len: usize) -> Result<Vec<u8>> {
        match positional::read_vec_at(&self.file, offset, len) {
            Ok(buf) => Ok(buf),
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => {
                let wanted = u64::try_from(len).unwrap_or(u64::MAX);
                Err(Error::ShortRead {
                    path: self.path.clone(),
                    requested_end: offset.saturating_add(wanted),
                    available: self.len()?,
                })
            }
            Err(error) => Err(Error::io("read", &self.path, error)),
        }
    }

    fn try_read_at(&self, offset: u64, buf: &mut [u8]) -> Result<bool> {
        Ok(positional::read_exact_at_nowait(&self.file, offset, buf))
    }

    /// No read here promises not to wait: answer without allocating a buffer for nothing.
    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    fn try_read_vec_at(&self, _offset: u64, _len: usize) -> Result<Option<Vec<u8>>> {
        Ok(None)
    }

    fn len(&self) -> Result<u64> {
        self.file
            .metadata()
            .map(|metadata| metadata.len())
            .map_err(|error| Error::io("stat", &self.path, error))
    }
}

#[derive(Debug)]
struct OsLock {
    /// The lock lives as long as this handle; the OS releases it when the file closes.
    _file: File,
}

impl StorageLock for OsLock {}
