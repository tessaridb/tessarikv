//! Durable flushes, per platform.

use std::fs::File;
use std::path::Path;

use crate::Result;

/// Flushes a file's data to stable storage.
///
/// Whether the log may run syncs of one segment side by side. Measured: on Linux overlapping
/// syncs raise synced-write throughput at 2 to 16 writers; on macOS each full flush empties the
/// drive's cache in turn, so overlapping them only splits a group commit into more flushes
/// (30 to 40 % slower at 8 to 16 writers). Windows is not measured and keeps one sync at a time.
pub(super) const SYNCS_OVERLAP: bool = cfg!(target_os = "linux");

/// Apple platforms need `F_FULLFSYNC`: a plain `fsync` there returns once the drive has the
/// data in its volatile cache.
#[cfg(target_vendor = "apple")]
pub(super) fn full_sync(file: &File) -> std::io::Result<()> {
    rustix::fs::fcntl_fullfsync(file).map_err(std::io::Error::from)
}

/// Flushes a file's data to stable storage: `fdatasync` on Linux, `FlushFileBuffers` on Windows.
#[cfg(not(target_vendor = "apple"))]
pub(super) fn full_sync(file: &File) -> std::io::Result<()> {
    file.sync_data()
}

/// Makes a directory's entries (creations, renames, removals) durable.
#[cfg(unix)]
pub(super) fn sync_directory(dir: &Path) -> Result<()> {
    File::open(dir)
        .and_then(|handle| handle.sync_all())
        .map_err(|error| crate::Error::io("sync directory", dir, error))
}

/// Windows has no directory handle to flush: NTFS journals metadata changes itself.
#[cfg(not(unix))]
pub(super) fn sync_directory(_dir: &Path) -> Result<()> {
    Ok(())
}
