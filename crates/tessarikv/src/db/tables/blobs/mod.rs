//! Blob files as the database uses them: the values a keyspace keeps apart from its tables,
//! read through the index a table entry holds.
//!
//! A blob file is written beside the table that first refers to it, by the same flush or
//! compaction, and synced with it before the manifest edit naming the table. It is never named
//! in the manifest itself: the tables that refer to it record its number (see
//! [`tessarikv_format::Table::blob_files`]), and every open table file holds the blob files it
//! refers to. A blob file is therefore read only through a live table, and is removed once the
//! last table holding it is gone — retired from every version and released by its last reader
//! — the way a table file is removed once nothing reads it. At open, blob files no table names
//! are what a crash left between writing a table and naming it, and are removed.

mod writer;

#[cfg(test)]
mod files_tests;
#[cfg(test)]
mod tests;

use std::collections::HashMap;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tessarikv_format::{
    BlobIndex, BlockHandle, FileKind, FileSealer, SEAL_HEADER_BYTES, open_blob,
};

use crate::db::block_cache::BlockCache;
use crate::db::table_blocks::multi_thread;
use crate::storage::{ReadableFile, Storage};
use crate::{Error, Result};

pub(crate) use writer::BlobWriter;

/// An open blob file.
pub(crate) struct BlobFile {
    id: u64,
    path: PathBuf,
    file: Arc<dyn ReadableFile>,
    len: u64,
    /// The file's key, when it is encrypted.
    sealer: Option<Arc<FileSealer>>,
    /// Whether a read that may block runs on the reading task's thread.
    reads_on_worker: bool,
    cache: Arc<BlockCache>,
}

impl fmt::Debug for BlobFile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BlobFile")
            .field("id", &self.id)
            .field("path", &self.path)
            .field("len", &self.len)
            .finish_non_exhaustive()
    }
}

impl BlobFile {
    /// Opens blob file `id` in `dir`, keyed by `cache`'s sealing when the store is encrypted.
    /// Runs on a blocking thread.
    ///
    /// # Errors
    ///
    /// The storage's error for a failed open or read, and the sealing's for a header that is
    /// not one or a key the provider lacks.
    pub(crate) fn open(
        storage: &dyn Storage,
        dir: &Path,
        id: u64,
        cache: &Arc<BlockCache>,
    ) -> Result<Arc<Self>> {
        let path = blob_path(dir, id);
        let file = storage.open(&path)?;
        let len = file.len()?;
        let sealer = match cache.sealing() {
            Some(sealing) => {
                let mut header = [0; SEAL_HEADER_BYTES];
                file.read_at(0, &mut header)?;
                Some(sealing.open_file(&header, FileKind::Blob, &path)?)
            }
            None => None,
        };
        Ok(Arc::new(Self {
            id,
            path,
            file,
            len,
            sealer,
            reads_on_worker: cache.reads_on_worker(),
            cache: Arc::clone(cache),
        }))
    }

    /// The file's number.
    pub(crate) fn id(&self) -> u64 {
        self.id
    }

    /// The value `handle` names, read where a table block is read (see
    /// [`super::table_blocks::TableBlocks::load`]): on this task's thread when the storage
    /// promises not to block or the database reads blocks on its workers, in place on a worker
    /// of a multi-thread runtime, and on a blocking thread otherwise.
    ///
    /// # Errors
    ///
    /// As [`BlobFile::read_blocking`], and [`Error::ReadFailed`] when the reading thread stopped
    /// without an answer.
    pub(crate) async fn read(self: &Arc<Self>, handle: BlockHandle) -> Result<Vec<u8>> {
        let len = self.checked_len(handle)?;
        let value = match self.file.try_read_vec_at(handle.offset(), len)? {
            Some(sealed) => self.unseal(handle, sealed)?,
            None if self.reads_on_worker => self.read_blocking(handle)?,
            None if multi_thread() => tokio::task::block_in_place(|| self.read_blocking(handle))?,
            None => {
                let blob = Arc::clone(self);
                tokio::task::spawn_blocking(move || blob.read_blocking(handle))
                    .await
                    .map_err(|_| Error::ReadFailed)??
            }
        };
        Ok(value)
    }

    /// The value `handle` names, read and verified on this thread.
    ///
    /// # Errors
    ///
    /// The storage's error for a failed read, and [`Error::Corruption`] for a handle past the
    /// file or a value whose seal does not hold.
    pub(crate) fn read_blocking(&self, handle: BlockHandle) -> Result<Vec<u8>> {
        let len = self.checked_len(handle)?;
        let sealed = self.file.read_vec_at(handle.offset(), len)?;
        self.unseal(handle, sealed)
    }

    /// The length of the sealed value at `handle`. The handle comes from a verified table, but
    /// the blob file is still untrusted: nothing is allocated for a value that does not fit in
    /// it.
    fn checked_len(&self, handle: BlockHandle) -> Result<usize> {
        handle
            .offset()
            .checked_add(handle.size())
            .filter(|end| *end <= self.len)
            .and_then(|_| usize::try_from(handle.size()).ok())
            .ok_or_else(|| Error::Corruption {
                what: "blob file",
                path: Some(self.path.clone()),
                offset: Some(handle.offset()),
                detail: "a blob index points past the file".to_owned(),
            })
    }

    fn unseal(&self, handle: BlockHandle, sealed: Vec<u8>) -> Result<Vec<u8>> {
        self.cache.count_read(handle.size());
        open_blob(sealed, handle, self.sealer.as_deref()).map_err(|error| Error::Corruption {
            what: "blob file",
            path: Some(self.path.clone()),
            offset: Some(handle.offset()),
            detail: error.to_string(),
        })
    }
}

/// The file of blob file `id` in `dir`.
pub(crate) fn blob_path(dir: &Path, id: u64) -> PathBuf {
    dir.join(format!("{id:020}.blob"))
}

/// The blob file a stored index names, found among `blobs`, the blob files of the table that
/// stores it, and the place of the value in it.
///
/// # Errors
///
/// [`Error::Corruption`] for bytes that are not an index, or an index naming a blob file the
/// table does not record.
pub(crate) fn locate<'b>(
    blobs: &'b [Arc<BlobFile>],
    stored: &[u8],
) -> Result<(&'b Arc<BlobFile>, BlockHandle)> {
    let index = BlobIndex::decode(stored).map_err(|error| Error::Corruption {
        what: "table",
        path: None,
        offset: None,
        detail: error.to_string(),
    })?;
    let blob = blobs
        .binary_search_by_key(&index.file, |blob| blob.id)
        .ok()
        .and_then(|at| blobs.get(at))
        .ok_or_else(|| Error::Corruption {
            what: "table",
            path: None,
            offset: None,
            detail: "a blob index names a blob file the table does not record".to_owned(),
        })?;
    Ok((blob, index.handle))
}

/// The blob files `ids` names, in its order, found in `open`, the blob files already open, or
/// opened from `dir` and added to it. Runs on a blocking thread.
///
/// # Errors
///
/// As [`BlobFile::open`].
pub(crate) fn resolve(
    ids: &[u64],
    open: &mut HashMap<u64, Arc<BlobFile>>,
    storage: &dyn Storage,
    dir: &Path,
    cache: &Arc<BlockCache>,
) -> Result<Arc<[Arc<BlobFile>]>> {
    let mut blobs = Vec::with_capacity(ids.len());
    for &id in ids {
        let blob = match open.get(&id) {
            Some(blob) => Arc::clone(blob),
            None => {
                let blob = BlobFile::open(storage, dir, id, cache)?;
                open.insert(id, Arc::clone(&blob));
                blob
            }
        };
        blobs.push(blob);
    }
    Ok(blobs.into())
}

/// The blob files among `known` that a table recording `ids` refers to, in `ids`' order.
///
/// # Errors
///
/// [`Error::Corruption`] when a recorded number is not among `known`.
pub(crate) fn among(ids: &[u64], known: &[Arc<BlobFile>]) -> Result<Arc<[Arc<BlobFile>]>> {
    ids.iter()
        .map(|id| {
            known
                .iter()
                .find(|blob| blob.id == *id)
                .cloned()
                .ok_or_else(|| Error::Corruption {
                    what: "table",
                    path: None,
                    offset: None,
                    detail: "a table records a blob file its job did not hold".to_owned(),
                })
        })
        .collect()
}
