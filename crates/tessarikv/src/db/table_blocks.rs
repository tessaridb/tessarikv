//! The data blocks of one table file: answered from the block cache when they are there, read
//! from storage when they are not.
//!
//! A table's walk asks [`TableBlocks`] for each data block it reaches. A block in the cache is
//! answered at once, on the caller's thread; a block that is not makes the walk wait, and the
//! reader then calls [`TableBlocks::load`]. The block is read on the caller's thread when the
//! storage can read it without blocking (memory, or a page the file system already holds).
//! Otherwise a worker of a multi-thread runtime reads it in place after handing its other tasks
//! to another thread, and any other caller hands the read to a blocking thread, so no task waits
//! behind a device read. A hand-off read whose caller is dropped still finishes its one block on
//! that thread (a blocking task cannot be stopped), and its result is discarded.

use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tessarikv_constants::LEVELS;
use tessarikv_format::{
    Block, BlockHandle, BlockSource, FileKind, FileSealer, OpenError, SEAL_HEADER_BYTES, Table,
    TableIter, unseal_block,
};
use tessarikv_types::compare_internal;

use super::block_cache::{BlockCache, BlockKey};
use super::flush_job::table_path;
use super::tables::{format_error, user_key};
use super::view::TableFile;
use crate::storage::{ReadableFile, Storage};
use crate::version::Version;
use crate::{Error, Result};

/// The data blocks of one open table file.
pub(crate) struct TableBlocks {
    id: u64,
    path: PathBuf,
    /// The open file; none when open files are bounded and the cache keeps the handle.
    file: Option<Arc<dyn ReadableFile>>,
    len: u64,
    cache: Arc<BlockCache>,
    /// The file's key, when it is encrypted.
    sealer: Option<Arc<FileSealer>>,
}

impl fmt::Debug for TableBlocks {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TableBlocks")
            .field("id", &self.id)
            .field("path", &self.path)
            .field("len", &self.len)
            .finish_non_exhaustive()
    }
}

impl BlockSource for TableBlocks {
    fn block(&self, handle: BlockHandle) -> tessarikv_format::Result<Option<Block>> {
        Ok(self.cache.get(self.key(handle)))
    }
}

impl TableBlocks {
    /// The block at `handle`, read from storage and verified, and kept in the cache unless
    /// `fill_cache` is false. A read the storage can make without blocking is made here, on the
    /// caller's thread; any other is made in place on a multi-thread runtime's worker, and on a
    /// blocking thread elsewhere.
    ///
    /// # Errors
    ///
    /// The storage's error for a failed read, [`Error::Corruption`] for a damaged block, and
    /// [`Error::ReadFailed`] when the reading thread stopped without an answer.
    pub(crate) async fn load(
        self: &Arc<Self>,
        handle: BlockHandle,
        fill_cache: bool,
    ) -> Result<Block> {
        let block = match self.read_inline(handle)? {
            Some(block) => block,
            // A worker of a multi-thread runtime reads in place, its other tasks moving to another
            // thread meanwhile; any other caller hands the read to a blocking thread.
            None if multi_thread() => tokio::task::block_in_place(|| self.read(handle))?,
            None => {
                let blocks = Arc::clone(self);
                tokio::task::spawn_blocking(move || blocks.read(handle))
                    .await
                    .map_err(|_| Error::ReadFailed)??
            }
        };
        self.cache.count_read(handle.size());
        if fill_cache {
            self.cache.insert(self.key(handle), block.clone());
        }
        Ok(block)
    }

    /// Finishes the move of `versions`, a walk of this file's table, loading each block it
    /// waits for.
    ///
    /// # Errors
    ///
    /// As [`TableBlocks::load`]; the move is then left waiting.
    pub(crate) async fn complete(
        self: &Arc<Self>,
        versions: &mut TableIter,
        fill_cache: bool,
    ) -> Result<()> {
        while let Some(handle) = versions.pending() {
            let block = self.load(handle, fill_cache).await?;
            versions.resume(block);
        }
        Ok(())
    }

    /// Finishes the move of `versions` on this blocking thread, reading each block it waits
    /// for without keeping it in the cache: a compaction reads every block once.
    ///
    /// # Errors
    ///
    /// As [`TableBlocks::load`].
    pub(crate) fn complete_blocking(&self, versions: &mut TableIter) -> Result<()> {
        while let Some(handle) = versions.pending() {
            versions.resume(self.read(handle)?);
        }
        Ok(())
    }

    /// Reads and verifies the block at `handle`. Runs on a blocking thread.
    fn read(&self, handle: BlockHandle) -> Result<Block> {
        let mut sealed = self.buffer(handle)?;
        let file = match &self.file {
            Some(file) => Arc::clone(file),
            None => self.cache.file(self.id, &self.path)?,
        };
        file.read_at(handle.offset(), &mut sealed)?;
        self.unseal(handle, &sealed)
    }

    /// Reads and verifies the block at `handle` if the storage can do it without blocking;
    /// `None` when it cannot.
    fn read_inline(&self, handle: BlockHandle) -> Result<Option<Block>> {
        // A file let go is opened again only where blocking is allowed.
        let Some(file) = self
            .file
            .clone()
            .or_else(|| self.cache.cached_file(self.id))
        else {
            return Ok(None);
        };
        let mut sealed = self.buffer(handle)?;
        if !file.try_read_at(handle.offset(), &mut sealed)? {
            return Ok(None);
        }
        self.unseal(handle, &sealed).map(Some)
    }

    /// A buffer for the sealed block at `handle`. The handle comes from a verified index, but
    /// the file is still untrusted: nothing is allocated for a block that does not fit inside it.
    fn buffer(&self, handle: BlockHandle) -> Result<Vec<u8>> {
        let size = handle
            .offset()
            .checked_add(handle.size())
            .filter(|end| *end <= self.len)
            .and_then(|_| usize::try_from(handle.size()).ok())
            .ok_or_else(|| Error::Corruption {
                what: "table",
                path: Some(self.path.clone()),
                offset: Some(handle.offset()),
                detail: "a block handle points past the file".to_owned(),
            })?;
        Ok(vec![0; size])
    }

    fn unseal(&self, handle: BlockHandle, sealed: &[u8]) -> Result<Block> {
        unseal_block(
            sealed,
            handle.offset(),
            self.sealer.as_deref(),
            compare_internal,
        )
        .map_err(|error| format_error(&error, Some(&self.path)))
    }

    fn key(&self, handle: BlockHandle) -> BlockKey {
        BlockKey {
            file: self.id,
            offset: handle.offset(),
        }
    }
}

/// Whether the caller runs on a multi-thread runtime, where a worker may block in place.
fn multi_thread() -> bool {
    tokio::runtime::Handle::try_current()
        .is_ok_and(|runtime| runtime.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread)
}

/// Opens table file `id` at `path`, keeping its metadata in memory and reading its data blocks
/// through `cache`. The metadata is read from `written` when the caller still holds the bytes it
/// just wrote, and from the file otherwise. Runs on a blocking thread.
///
/// # Errors
///
/// The storage's error for a failed open or read, and [`Error::Corruption`] for a file that is
/// not a whole table of this format.
pub(crate) fn open_table(
    storage: &dyn Storage,
    path: &Path,
    id: u64,
    cache: &Arc<BlockCache>,
    written: Option<(u64, &[u8])>,
) -> Result<(Table, Arc<TableBlocks>)> {
    let file = storage.open(path)?;
    let len = file.len()?;
    let sealer = match cache.sealing() {
        Some(sealing) => {
            let mut header = [0; SEAL_HEADER_BYTES];
            file.read_at(0, &mut header)?;
            Some(sealing.open_file(&header, FileKind::Table, path)?)
        }
        None => None,
    };
    let blocks = Arc::new(TableBlocks {
        id,
        path: path.to_path_buf(),
        file: if cache.bounds_open_files() {
            cache.offer_file(id, Arc::clone(&file));
            None
        } else {
            Some(Arc::clone(&file))
        },
        len,
        cache: Arc::clone(cache),
        sealer,
    });
    // The bytes just written from `base` on answer what they cover; the file answers the rest.
    let read = |offset: u64, buf: &mut [u8]| {
        let found = written.and_then(|(base, bytes)| {
            let start = usize::try_from(offset.checked_sub(base)?).ok()?;
            bytes.get(start..start.checked_add(buf.len())?)
        });
        match found {
            Some(found) => {
                buf.copy_from_slice(found);
                Ok(())
            }
            None => file.read_at(offset, buf),
        }
    };
    let source: Arc<dyn BlockSource> = Arc::<TableBlocks>::clone(&blocks);
    let table = Table::open_with(
        len,
        read,
        source,
        blocks.sealer.as_deref(),
        compare_internal,
        user_key,
    )
    .map_err(|error| match error {
        OpenError::Format(error) => format_error(&error, Some(path)),
        OpenError::Read(error) => error,
    })?;
    Ok((table, blocks))
}

/// Opens every table file `version` names, newest first, reading their data blocks through
/// `cache`, and removes the table files it does not name: those a crash left between writing a
/// table and naming it in the manifest. Runs on a blocking thread.
pub(super) fn restore_tables(
    storage: &dyn Storage,
    dir: &Path,
    version: &Version,
    cache: &Arc<BlockCache>,
) -> Result<Vec<TableFile>> {
    let mut files = Vec::new();
    let keyspaces = std::iter::once(0).chain(version.keyspaces().map(|(id, _)| id));
    for keyspace in keyspaces {
        for meta in (0..LEVELS).flat_map(|level| version.tables(keyspace, level)) {
            let path = table_path(dir, meta.id);
            let (table, blocks) = open_table(storage, &path, meta.id, cache, None)?;
            files.push(TableFile {
                meta: Arc::clone(meta),
                table,
                blocks,
            });
        }
    }
    for name in storage.list(dir)? {
        let named = name
            .strip_suffix(".sst")
            .and_then(|stem| stem.parse::<u64>().ok())
            .map(|id| files.iter().any(|file| file.meta.id == id));
        if named == Some(false) {
            storage.remove(&dir.join(&name))?;
        }
    }
    Ok(files)
}
