//! The data blocks of one table file: answered from the block cache when they are there, read
//! from storage on a blocking thread when they are not.
//!
//! A table's walk asks [`TableBlocks`] for each data block it reaches. A block in the cache is
//! answered at once, on the caller's thread; a block that is not makes the walk wait, and the
//! reader then calls [`TableBlocks::load`], which reads and verifies the block on a blocking
//! thread so the runtime's workers never wait for storage. A read whose caller is dropped still
//! finishes its one block on that thread (a blocking task cannot be stopped), and its result is
//! discarded.

use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tessarikv_format::{
    Block, BlockHandle, BlockSource, OpenError, Table, TableIter, unseal_block,
};
use tessarikv_types::compare_internal;

use super::block_cache::{BlockCache, BlockKey};
use super::tables::{format_error, user_key};
use crate::storage::{ReadableFile, Storage};
use crate::{Error, Result};

/// The data blocks of one open table file.
pub(crate) struct TableBlocks {
    id: u64,
    path: PathBuf,
    file: Arc<dyn ReadableFile>,
    len: u64,
    cache: Arc<BlockCache>,
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
    /// The block at `handle`, read from storage and verified on a blocking thread, and kept in
    /// the cache unless `fill_cache` is false.
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
        let blocks = Arc::clone(self);
        let block = tokio::task::spawn_blocking(move || blocks.read(handle))
            .await
            .map_err(|_| Error::ReadFailed)??;
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
        let corrupt = |detail: &str| Error::Corruption {
            what: "table",
            path: Some(self.path.clone()),
            offset: Some(handle.offset()),
            detail: detail.to_owned(),
        };
        // The handle comes from a verified index, but the file is still untrusted: nothing is
        // allocated for a block that does not fit inside it.
        let size = handle
            .offset()
            .checked_add(handle.size())
            .filter(|end| *end <= self.len)
            .and_then(|_| usize::try_from(handle.size()).ok())
            .ok_or_else(|| corrupt("a block handle points past the file"))?;
        let mut sealed = vec![0; size];
        self.file.read_at(handle.offset(), &mut sealed)?;
        unseal_block(&sealed, compare_internal)
            .map_err(|error| format_error(&error, Some(&self.path)))
    }

    fn key(&self, handle: BlockHandle) -> BlockKey {
        BlockKey {
            file: self.id,
            offset: handle.offset(),
        }
    }
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
    written: Option<&[u8]>,
) -> Result<(Table, Arc<TableBlocks>)> {
    let file = storage.open(path)?;
    let len = file.len()?;
    let blocks = Arc::new(TableBlocks {
        id,
        path: path.to_path_buf(),
        file: Arc::clone(&file),
        len,
        cache: Arc::clone(cache),
    });
    let read = |offset: u64, buf: &mut [u8]| match written {
        Some(bytes) => usize::try_from(offset)
            .ok()
            .and_then(|start| bytes.get(start..start.checked_add(buf.len())?))
            .map(|found| buf.copy_from_slice(found))
            .ok_or_else(|| Error::ShortRead {
                path: path.to_path_buf(),
                requested_end: offset.saturating_add(u64::try_from(buf.len()).unwrap_or(u64::MAX)),
                available: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
            }),
        None => file.read_at(offset, buf),
    };
    let source: Arc<dyn BlockSource> = Arc::<TableBlocks>::clone(&blocks);
    let table = Table::open_with(len, read, source, compare_internal, user_key).map_err(
        |error| match error {
            OpenError::Format(error) => format_error(&error, Some(path)),
            OpenError::Read(error) => error,
        },
    )?;
    Ok((table, blocks))
}
