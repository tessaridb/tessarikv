//! Verifying every table file against its checksums, read from storage.

use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::Arc;

use tessarikv_constants::DEFAULT_BLOCK_BYTES;
use tessarikv_types::{InternalKeyRef, ValueKind};
use tokio::task::{JoinError, JoinSet};

use super::Db;
use super::block_cache::BlockCache;
use super::flush_job::table_path;
use super::table_blocks::open_table;
use super::tables::blobs::{BlobFile, locate};
use super::tables::format_error;
use crate::storage::Storage;
use crate::{Error, Result};

impl Db {
    /// Reads every table file of the database from storage and verifies it whole: its footer,
    /// index, filter and properties, its range deletes and every data block, each against the
    /// checksum it was written with (and its authentication tag, when encrypted), and every
    /// value it keeps in a blob file the same way. The block cache
    /// is neither consulted nor filled, so a block whose good copy is cached is still read from
    /// the device. Files are verified concurrently, each on a blocking thread. A file a
    /// compaction removes during the sweep is passed over: its entries are in newer files, which
    /// were verified as they were written. In memory there are no table files and it returns at
    /// once.
    ///
    /// # Errors
    ///
    /// [`Error::Corruption`] naming the first damaged file found, the storage's error for a
    /// failed read, and [`Error::ReadFailed`] when a reading thread stopped without an answer.
    pub async fn verify_checksums(&self) -> Result<()> {
        let (storage, dir, cache) = match (self.read_only_files(), &self.installer) {
            (Some(files), _) => (&files.storage, &files.dir, &files.cache),
            (None, Some(installer)) => (&installer.storage, &installer.dir, &installer.cache),
            (None, None) => return Ok(()),
        };
        let ids: Vec<(u64, Arc<[Arc<BlobFile>]>)> = self
            .view
            .load()
            .all_files()
            .iter()
            .map(|file| (file.meta.id, Arc::clone(&file.blobs)))
            .collect();
        let at_once = std::thread::available_parallelism().map_or(1, NonZeroUsize::get);
        let mut running = JoinSet::new();
        for (id, blobs) in ids {
            if running.len() >= at_once {
                joined(running.join_next().await)?;
            }
            let storage = Arc::clone(storage);
            let path = table_path(dir, id);
            // Holds nothing: every block is read from storage.
            let cache = Arc::new(
                BlockCache::new(0, DEFAULT_BLOCK_BYTES).with_sealing(cache.shared_sealing()),
            );
            running
                .spawn_blocking(move || verify_table(storage.as_ref(), &path, id, &cache, &blobs));
        }
        while let Some(done) = running.join_next().await {
            joined(Some(done))?;
        }
        Ok(())
    }
}

/// The answer of a finished verification; a thread that died gave none.
fn joined(done: Option<std::result::Result<Result<()>, JoinError>>) -> Result<()> {
    match done {
        Some(Ok(verified)) => verified,
        Some(Err(_)) => Err(Error::ReadFailed),
        None => Ok(()),
    }
}

/// Opens table file `id` at `path` through `cache`, which keeps nothing, and reads it whole,
/// with every value it keeps in `blobs`, the blob files it holds. Runs on a blocking thread.
fn verify_table(
    storage: &dyn Storage,
    path: &Path,
    id: u64,
    cache: &Arc<BlockCache>,
    blobs: &[Arc<BlobFile>],
) -> Result<()> {
    let (table, blocks) = match open_table(storage, path, id, cache, None) {
        Ok(opened) => opened,
        // Removed by a compaction since the sweep began.
        Err(Error::NotFound { .. }) => return Ok(()),
        Err(error) => return Err(error),
    };
    table
        .range_deletes()
        .map_err(|error| format_error(&error, Some(path)))?;
    let mut walk = table.iter();
    walk.seek_to_first();
    blocks.complete_blocking(&mut walk)?;
    while walk.valid() {
        let in_blob = InternalKeyRef::parse(walk.key())
            .is_ok_and(|key| matches!(key.trailer().kind(), Ok(ValueKind::BlobIndex)));
        if in_blob {
            let (blob, handle) = locate(blobs, walk.value())?;
            blob.read_blocking(handle)?;
        }
        walk.next();
        blocks.complete_blocking(&mut walk)?;
    }
    walk.status()
        .map_err(|error| format_error(&error, Some(path)))
}
