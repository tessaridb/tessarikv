//! Writing the blob file of one table: created at its first large value, synced with the table.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use tessarikv_format::{BlobBuilder, BlobIndex, Compression, FileKind};

use super::{BlobFile, blob_path};
use crate::db::block_cache::BlockCache;
use crate::db::flush_job::FileNumbers;
use crate::db::rate_limiter::RateLimiter;
use crate::storage::{Storage, WritableFile};
use crate::{Error, Result};

/// The blob file a table being written moves its large values into.
pub(crate) struct BlobWriter {
    storage: Arc<dyn Storage>,
    dir: PathBuf,
    numbers: Arc<FileNumbers>,
    cache: Arc<BlockCache>,
    /// Size from which a value moves out; `None` keeps every value in the table.
    threshold: Option<u64>,
    compression: Compression,
    /// Paces the file's writes, when the writing job is paced.
    limiter: Option<Arc<RateLimiter>>,
    open: Option<Open>,
}

/// The blob file being written.
struct Open {
    builder: BlobBuilder,
    file: Box<dyn WritableFile>,
}

impl BlobWriter {
    /// A writer of blob file in `dir` for values of at least `threshold` bytes, stored with
    /// `compression`, its number taken from `numbers` when its first value comes.
    pub(crate) fn new(
        storage: Arc<dyn Storage>,
        dir: &Path,
        numbers: Arc<FileNumbers>,
        cache: Arc<BlockCache>,
        threshold: Option<u64>,
        compression: Compression,
    ) -> Self {
        Self {
            storage,
            dir: dir.to_path_buf(),
            numbers,
            cache,
            threshold,
            compression,
            limiter: None,
            open: None,
        }
    }

    /// The writer pacing its writes through `limiter`.
    pub(crate) fn paced(mut self, limiter: Option<Arc<RateLimiter>>) -> Self {
        self.limiter = limiter;
        self
    }

    /// Whether a value of `len` bytes moves out of the table.
    pub(crate) fn separates(&self, len: usize) -> bool {
        self.threshold
            .is_some_and(|threshold| u64::try_from(len).is_ok_and(|len| len >= threshold))
    }

    /// Writes `value` to the blob file, creating it first when this is its first value, and
    /// returns where it is.
    ///
    /// # Errors
    ///
    /// The storage's error when creating or appending fails, and [`Error::Corruption`] when the
    /// format refuses the value.
    pub(crate) fn add(&mut self, value: &[u8]) -> Result<BlobIndex> {
        let open = match self.open.take() {
            Some(open) => open,
            None => self.create()?,
        };
        let open = self.open.insert(open);
        let index = open.builder.add(value).map_err(|error| Error::Corruption {
            what: "blob file",
            path: None,
            offset: None,
            detail: error.to_string(),
        })?;
        let piece = open.builder.take_finished();
        if let Some(limiter) = &self.limiter {
            limiter.pace(u64::try_from(piece.len()).unwrap_or(u64::MAX));
        }
        open.file.append(&piece)?;
        Ok(index)
    }

    /// The number of the blob file, once its first value made it.
    pub(crate) fn id(&self) -> Option<u64> {
        self.open.as_ref().map(|open| open.builder.file())
    }

    /// Syncs the blob file and opens it for reading; `None` when no value moved out.
    ///
    /// # Errors
    ///
    /// The storage's error when the sync or the open fails.
    pub(crate) fn finish(self) -> Result<Option<Arc<BlobFile>>> {
        let Some(Open { builder, mut file }) = self.open else {
            return Ok(None);
        };
        let id = builder.file();
        file.append(&builder.finish())?;
        file.sync()?;
        drop(file);
        BlobFile::open(self.storage.as_ref(), &self.dir, id, &self.cache).map(Some)
    }

    /// Removes the blob file, written for a table no manifest edit will name.
    pub(crate) fn discard(self) {
        if let Some(id) = self.id() {
            drop(self.open);
            let _ = self.storage.remove(&blob_path(&self.dir, id));
        }
    }

    fn create(&self) -> Result<Open> {
        let id = self.numbers.take()?;
        let file = self.storage.create(&blob_path(&self.dir, id))?;
        let builder = match self.cache.sealing() {
            Some(sealing) => {
                let (header, sealer) = sealing.new_file(FileKind::Blob)?;
                BlobBuilder::sealed(id, self.compression, sealer, &header)
            }
            None => BlobBuilder::new(id, self.compression),
        };
        Ok(Open { builder, file })
    }
}
