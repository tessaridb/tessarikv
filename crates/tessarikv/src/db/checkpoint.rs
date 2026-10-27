//! A checkpoint: a consistent copy of a live database in another directory.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::sync::oneshot;

use super::Db;
use super::catalog::{Command, Pinned};
use super::flush_job::table_path;
use super::key_check::KEY_CHECK;
use super::open::IDENTITY;
use super::tables::blobs::blob_path;
use crate::sealing::Sealing;
use crate::storage::Storage;
use crate::version::manifest::ManifestStore;
use crate::{Error, Result};

/// Bytes copied at a time.
const COPY_CHUNK: usize = 1024 * 1024;

impl Db {
    /// Writes a copy of the database into `dir` on the same storage, holding every batch
    /// committed before the call; the copy opens as a database of its own, with the same options
    /// (and the same key provider, when encrypted). The batches still in memory are flushed
    /// first, then the table files of that moment and the blob files they hold are copied with a
    /// manifest naming them, so the copy needs no log; where the storage can, a file is linked
    /// under a second name instead of copied (neither is ever written again). Reads and writes go
    /// on meanwhile; writes after the call are not in the copy. The files copied are kept from
    /// removal until the copy is done.
    ///
    /// # Errors
    ///
    /// [`Error::Unsupported`] for a database in memory, [`Error::AlreadyExists`] when `dir`
    /// holds anything, [`Error::FlushFailed`] or [`Error::CompactionFailed`] when the background
    /// work stopped, and the storage's error for a failed read or write.
    pub async fn checkpoint(&self, dir: impl AsRef<Path>) -> Result<()> {
        let Some(installer) = &self.installer else {
            return Err(Error::Unsupported {
                what: "a checkpoint of a database in memory",
            });
        };
        self.flush().await?;
        let (reply, pinned) = oneshot::channel();
        installer
            .requests
            .send(Command::Checkpoint(reply))
            .await
            .map_err(|_| Error::Closed)?;
        let pinned = pinned.await.map_err(|_| Error::Closed)?;
        let copy = Copy {
            storage: Arc::clone(&installer.storage),
            from: installer.dir.clone(),
            to: dir.as_ref().to_path_buf(),
            sealing: installer.cache.shared_sealing(),
        };
        tokio::task::spawn_blocking(move || copy.run(&pinned))
            .await
            .map_err(|_| Error::Unsupported {
                what: "a checkpoint after its copying thread panicked",
            })?
    }
}

/// Where a checkpoint copies from and to.
struct Copy {
    storage: Arc<dyn Storage>,
    from: PathBuf,
    to: PathBuf,
    sealing: Option<Arc<Sealing>>,
}

impl Copy {
    /// Copies the table files `pinned` names and their blob files, the key check when there is one, and the
    /// database's identity, then writes a manifest of the version and makes it current. Runs on a
    /// blocking thread; `pinned` keeps the files from removal until it returns.
    fn run(&self, pinned: &Pinned) -> Result<()> {
        let (version, view) = pinned;
        if self
            .storage
            .list(&self.to)
            .is_ok_and(|names| !names.is_empty())
        {
            return Err(Error::AlreadyExists {
                path: self.to.clone(),
            });
        }
        self.storage.create_dir_all(&self.to)?;
        for file in view.all_files() {
            let id = file.meta.id;
            let (from, to) = (table_path(&self.from, id), table_path(&self.to, id));
            // A table file is never written again, so a second name serves as well as a copy.
            if !self.storage.link(&from, &to) {
                self.copy(&from, &to)?;
            }
        }
        let mut blobs: Vec<u64> = view
            .all_files()
            .iter()
            .flat_map(|file| file.blobs.iter().map(|blob| blob.id()))
            .collect();
        blobs.sort_unstable();
        blobs.dedup();
        for id in blobs {
            let (from, to) = (blob_path(&self.from, id), blob_path(&self.to, id));
            if !self.storage.link(&from, &to) {
                self.copy(&from, &to)?;
            }
        }
        let key_check = self.from.join(KEY_CHECK);
        if self.storage.exists(&key_check)? {
            self.copy(&key_check, &self.to.join(KEY_CHECK))?;
        }
        let mut identity = self.storage.create(&self.to.join(IDENTITY))?;
        identity.sync()?;
        drop(identity);
        // Synced, made current, and the directory synced: the copy is whole from here on.
        ManifestStore::create(
            Arc::clone(&self.storage),
            &self.to,
            version,
            1,
            self.sealing.as_deref(),
        )?;
        Ok(())
    }

    /// Copies the file at `from` to a new file at `to`, synced.
    fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        let source = self.storage.open(from)?;
        let len = source.len()?;
        let mut target = self.storage.create(to)?;
        let mut buffer = Vec::new();
        let mut offset = 0;
        while offset < len {
            let left = usize::try_from(len.saturating_sub(offset)).unwrap_or(usize::MAX);
            buffer.resize(left.min(COPY_CHUNK), 0);
            source.read_at(offset, &mut buffer)?;
            target.append(&buffer)?;
            offset = offset.saturating_add(u64::try_from(buffer.len()).unwrap_or(u64::MAX));
        }
        target.sync()
    }
}
