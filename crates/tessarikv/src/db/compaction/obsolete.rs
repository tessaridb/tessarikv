//! Files no version names any more, removed once their last reader lets go: a table when no
//! view or job reads it, a blob file when no table holds it.

use std::path::Path;
use std::sync::Arc;

use super::Compactions;
use crate::db::block_cache::BlockCache;
use crate::db::flush_job::table_path;
use crate::db::tables::blobs::blob_path;
use crate::db::view::TableFile;
use crate::storage::Storage;

impl Compactions {
    /// Records `file`, in `dir`, as named by no version: it and the blob files it holds are
    /// removed once nothing reads them.
    pub(super) fn retire_file(&mut self, file: &TableFile, dir: &Path) {
        self.obsolete.push((
            Arc::downgrade(&file.blocks),
            file.meta.id,
            table_path(dir, file.meta.id),
        ));
        for blob in file.blobs.iter() {
            // Several retired tables may hold one blob file; it is removed once.
            if !self
                .obsolete_blobs
                .iter()
                .any(|(_, id, _)| *id == blob.id())
            {
                self.obsolete_blobs.push((
                    Arc::downgrade(blob),
                    blob.id(),
                    blob_path(dir, blob.id()),
                ));
            }
        }
    }

    /// Removes the files nothing reads any more; a removal that fails is tried again at the
    /// next sweep. A blob file goes only once every table holding it is gone: a table still
    /// named, or still read, keeps it.
    pub(super) async fn sweep(&mut self, storage: Arc<dyn Storage>, cache: Arc<BlockCache>) {
        let (unread, read): (Vec<_>, Vec<_>) = std::mem::take(&mut self.obsolete)
            .into_iter()
            .partition(|(blocks, _, _)| blocks.strong_count() == 0);
        self.obsolete = read;
        let (unheld, held): (Vec<_>, Vec<_>) = std::mem::take(&mut self.obsolete_blobs)
            .into_iter()
            .partition(|(blob, _, _)| blob.strong_count() == 0);
        self.obsolete_blobs = held;
        if unread.is_empty() && unheld.is_empty() {
            return;
        }
        let blob_storage = Arc::clone(&storage);
        let failed = tokio::task::spawn_blocking(move || {
            let tables = unread
                .into_iter()
                // A handle still kept would stop the removal where open files cannot be removed.
                .filter(|(_, id, path)| !cache.forget_file(*id) || storage.remove(path).is_err())
                .collect::<Vec<_>>();
            let blobs = unheld
                .into_iter()
                .filter(|(_, _, path)| blob_storage.remove(path).is_err())
                .collect::<Vec<_>>();
            (tables, blobs)
        })
        .await
        .unwrap_or_default();
        self.obsolete.extend(failed.0);
        self.obsolete_blobs.extend(failed.1);
    }
}
