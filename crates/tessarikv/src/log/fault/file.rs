//! The writable files of [`super::FaultStorage`] and the syncers they hand out.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use super::FaultStorage;
use crate::storage::{FileSyncer, Storage, WritableFile};
use crate::{Error, Result};

pub(super) struct FaultFile {
    pub(super) inner: Box<dyn WritableFile>,
    pub(super) path: PathBuf,
    pub(super) storage: FaultStorage,
}

impl WritableFile for FaultFile {
    fn append(&mut self, data: &[u8]) -> Result<()> {
        let state = &self.storage.state;
        if state.fail_table_writes.load(Ordering::SeqCst)
            && self
                .path
                .extension()
                .is_some_and(|extension| extension == "sst")
        {
            return Err(Error::Io {
                operation: "append",
                path: self.path.clone(),
                source: std::io::Error::other("injected write failure"),
            });
        }
        let _change = self.storage.change();
        self.inner.append(data)
    }

    fn sync(&mut self) -> Result<()> {
        let state = &self.storage.state;
        state.gate.pass(&self.path);
        if state.fail_sync.load(Ordering::SeqCst) {
            return Err(Error::Io {
                operation: "sync",
                path: self.path.clone(),
                source: std::io::Error::other("injected sync failure"),
            });
        }
        self.storage.capture(&self.path, "before syncing");
        {
            let _change = self.storage.change();
            self.inner.sync()?;
            state
                .synced
                .pin()
                .insert(self.path.clone(), self.inner.len());
        }
        self.storage.capture(&self.path, "after syncing");
        Ok(())
    }

    fn len(&self) -> u64 {
        self.inner.len()
    }

    fn syncer(&self) -> Option<Arc<dyn FileSyncer>> {
        Some(Arc::new(FaultSyncer {
            path: self.path.clone(),
            storage: self.storage.clone(),
        }))
    }
}

/// Syncs a file from another thread, through the same gate and failures as its writer's syncs.
/// It covers what was appended before the call: what a real sync is sure to cover.
struct FaultSyncer {
    path: PathBuf,
    storage: FaultStorage,
}

impl FileSyncer for FaultSyncer {
    fn sync(&self) -> Result<()> {
        let state = &self.storage.state;
        let covered = self.storage.files.open(&self.path)?.len()?;
        state.gate.pass(&self.path);
        if state.fail_sync.load(Ordering::SeqCst) {
            return Err(Error::Io {
                operation: "sync",
                path: self.path.clone(),
                source: std::io::Error::other("injected sync failure"),
            });
        }
        self.storage.capture(&self.path, "before syncing");
        {
            let _change = self.storage.change();
            let synced = state.synced.pin();
            let before = synced.get(&self.path).copied().unwrap_or(0);
            synced.insert(self.path.clone(), before.max(covered));
        }
        self.storage.capture(&self.path, "after syncing");
        Ok(())
    }
}
