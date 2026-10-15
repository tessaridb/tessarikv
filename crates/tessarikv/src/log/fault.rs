//! A file system for log tests: memory files that remember how much of each was synced, can be
//! told to fail or hold every sync, and can take crash images around every durable step.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};

use crate::storage::{MemoryStorage, ReadableFile, Storage, StorageLock, WritableFile};
use crate::{Error, Result};

#[derive(Debug, Clone, Default)]
pub(crate) struct FaultStorage {
    files: MemoryStorage,
    state: Arc<State>,
}

#[derive(Debug, Default)]
struct State {
    /// Bytes of each file the last successful sync covered.
    synced: papaya::HashMap<PathBuf, u64>,
    fail_sync: AtomicBool,
    /// While set, syncs wait.
    paused: Mutex<bool>,
    resumed: std::sync::Condvar,
    /// When armed, crash images are taken around every durable step outside the log.
    capture: OnceLock<Capture>,
}

#[derive(Debug)]
struct Capture {
    dir: PathBuf,
    acknowledged: Arc<AtomicU64>,
    images: Mutex<Vec<CrashImage>>,
}

/// The files as a crash at one durable step would leave them.
#[derive(Debug)]
pub(crate) struct CrashImage {
    /// Writes acknowledged before the image was taken.
    pub(crate) acknowledged: u64,
    /// The step, for failure messages.
    pub(crate) step: String,
    pub(crate) storage: FaultStorage,
}

impl FaultStorage {
    /// Every later sync fails.
    pub(crate) fn fail_syncs(&self) {
        // SeqCst: a test flag, read by the log writer on another thread.
        self.state.fail_sync.store(true, Ordering::SeqCst);
    }

    /// Every later sync waits until [`FaultStorage::resume_syncs`].
    pub(crate) fn pause_syncs(&self) {
        *self.state.paused.lock().expect("the gate") = true;
    }

    /// Lets waiting and later syncs through.
    pub(crate) fn resume_syncs(&self) {
        *self.state.paused.lock().expect("the gate") = false;
        self.state.resumed.notify_all();
    }

    /// From now on, takes crash images of `dir` around every sync of a file that is not a log
    /// segment and after every removal, each tagged with `acknowledged` as read before the copy.
    pub(crate) fn capture_steps(&self, dir: &Path, acknowledged: Arc<AtomicU64>) {
        let capture = Capture {
            dir: dir.to_path_buf(),
            acknowledged,
            images: Mutex::new(Vec::new()),
        };
        assert!(self.state.capture.set(capture).is_ok(), "armed once");
    }

    /// The crash images taken so far.
    pub(crate) fn take_images(&self) -> Vec<CrashImage> {
        self.state.capture.get().map_or_else(Vec::new, |capture| {
            std::mem::take(&mut *capture.images.lock().expect("the images"))
        })
    }

    /// A power-loss and a process-crash image, when capture is armed and `path` is not a log
    /// segment.
    fn capture(&self, path: &Path, step: &str) {
        let Some(capture) = self.state.capture.get() else {
            return;
        };
        if path.extension().is_some_and(|extension| extension == "log") && step != "removed" {
            return;
        }
        // SeqCst: read before the copy, so every write it counts is in the image.
        let acknowledged = capture.acknowledged.load(Ordering::SeqCst);
        let name = path
            .file_name()
            .map(|name| name.to_string_lossy().into_owned());
        for power_loss in [true, false] {
            let image = CrashImage {
                acknowledged,
                step: format!("{step} {name:?}, power loss {power_loss}"),
                storage: self.crash(&capture.dir, power_loss),
            };
            capture.images.lock().expect("the images").push(image);
        }
    }

    /// Everything appended to `path`.
    pub(crate) fn bytes(&self, path: &Path) -> Vec<u8> {
        contents(&*self.files.open(path).expect("the file exists"))
    }

    /// The files of `dir` as a crash would leave them: everything appended after a process
    /// crash, only what was synced after a power loss.
    pub(crate) fn crash(&self, dir: &Path, power_loss: bool) -> Self {
        let image = Self::default();
        image.files.create_dir_all(dir).expect("directory");
        for name in self.files.list(dir).expect("listable") {
            let path = dir.join(name);
            // A file removed since the listing is gone from the image too.
            let Ok(file) = self.files.open(&path) else {
                continue;
            };
            let synced = self.state.synced.pin().get(&path).copied().unwrap_or(0);
            let mut bytes = contents(&*file);
            if power_loss {
                bytes.truncate(usize::try_from(synced).expect("fits in memory"));
            }
            let mut file = image.files.create(&path).expect("created");
            file.append(&bytes).expect("appended");
            image.state.synced.pin().insert(path, file.len());
        }
        image
    }

    /// Appends `bytes` to `path` as a torn write would leave them on the device: present after
    /// any later crash, power loss included.
    pub(crate) fn append_raw(&self, path: &Path, bytes: &[u8]) {
        let mut all = self.bytes(path);
        all.extend_from_slice(bytes);
        let mut file = self.files.create(path).expect("replaced");
        file.append(&all).expect("appended");
        self.state
            .synced
            .pin()
            .insert(path.to_path_buf(), file.len());
    }

    /// The prefix of `path` a power loss would leave: what the last successful sync covered.
    pub(crate) fn synced_bytes(&self, path: &Path) -> Vec<u8> {
        let synced = self.state.synced.pin().get(path).copied().unwrap_or(0);
        let mut bytes = self.bytes(path);
        bytes.truncate(usize::try_from(synced).expect("fits in memory"));
        bytes
    }
}

/// Everything in `file`.
fn contents(file: &dyn ReadableFile) -> Vec<u8> {
    let len = usize::try_from(file.len().expect("length is known")).expect("fits in memory");
    let mut bytes = vec![0; len];
    file.read_at(0, &mut bytes).expect("readable");
    bytes
}

impl Storage for FaultStorage {
    fn create(&self, path: &Path) -> Result<Box<dyn WritableFile>> {
        Ok(Box::new(FaultFile {
            inner: self.files.create(path)?,
            path: path.to_path_buf(),
            storage: self.clone(),
        }))
    }

    fn open(&self, path: &Path) -> Result<Arc<dyn ReadableFile>> {
        self.files.open(path)
    }

    fn exists(&self, path: &Path) -> Result<bool> {
        self.files.exists(path)
    }

    fn remove(&self, path: &Path) -> Result<()> {
        self.files.remove(path)?;
        self.capture(path, "removed");
        Ok(())
    }

    fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        self.files.rename(from, to)?;
        // What was synced stays synced: the bytes follow the file to its new name. (Whether the
        // new NAME survives a power loss before the directory is synced is not modelled.)
        let synced = self.state.synced.pin();
        let len = synced.get(from).copied();
        synced.remove(from);
        match len {
            Some(len) => synced.insert(to.to_path_buf(), len),
            None => synced.remove(to),
        };
        Ok(())
    }

    fn list(&self, dir: &Path) -> Result<Vec<String>> {
        self.files.list(dir)
    }

    fn create_dir_all(&self, dir: &Path) -> Result<()> {
        self.files.create_dir_all(dir)
    }

    fn sync_dir(&self, dir: &Path) -> Result<()> {
        self.files.sync_dir(dir)
    }

    fn lock(&self, path: &Path) -> Result<Box<dyn StorageLock>> {
        self.files.lock(path)
    }
}

struct FaultFile {
    inner: Box<dyn WritableFile>,
    path: PathBuf,
    storage: FaultStorage,
}

impl WritableFile for FaultFile {
    fn append(&mut self, data: &[u8]) -> Result<()> {
        self.inner.append(data)
    }

    fn sync(&mut self) -> Result<()> {
        let state = &self.storage.state;
        let mut paused = state.paused.lock().expect("the gate");
        while *paused {
            paused = state.resumed.wait(paused).expect("the gate");
        }
        drop(paused);
        if state.fail_sync.load(Ordering::SeqCst) {
            return Err(Error::Io {
                operation: "sync",
                path: self.path.clone(),
                source: std::io::Error::other("injected sync failure"),
            });
        }
        self.storage.capture(&self.path, "before syncing");
        self.inner.sync()?;
        state
            .synced
            .pin()
            .insert(self.path.clone(), self.inner.len());
        self.storage.capture(&self.path, "after syncing");
        Ok(())
    }

    fn len(&self) -> u64 {
        self.inner.len()
    }
}
