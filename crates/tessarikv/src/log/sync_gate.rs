//! A gate tests close to hold every sync until they open it, knowing which files wait.

use std::path::{Path, PathBuf};
use std::sync::{Condvar, Mutex};

#[derive(Debug, Default)]
pub(super) struct SyncGate {
    /// While set, syncs wait.
    paused: Mutex<bool>,
    resumed: Condvar,
    /// The files whose syncs are waiting.
    waiting: Mutex<Vec<PathBuf>>,
}

impl SyncGate {
    /// Every later sync waits until [`SyncGate::resume`].
    pub(super) fn pause(&self) {
        *self.paused.lock().expect("the gate") = true;
    }

    /// Lets waiting and later syncs through.
    pub(super) fn resume(&self) {
        *self.paused.lock().expect("the gate") = false;
        self.resumed.notify_all();
    }

    /// Whether a sync of a file whose name starts with `prefix` is waiting.
    pub(super) fn waiting(&self, prefix: &str) -> bool {
        self.waiting.lock().expect("the list").iter().any(|path| {
            path.file_name()
                .is_some_and(|name| name.to_string_lossy().starts_with(prefix))
        })
    }

    /// Returns once the gate is open, listed as waiting on `path` meanwhile.
    pub(super) fn pass(&self, path: &Path) {
        let mut paused = self.paused.lock().expect("the gate");
        if !*paused {
            return;
        }
        self.waiting
            .lock()
            .expect("the list")
            .push(path.to_path_buf());
        while *paused {
            paused = self.resumed.wait(paused).expect("the gate");
        }
        drop(paused);
        let mut waiting = self.waiting.lock().expect("the list");
        if let Some(index) = waiting.iter().position(|waiting| waiting == path) {
            waiting.swap_remove(index);
        }
    }
}
