//! A gate tests close to hold every sync until they open it, knowing which files wait.

use std::path::{Path, PathBuf};
use std::sync::{Condvar, Mutex};

#[derive(Debug, Default)]
pub(super) struct SyncGate {
    /// While set, syncs wait.
    paused: Mutex<bool>,
    resumed: Condvar,
    /// The syncs waiting, by ticket, oldest first, and the tickets let through while paused.
    waiting: Mutex<Waiting>,
}

#[derive(Debug, Default)]
struct Waiting {
    next_ticket: u64,
    files: Vec<(u64, PathBuf)>,
    let_through: Vec<u64>,
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

    /// Lets the sync that started waiting last through, the others still waiting.
    pub(super) fn let_newest_through(&self) {
        let _paused = self.paused.lock().expect("the gate");
        let mut waiting = self.waiting.lock().expect("the list");
        if let Some(&(ticket, _)) = waiting.files.last() {
            waiting.let_through.push(ticket);
        }
        self.resumed.notify_all();
    }

    /// How many syncs of files whose names start with `prefix` are waiting.
    pub(super) fn waiting(&self, prefix: &str) -> usize {
        self.waiting
            .lock()
            .expect("the list")
            .files
            .iter()
            .filter(|(_, path)| {
                path.file_name()
                    .is_some_and(|name| name.to_string_lossy().starts_with(prefix))
            })
            .count()
    }

    /// Returns once the gate is open, or this sync is let through, listed as waiting on `path`
    /// meanwhile.
    pub(super) fn pass(&self, path: &Path) {
        let mut paused = self.paused.lock().expect("the gate");
        if !*paused {
            return;
        }
        let ticket = {
            let mut waiting = self.waiting.lock().expect("the list");
            let ticket = waiting.next_ticket;
            waiting.next_ticket = ticket.wrapping_add(1);
            waiting.files.push((ticket, path.to_path_buf()));
            ticket
        };
        while *paused
            && !self
                .waiting
                .lock()
                .expect("the list")
                .let_through
                .contains(&ticket)
        {
            paused = self.resumed.wait(paused).expect("the gate");
        }
        drop(paused);
        let mut waiting = self.waiting.lock().expect("the list");
        waiting.files.retain(|(waiting, _)| *waiting != ticket);
        waiting.let_through.retain(|waiting| *waiting != ticket);
    }
}
