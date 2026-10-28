//! How far the log writer task has got, published to the writers waiting on it.

use super::Wait;

/// How far the log has got, in sequence numbers.
#[derive(Debug, Clone, Copy)]
pub(super) struct Progress {
    /// Every sequence up to this one is written or skipped.
    written: u64,
    /// Every frame up to this sequence is on stable storage.
    durable: u64,
    /// The log stopped after a failed write or sync.
    pub(super) failed: bool,
}

impl Progress {
    /// Nothing written yet; the first entry is expected at `next_sequence`.
    pub(super) fn new(next_sequence: u64) -> Self {
        let before = next_sequence.saturating_sub(1);
        Self {
            written: before,
            durable: before,
            failed: false,
        }
    }

    /// Every sequence up to this one is written or skipped.
    pub(super) fn written(&self) -> u64 {
        self.written
    }

    /// Every sequence through `last` is written or skipped.
    pub(super) fn written_through(&mut self, last: u64) {
        self.written = last;
    }

    /// Every frame through `through` is on stable storage; an earlier sync never lowers it.
    pub(super) fn durable_through(&mut self, through: u64) {
        self.durable = self.durable.max(through);
    }

    /// Whether a frame ending at `last` has reached `wait`.
    pub(super) fn covers(&self, last: u64, wait: Wait) -> bool {
        match wait {
            Wait::Written => self.written >= last,
            Wait::Durable => self.durable >= last,
        }
    }
}
