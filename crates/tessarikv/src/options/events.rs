//! The embedder's view of what the engine does in the background: flushes and compactions
//! finishing, writes being paced or held, and the database stopping on a failure.

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tessarikv_types::KeyspaceId;

use super::db::{DbOptions, DbOptionsBuilder};
use crate::Error;

/// Receives the engine's background events. Every method has an empty default, so a listener
/// implements only those it needs.
///
/// The methods are called on the engine's background task as the events happen, one at a time
/// and in order. They must return quickly and must not block or wait on the database: a
/// listener with work to do hands it to a channel and returns.
pub trait EventListener: Send + Sync {
    /// A name for the options record.
    fn name(&self) -> &str;

    /// A flush's tables were installed: their writes are on table files.
    fn flush_completed(&self, _event: &FlushCompleted) {}

    /// A compaction's output was installed in place of its inputs.
    fn compaction_completed(&self, _event: &CompactionCompleted) {}

    /// What the levels ask of writers changed.
    fn stall_changed(&self, _condition: StallCondition) {}

    /// The database stopped taking writes after a failure; [`crate::Db::background_error`]
    /// reports it from now on. Called once.
    fn background_error(&self, _error: &Error) {}
}

/// A flush that was installed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct FlushCompleted {
    /// Table files written, over every keyspace of the flush.
    pub tables: u32,
    /// Their bytes.
    pub bytes: u64,
    /// Every write up to this sequence number is now on table files.
    pub flushed_sequence: u64,
}

/// A compaction that was installed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct CompactionCompleted {
    /// The keyspace compacted.
    pub keyspace: KeyspaceId,
    /// The level its output went to.
    pub output_level: u8,
    /// Table files it read.
    pub input_tables: u32,
    /// Table files it wrote; a table moved down a level without being rewritten counts too.
    pub output_tables: u32,
    /// Bytes of the files it read.
    pub input_bytes: u64,
    /// Bytes of the files it wrote.
    pub output_bytes: u64,
}

/// What the levels ask of writers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum StallCondition {
    /// Writes go freely.
    Normal,
    /// Writes are paced; `trigger` says why.
    Delayed {
        /// The trigger that holds.
        trigger: &'static str,
    },
    /// Writes wait for a compaction; `trigger` says why.
    Stopped {
        /// The trigger that holds.
        trigger: &'static str,
    },
}

/// A shared listener in the options: equal when it is the same listener, shown by name.
#[derive(Clone)]
pub(crate) struct ListenerHandle(pub(crate) Arc<dyn EventListener>);

impl PartialEq for ListenerHandle {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for ListenerHandle {}

impl fmt::Debug for ListenerHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0.name())
    }
}

impl DbOptions {
    /// The listener the engine reports its background events to (default none).
    #[must_use]
    pub fn event_listener(&self) -> Option<&Arc<dyn EventListener>> {
        self.event_listener.as_ref().map(|handle| &handle.0)
    }
}

impl DbOptionsBuilder {
    /// Sets [`DbOptions::event_listener`].
    pub fn event_listener(mut self, listener: Arc<dyn EventListener>) -> Self {
        self.options.event_listener = Some(ListenerHandle(listener));
        self
    }
}

/// The listener as the engine calls it: the failure that stops the database is reported once,
/// whichever part of the engine fails first.
pub(crate) struct Reporter {
    listener: Arc<dyn EventListener>,
    failed: AtomicBool,
}

impl Reporter {
    /// The reporter for `options`' listener, when they name one.
    pub(crate) fn of(options: &DbOptions) -> Option<Arc<Self>> {
        options.event_listener().map(|listener| {
            Arc::new(Self {
                listener: Arc::clone(listener),
                failed: AtomicBool::new(false),
            })
        })
    }

    /// The listener, for the events reported as they happen.
    pub(crate) fn listener(&self) -> &dyn EventListener {
        self.listener.as_ref()
    }

    /// Reports `error` unless a failure was reported already.
    pub(crate) fn background_error(&self, error: &Error) {
        // Relaxed: the swap's atomicity alone makes one caller the first; nothing else is
        // published through the flag.
        if !self.failed.swap(true, Ordering::Relaxed) {
            self.listener.background_error(error);
        }
    }
}
