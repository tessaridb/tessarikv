//! Options for one write.

/// What a successful write promises to survive.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Durability {
    /// The log record is on stable storage before the write returns: survives power loss.
    Sync,
    /// The log record is handed to the operating system before the write returns: survives a
    /// process crash, may be lost with the machine until a later synced write or log sync.
    Buffered,
    /// No log record: the write lives in memory until its memtable is flushed and is lost on
    /// any crash before that. For bulk loads that can be redone.
    NoWal,
}

/// How one write behaves.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct WriteOptions {
    /// The durability this write is acknowledged with (default [`Durability::Sync`]).
    pub durability: Durability,
    /// Fail with an `Unavailable` error instead of waiting when the engine is slowing writes
    /// down to let flushes and compactions catch up (default `false`).
    pub no_slowdown: bool,
}

impl Default for WriteOptions {
    fn default() -> Self {
        Self {
            durability: Durability::Sync,
            no_slowdown: false,
        }
    }
}
