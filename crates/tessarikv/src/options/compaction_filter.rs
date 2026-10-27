//! A keyspace's compaction filter: the embedder's per-entry say over which values compaction
//! keeps.

use std::fmt;
use std::sync::Arc;

/// What compaction does with a value its filter was shown.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilterDecision {
    /// Write the value out unchanged.
    Keep,
    /// Delete the key at the value's version: from then on it reads as absent, and the older
    /// versions of it stay hidden, as after a delete written at that version.
    Remove,
}

/// Decides, during compaction, whether a keyspace keeps a value.
///
/// Compaction shows the filter the newest version of a key it writes out when that version is
/// a value (a put, or an expiring put that has not expired) newer than every snapshot held when
/// the compaction began, at or below the visible sequence. Deletes, versions a held snapshot
/// can read, and versions written while the compaction runs are never shown, so a snapshot held
/// across a compaction keeps its answer; a snapshot taken while a compaction runs may see a
/// removed value disappear once that compaction is installed.
///
/// The filter runs on compaction threads, possibly several at once, so it is `Send + Sync`.
/// Its decision must depend only on the key and the value — a key is shown again by later
/// compactions, a version may never be shown at all (one still in memory, or in a level no
/// compaction selects), and the database must not be called from inside it. Filtering is
/// therefore eventual: a rule a read must never break belongs in the reader as well.
pub trait CompactionFilter: Send + Sync {
    /// A name for the options record.
    fn name(&self) -> &str;

    /// What compaction does with `value`, the value of `key`.
    fn filter(&self, key: &[u8], value: &[u8]) -> FilterDecision;
}

/// A shared filter in a keyspace's options: equal when it is the same filter, shown by name.
#[derive(Clone)]
pub(crate) struct FilterHandle(pub(crate) Arc<dyn CompactionFilter>);

impl PartialEq for FilterHandle {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for FilterHandle {}

impl fmt::Debug for FilterHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0.name())
    }
}
