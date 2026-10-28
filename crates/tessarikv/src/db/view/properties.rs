//! A keyspace's properties: the state of its levels and in-memory tables, read without walking
//! the table files.

use tessarikv_constants::LEVELS;
use tessarikv_types::KeyspaceId;

use crate::Result;
use crate::db::Db;

/// What a keyspace holds right now, from [`Db::properties`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct KeyspaceProperties {
    /// Table files in each level, level 0 first.
    pub files_per_level: [u32; LEVELS],
    /// Bytes of the table files in each level, level 0 first.
    pub bytes_per_level: [u64; LEVELS],
    /// Bytes compaction owes the levels (estimated by the engine after every flush and
    /// compaction; always 0 for a style other than leveled).
    pub pending_compaction_bytes: u64,
    /// An estimate of the bytes of live data: those of the deepest level holding files, where
    /// leveled compaction keeps most of a keyspace's data.
    pub estimated_live_bytes: u64,
    /// Bytes held by the keyspace's in-memory tables, the one taking writes and those waiting
    /// for a flush.
    pub memtable_bytes: u64,
    /// In-memory tables waiting for a flush.
    pub immutable_memtables: u32,
}

impl Db {
    /// The properties of `keyspace`: its levels as the current view holds them, counted when
    /// that view was published, what compaction owes as of the engine's last estimate, and its
    /// in-memory tables now. Costs no I/O and no walk of the table files.
    ///
    /// # Errors
    ///
    /// [`crate::Error::UnknownKeyspace`] for a keyspace the database does not hold.
    pub fn properties(&self, keyspace: KeyspaceId) -> Result<KeyspaceProperties> {
        self.check_keyspace(keyspace)?;
        let view = self.view.load();
        let levels = view.level_figures(keyspace.get());
        let mut memtable_bytes = 0_u64;
        let mut memtables = 0_u32;
        for table in view.memtables(keyspace.get()) {
            memtable_bytes = memtable_bytes.saturating_add(table.approximate_bytes());
            memtables = memtables.saturating_add(1);
        }
        let estimated_live_bytes = levels
            .bytes
            .iter()
            .rev()
            .copied()
            .find(|bytes| *bytes > 0)
            .unwrap_or(0);
        Ok(KeyspaceProperties {
            files_per_level: levels.files,
            bytes_per_level: levels.bytes,
            pending_compaction_bytes: self
                .installer
                .as_ref()
                .map_or(0, |installer| installer.signals.owed(keyspace.get())),
            estimated_live_bytes,
            memtable_bytes,
            immutable_memtables: memtables.saturating_sub(1),
        })
    }
}
