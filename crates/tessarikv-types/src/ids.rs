//! Identifiers for keyspaces and files.

/// A keyspace (column family): an independently configured ordered key space inside one database.
///
/// Keyspaces share the database's log, so one write batch may span several of them atomically.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct KeyspaceId(u32);

impl KeyspaceId {
    /// The keyspace every database has from creation.
    pub const DEFAULT: Self = Self(0);

    /// Wraps a raw keyspace id.
    #[must_use]
    pub const fn new(raw: u32) -> Self {
        Self(raw)
    }

    /// The raw id, as stored in a log record or the manifest.
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0
    }
}

/// The number of a file in the database directory: a log segment, a table or a manifest.
///
/// Numbers are allocated once and never reused, so a number names one file for the life of the
/// database.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FileNumber(u64);

impl FileNumber {
    /// Wraps a raw file number.
    #[must_use]
    pub const fn new(raw: u64) -> Self {
        Self(raw)
    }

    /// The raw number.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}
