//! The database: atomic batches in, consistent point reads and ordered scans out.
//!
//! This first form keeps everything in memory, in one table per keyspace, and serves the default
//! keyspace. A write claims one sequence number per entry, applies its batch to the table and
//! completes the claim; a read takes the highest sequence covering only completed batches as its
//! snapshot. Writes from many tasks run concurrently, and reads never wait for them.

mod read;
mod write;

#[cfg(test)]
mod tests;

use tessarikv_constants::COMMIT_RING_SLOTS;
use tessarikv_sync::Sequencer;
use tessarikv_types::{KeyspaceId, SequenceNumber};

use crate::memtable::Memtable;
use crate::{DbOptions, Error, Result, StorageMode};

pub use read::ScanPage;

/// An open database.
///
/// # Example
///
/// ```
/// use tessarikv::{Db, DbOptions, KeyspaceId, ReadOptions, WriteBatch, WriteOptions};
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let db = Db::open(DbOptions::in_memory()).await?;
/// let mut batch = WriteBatch::new();
/// batch.put(KeyspaceId::DEFAULT, b"apple", b"red")?;
/// db.write(&batch, &WriteOptions::default()).await?;
/// let value = db.get(KeyspaceId::DEFAULT, b"apple", &ReadOptions::default()).await?;
/// assert_eq!(value.as_deref(), Some(&b"red"[..]));
/// # Ok(())
/// # }
/// ```
pub struct Db {
    sequencer: Sequencer,
    table: Memtable,
}

impl Db {
    /// Opens a database.
    ///
    /// # Errors
    ///
    /// [`Error::Unsupported`] for [`StorageMode::Os`]: a database on storage needs its log,
    /// which this version does not write yet.
    pub async fn open(options: DbOptions) -> Result<Self> {
        match options.storage() {
            StorageMode::Memory => Ok(Self {
                sequencer: Sequencer::new(0, COMMIT_RING_SLOTS),
                table: Memtable::new(),
            }),
            StorageMode::Os => Err(Error::Unsupported {
                what: "a database on storage",
            }),
        }
    }

    /// Refuses a keyspace this database does not hold.
    fn check_keyspace(keyspace: KeyspaceId) -> Result<()> {
        if keyspace == KeyspaceId::DEFAULT {
            Ok(())
        } else {
            Err(Error::UnknownKeyspace {
                keyspace: keyspace.get(),
            })
        }
    }

    /// The snapshot a read takes: the highest sequence covering only completed batches.
    fn snapshot(&self) -> Result<SequenceNumber> {
        SequenceNumber::new(self.sequencer.visible()).map_err(|_| Error::SequenceExhausted)
    }
}
