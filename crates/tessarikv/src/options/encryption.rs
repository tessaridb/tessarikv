//! Encryption at rest: where a database's keys come from and which one new files use.

use std::fmt;
use std::sync::Arc;

use tessarikv_format::RootKey;

use super::db::{DbOptions, DbOptionsBuilder};

/// Hands out root keys by id. The database stores no key: each encrypted file names the id of
/// the root key its own key is derived from, and the database asks the provider for that root
/// key when it opens the file.
///
/// The database calls it on a blocking thread, when it opens and when it opens a file, so a
/// provider may read a key store; it should answer from memory where it can, since every table
/// file opened waits for it.
pub trait KeyProvider: Send + Sync {
    /// The root key with id `key_id`, or `None` when this provider has none.
    fn root_key(&self, key_id: u32) -> Option<RootKey>;
}

/// Encryption at rest: a [`KeyProvider`] and the id of the root key new files use. Files keep
/// the id they were written with, so a key stays needed until no file names it.
#[derive(Clone)]
pub struct Encryption {
    provider: Arc<dyn KeyProvider>,
    key_id: u32,
}

impl Encryption {
    /// The id of the root key new files are encrypted under.
    #[must_use]
    pub const fn key_id(&self) -> u32 {
        self.key_id
    }

    /// Where root keys come from.
    pub(crate) fn provider(&self) -> &dyn KeyProvider {
        self.provider.as_ref()
    }
}

/// Names the key id and never a key.
impl fmt::Debug for Encryption {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Encryption")
            .field("key_id", &self.key_id)
            .finish_non_exhaustive()
    }
}

/// Equal when both name the same provider and key id.
impl PartialEq for Encryption {
    fn eq(&self, other: &Self) -> bool {
        self.key_id == other.key_id && Arc::ptr_eq(&self.provider, &other.provider)
    }
}

impl Eq for Encryption {}

impl DbOptions {
    /// Encryption at rest; `None` (the default) writes files in the clear. Table files and log
    /// segments are then encrypted and authenticated, each table file and each open's log under
    /// a key of its own derived from the provider's root key, and a database encrypted once opens
    /// only with its key. File names, sizes, the sizes of the blocks and log frames in them, and
    /// the sequence number of each logged batch stay visible.
    #[must_use]
    pub const fn encryption(&self) -> Option<&Encryption> {
        self.encryption.as_ref()
    }
}

impl DbOptionsBuilder {
    /// Sets [`DbOptions::encryption`]: root keys come from `provider`, and new files use the one
    /// with id `key_id`.
    pub fn encryption(mut self, provider: Arc<dyn KeyProvider>, key_id: u32) -> Self {
        self.options.encryption = Some(Encryption { provider, key_id });
        self
    }
}
