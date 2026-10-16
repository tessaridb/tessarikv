//! How many bytes of table files a key range takes, from table metadata and indexes.

use tessarikv_types::{KeyspaceId, SequenceNumber, encode_internal, seek_trailer};

use super::Db;
use super::tables::{format_error, user_key};
use crate::Result;

impl Db {
    /// About how many bytes of table files the user keys `[start, end)` of `keyspace` take
    /// (unbounded above when `end` is `None`): a table inside the range counts whole, one it cuts
    /// counts the data between the two keys as its index places them, to within a block. Nothing
    /// is read from storage. Writes still in memory are not counted; [`Db::flush`] first to count
    /// them. In memory there are no table files and the size is zero.
    ///
    /// # Errors
    ///
    /// [`crate::Error::UnknownKeyspace`] for a keyspace this database does not hold, and
    /// [`crate::Error::Corruption`] for a damaged table index.
    pub fn approximate_size(
        &self,
        keyspace: KeyspaceId,
        start: &[u8],
        end: Option<&[u8]>,
    ) -> Result<u64> {
        Self::check_keyspace(keyspace)?;
        let at = |key: &[u8]| {
            let mut internal = Vec::with_capacity(key.len().saturating_add(8));
            encode_internal(key, seek_trailer(SequenceNumber::MAX), &mut internal);
            internal
        };
        let (low, high) = (at(start), end.map(at));
        let view = self.view.load();
        let mut total: u64 = 0;
        for file in view.files(keyspace.get()) {
            let (smallest, largest) = (user_key(&file.meta.smallest), user_key(&file.meta.largest));
            if largest < start || end.is_some_and(|end| end <= smallest) {
                continue;
            }
            let bytes = if start <= smallest && end.is_none_or(|end| largest < end) {
                file.meta.size
            } else {
                let offset = |key: &[u8]| {
                    file.table
                        .offset_of(key)
                        .map_err(|error| format_error(&error, None))
                };
                let to = high.as_deref().map_or(Ok(file.meta.size), offset)?;
                to.saturating_sub(offset(&low)?)
            };
            total = total.saturating_add(bytes);
        }
        Ok(total)
    }
}
