//! Many point reads at one snapshot.

use tessarikv_types::{KeyspaceId, SequenceNumber};

use super::Db;
use super::read::get_in;
use super::view::View;
use crate::{ReadOptions, Result};

impl Db {
    /// The values of `keys` in `keyspace`, in the order given (`None` for an absent key), all
    /// read at ONE snapshot: no batch is seen for some keys and not others, which separate
    /// [`Db::get`] calls do not promise.
    ///
    /// # Errors
    ///
    /// As [`Db::get`].
    pub async fn multi_get<K: AsRef<[u8]>>(
        &self,
        keyspace: KeyspaceId,
        keys: impl IntoIterator<Item = K>,
        options: &ReadOptions,
    ) -> Result<Vec<Option<Vec<u8>>>> {
        Self::check_keyspace(keyspace)?;
        // The view before the sequence, as in `Db::get`: one of each for every key.
        let view = self.view.load();
        let snapshot = self.visible_sequence()?;
        read_all(&view, keyspace, keys, snapshot, options).await
    }

    /// The values of `keys` in `keyspace` at `snapshot`, as [`Db::multi_get`] returns them.
    pub(super) async fn multi_get_at<K: AsRef<[u8]>>(
        &self,
        keyspace: KeyspaceId,
        keys: impl IntoIterator<Item = K>,
        snapshot: SequenceNumber,
        options: &ReadOptions,
    ) -> Result<Vec<Option<Vec<u8>>>> {
        Self::check_keyspace(keyspace)?;
        let view = self.view.load();
        read_all(&view, keyspace, keys, snapshot, options).await
    }
}

/// Reads every key of `keys` from `view` at `snapshot`, in order.
async fn read_all<K: AsRef<[u8]>>(
    view: &View,
    keyspace: KeyspaceId,
    keys: impl IntoIterator<Item = K>,
    snapshot: SequenceNumber,
    options: &ReadOptions,
) -> Result<Vec<Option<Vec<u8>>>> {
    let keys = keys.into_iter();
    let mut values = Vec::with_capacity(keys.size_hint().0);
    for key in keys {
        values.push(get_in(view, keyspace, key.as_ref(), snapshot, options.fill_cache).await?);
    }
    Ok(values)
}
