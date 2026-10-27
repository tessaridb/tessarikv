//! Entries beyond a put, a delete and a range delete: expiring puts, which store their expiry
//! stamp (8 bytes, see [`split_expiring`](tessarikv_types::split_expiring)) at the start of
//! their value, and single deletes.

use std::time::{SystemTime, UNIX_EPOCH};

use tessarikv_constants::MAX_VALUE_BYTES;
use tessarikv_types::{EXPIRY_STAMP_BYTES, KeyspaceId, ValueKind};

use super::{WriteBatch, check_size};
use crate::Result;

impl WriteBatch {
    /// Adds `key ↦ value` in `keyspace` until `expires_at`: from that time on (to the
    /// millisecond, by the wall clock of the reading process) the key reads as absent, the older
    /// versions of it stay hidden as after a delete, and compaction removes it. A time before the
    /// Unix epoch has already passed.
    ///
    /// # Errors
    ///
    /// As for [`WriteBatch::put`]; the value's limit is [`MAX_VALUE_BYTES`] less the 8-byte
    /// stamp stored with it, refused as `TooLarge` of an "expiring value".
    pub fn put_expiring(
        &mut self,
        keyspace: KeyspaceId,
        key: &[u8],
        value: &[u8],
        expires_at: SystemTime,
    ) -> Result<()> {
        check_size(
            "expiring value",
            value.len(),
            MAX_VALUE_BYTES.saturating_sub(EXPIRY_STAMP_BYTES),
        )?;
        let millis = expires_at.duration_since(UNIX_EPOCH).map_or(0, |since| {
            u64::try_from(since.as_millis()).unwrap_or(u64::MAX)
        });
        let stamp = millis.to_be_bytes();
        self.append(
            keyspace,
            ValueKind::ExpiringValue,
            key,
            Some(&stamp),
            Some(value),
        )
    }

    /// Removes `key` from `keyspace`, for a key put exactly once since it was last absent and
    /// never overwritten; it reads as [`WriteBatch::delete`] does.
    ///
    /// The precondition is the caller's to keep: this build stores a single delete as a delete,
    /// so a key that broke it still reads as absent, but a later build may cancel a single
    /// delete against the one put below it during compaction, and a key put twice would then
    /// show its older value again. Use [`WriteBatch::delete`] for a key that may be overwritten.
    ///
    /// # Errors
    ///
    /// As for [`WriteBatch::put`].
    pub fn single_delete(&mut self, keyspace: KeyspaceId, key: &[u8]) -> Result<()> {
        self.append(keyspace, ValueKind::Delete, key, None, None)
    }
}
