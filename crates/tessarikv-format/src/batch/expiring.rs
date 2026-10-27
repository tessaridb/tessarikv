//! Expiring puts: an expiring put stores its expiry stamp (8 bytes, see
//! [`split_expiring`](tessarikv_types::split_expiring)) at the start of its value.

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
}
