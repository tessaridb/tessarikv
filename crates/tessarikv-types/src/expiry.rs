//! The expiry stamp an expiring put stores in front of its value.

/// Bytes of the stamp at the start of an expiring put's stored value: the expiry time in
/// milliseconds since the Unix epoch, big-endian.
pub const EXPIRY_STAMP_BYTES: usize = 8;

/// Splits an expiring put's stored value into its expiry time, in milliseconds since the Unix
/// epoch, and the value the caller wrote; `None` when the stored value is shorter than a stamp.
#[must_use]
pub fn split_expiring(stored: &[u8]) -> Option<(u64, &[u8])> {
    let (stamp, value) = stored.split_first_chunk::<EXPIRY_STAMP_BYTES>()?;
    Some((u64::from_be_bytes(*stamp), value))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_stamp_splits_from_its_value_and_a_short_one_is_refused() {
        let mut stored = 1_234_u64.to_be_bytes().to_vec();
        stored.extend_from_slice(b"value");
        assert_eq!(split_expiring(&stored), Some((1_234, &b"value"[..])));
        assert_eq!(split_expiring(&stored[..8]), Some((1_234, &b""[..])));
        assert_eq!(split_expiring(&stored[..7]), None);
    }
}
