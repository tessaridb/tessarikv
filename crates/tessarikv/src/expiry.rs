//! Expiring puts as readers see them: the value without its stamp until the stamp's time, and
//! absent — a delete at the put's version — from then on.

use std::time::{SystemTime, UNIX_EPOCH};

use tessarikv_types::{ValueKind, split_expiring};

use crate::Error;

/// What a reader sees in one version of a key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Seen<'a> {
    /// The key holds this value.
    Value(&'a [u8]),
    /// The key is absent from this version on: a delete, an expired put, or a range delete.
    Absent,
    /// An expiring put too short to hold its stamp; only a damaged file holds one.
    Malformed,
}

/// The wall clock in milliseconds since the Unix epoch; a clock set before the epoch reads 0.
pub(crate) fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| {
            u64::try_from(since.as_millis()).unwrap_or(u64::MAX)
        })
}

/// What a reader sees in a version of kind `kind` storing `stored`, by the clock now; the clock
/// is read only for an expiring put.
pub(crate) fn seen(kind: ValueKind, stored: &[u8]) -> Seen<'_> {
    match kind {
        ValueKind::ExpiringValue => seen_at(kind, stored, now_millis()),
        _ => seen_at(kind, stored, 0),
    }
}

/// What a reader sees in a version of kind `kind` storing `stored` at `now`, in milliseconds
/// since the Unix epoch: an expiring put has expired once `now` reaches its stamp.
pub(crate) fn seen_at(kind: ValueKind, stored: &[u8], now: u64) -> Seen<'_> {
    match kind {
        ValueKind::Value => Seen::Value(stored),
        ValueKind::ExpiringValue => match split_expiring(stored) {
            Some((expires_at, _)) if now >= expires_at => Seen::Absent,
            Some((_, value)) => Seen::Value(value),
            None => Seen::Malformed,
        },
        ValueKind::Delete | ValueKind::RangeDelete => Seen::Absent,
    }
}

/// The value the caller wrote in a put of kind `kind` storing `stored`: an expiring put's
/// without its stamp, whether or not it has expired.
pub(crate) fn written_value(kind: ValueKind, stored: &[u8]) -> &[u8] {
    match kind {
        ValueKind::ExpiringValue => split_expiring(stored).map_or(&[], |(_, value)| value),
        _ => stored,
    }
}

/// The error for an expiring put too short to hold its stamp, met in a table file.
pub(crate) fn malformed() -> Error {
    Error::Corruption {
        what: "table",
        path: None,
        offset: None,
        detail: "an expiring put is shorter than its expiry stamp".to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stamped(expires_at: u64, value: &[u8]) -> Vec<u8> {
        let mut stored = expires_at.to_be_bytes().to_vec();
        stored.extend_from_slice(value);
        stored
    }

    #[test]
    fn an_expiring_put_reads_its_value_until_its_stamp_and_absent_from_then_on() {
        let stored = stamped(1_000, b"v");
        let kind = ValueKind::ExpiringValue;
        assert_eq!(seen_at(kind, &stored, 999), Seen::Value(b"v"));
        assert_eq!(seen_at(kind, &stored, 1_000), Seen::Absent);
        assert_eq!(seen_at(kind, &stored[..7], 0), Seen::Malformed);
        assert_eq!(written_value(kind, &stored), b"v");
        assert_eq!(seen_at(ValueKind::Value, b"x", u64::MAX), Seen::Value(b"x"));
        assert_eq!(seen_at(ValueKind::Delete, b"", 0), Seen::Absent);
    }
}
