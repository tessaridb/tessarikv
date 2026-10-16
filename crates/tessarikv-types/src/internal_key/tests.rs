use std::cmp::Ordering;

use proptest::prelude::*;

use super::*;
use crate::{SequenceNumber, ValueKind};

fn encoded(user_key: &[u8], seq: u64, kind: ValueKind) -> Vec<u8> {
    let mut out = Vec::new();
    let sequence = SequenceNumber::new(seq).unwrap_or(SequenceNumber::MAX);
    encode_internal(user_key, Trailer::new(sequence, kind), &mut out);
    out
}

fn kind_strategy() -> impl Strategy<Value = ValueKind> {
    prop_oneof![
        Just(ValueKind::Delete),
        Just(ValueKind::Value),
        Just(ValueKind::RangeDelete)
    ]
}

proptest! {
    /// Encoding then parsing gives back the user key, the sequence and the kind.
    #[test]
    fn round_trip(user_key in proptest::collection::vec(any::<u8>(), 0..64),
                  seq in 0..=tessarikv_constants::MAX_SEQUENCE,
                  kind in kind_strategy()) {
        let bytes = encoded(&user_key, seq, kind);
        let parsed = InternalKeyRef::parse(&bytes);
        prop_assert!(parsed.is_ok());
        let parsed = parsed.unwrap_or_else(|_| InternalKeyRef::sentinel());
        prop_assert_eq!(parsed.user_key(), user_key.as_slice());
        prop_assert_eq!(parsed.trailer().sequence().get(), seq);
        prop_assert_eq!(parsed.trailer().kind(), Ok(kind));
    }

    /// The order is user key ascending, then sequence descending (newest version first).
    #[test]
    fn order_is_user_key_then_newest(a in proptest::collection::vec(any::<u8>(), 0..8),
                                     b in proptest::collection::vec(any::<u8>(), 0..8),
                                     sa in 0u64..1000, sb in 0u64..1000) {
        let ea = encoded(&a, sa, ValueKind::Value);
        let eb = encoded(&b, sb, ValueKind::Value);
        let expected = a.cmp(&b).then(sb.cmp(&sa));
        prop_assert_eq!(compare_internal(&ea, &eb), expected);
    }
}

/// The trap a byte-wise order falls into: "a" with any version sorts before "ab".
#[test]
fn a_shorter_user_key_sorts_first_whatever_its_trailer() {
    let short = encoded(b"a", 0, ValueKind::Delete);
    let long = encoded(b"ab", tessarikv_constants::MAX_SEQUENCE, ValueKind::Value);
    assert_eq!(compare_internal(&short, &long), Ordering::Less);
}

/// A seek key at snapshot S sorts before every version of its user key at or below S and after
/// every version above S.
#[test]
fn seek_key_lands_on_the_newest_visible_version() {
    let snapshot = SequenceNumber::new(10).unwrap_or(SequenceNumber::ZERO);
    let mut seek = Vec::new();
    encode_internal(b"k", seek_trailer(snapshot), &mut seek);
    assert_eq!(
        compare_internal(&seek, &encoded(b"k", 10, ValueKind::Value)),
        Ordering::Less
    );
    assert_eq!(
        compare_internal(&seek, &encoded(b"k", 10, ValueKind::RangeDelete)),
        Ordering::Less
    );
    assert_eq!(
        compare_internal(&seek, &encoded(b"k", 11, ValueKind::Delete)),
        Ordering::Greater
    );
}

#[test]
fn a_key_shorter_than_its_trailer_is_refused() {
    assert_eq!(
        InternalKeyRef::parse(&[1, 2, 3]).map(|key| key.user_key().len()),
        Err(crate::Error::ShortInternalKey { len: 3 })
    );
}

proptest! {
    /// Whatever the bytes, parsing an internal key returns an error or a key, and never panics.
    #[test]
    fn arbitrary_bytes_never_panic(bytes in proptest::collection::vec(any::<u8>(), 0..64)) {
        let _ = InternalKeyRef::parse(&bytes);
    }
}
