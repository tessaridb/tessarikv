use proptest::prelude::*;

use super::*;

proptest! {
    #[test]
    fn every_u32_round_trips(value in any::<u32>()) {
        let mut out = Vec::new();
        put_u32(&mut out, value);
        prop_assert_eq!(get_u32(&out), Some((value, out.len())));
    }
}

#[test]
fn truncated_and_oversized_encodings_are_refused() {
    assert_eq!(get_u32(&[0x80]), None);
    assert_eq!(get_u32(&[]), None);
    assert_eq!(get_u32(&[0xFF, 0xFF, 0xFF, 0xFF, 0x1F]), None);
    assert_eq!(
        get_u32(&[0xFF, 0xFF, 0xFF, 0xFF, 0x0F]),
        Some((u32::MAX, 5))
    );
    assert_eq!(get_u32(&[0x80, 0x80, 0x80, 0x80, 0x80, 0x01]), None);
}
