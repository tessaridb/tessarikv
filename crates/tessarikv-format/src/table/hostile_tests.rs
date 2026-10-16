//! Table files as hostile input: whatever the bytes, opening and walking a table returns an
//! error or entries, and never panics or loops.

use proptest::prelude::*;

use super::Compression;
use super::tests::{build, drain, open, sample};

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    #[test]
    fn arbitrary_bytes_never_panic(bytes in prop::collection::vec(any::<u8>(), 0..512)) {
        if let Ok(table) = open(bytes) {
            let mut walk = table.iter();
            walk.seek_to_first();
            let _ = drain(&mut walk);
        }
    }

    #[test]
    fn a_valid_table_with_a_region_overwritten_never_panics(
        start in any::<prop::sample::Index>(),
        noise in prop::collection::vec(any::<u8>(), 1..64),
    ) {
        let mut bytes = build(&sample(), Compression::Lz4);
        let at = start.index(bytes.len());
        for (offset, byte) in noise.iter().enumerate() {
            if let Some(slot) = bytes.get_mut(at.saturating_add(offset)) {
                *slot = *byte;
            }
        }
        if let Ok(table) = open(bytes) {
            let mut walk = table.iter();
            walk.seek_to_first();
            let _ = drain(&mut walk);
            walk.seek_to_last();
            let _ = table.range_deletes();
        }
    }
}
