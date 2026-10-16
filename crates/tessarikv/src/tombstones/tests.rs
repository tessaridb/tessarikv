use proptest::prelude::*;
use tessarikv_types::SequenceNumber;

use super::{Fragments, RangeTombstone};

fn seq(raw: u64) -> SequenceNumber {
    SequenceNumber::new(raw).unwrap()
}

/// Whether some tombstone visible at `snapshot` covers `key` with a sequence above `version`.
fn naive(tombstones: &[RangeTombstone], snapshot: u64, key: &[u8], version: u64) -> bool {
    tombstones.iter().any(|tombstone| {
        tombstone.sequence <= seq(snapshot)
            && tombstone.start.as_slice() <= key
            && key < tombstone.end.as_slice()
            && seq(version) < tombstone.sequence
    })
}

proptest! {
    #[test]
    fn fragments_delete_exactly_what_the_tombstones_say(
        ranges in proptest::collection::vec((0_u8..20, 0_u8..20, 1_u64..30), 0..12),
        snapshot in 0_u64..32,
    ) {
        let tombstones: Vec<_> = ranges
            .iter()
            .map(|&(start, end, sequence)| RangeTombstone {
                start: vec![start],
                end: vec![end],
                sequence: seq(sequence),
            })
            .collect();
        let fragments = Fragments::new(&tombstones, seq(snapshot));
        for key in 0_u8..21 {
            for version in 0_u64..32 {
                prop_assert_eq!(
                    fragments.deletes(&[key], seq(version)),
                    naive(&tombstones, snapshot, &[key], version),
                    "key {} version {}", key, version
                );
            }
        }
    }
}
