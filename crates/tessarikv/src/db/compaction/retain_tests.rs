use proptest::prelude::*;
use tessarikv_types::ValueKind;

use super::retain::Retention;

/// The newest version of `versions` (newest first) at or below `at`: its sequence and kind.
fn newest_at(versions: &[(u64, ValueKind)], at: u64) -> Option<(u64, ValueKind)> {
    versions
        .iter()
        .copied()
        .find(|(sequence, _)| *sequence <= at)
}

/// The value a reader at `at` sees: a delete reads as absent.
fn read_at(versions: &[(u64, ValueKind)], at: u64) -> Option<u64> {
    newest_at(versions, at)
        .filter(|(_, kind)| *kind == ValueKind::Value)
        .map(|(sequence, _)| sequence)
}

/// One key's versions, newest first, with distinct sequences.
fn versions() -> impl Strategy<Value = Vec<(u64, ValueKind)>> {
    proptest::collection::btree_map(1_u64..60, prop::bool::ANY, 0..12).prop_map(|map| {
        map.into_iter()
            .rev()
            .map(|(sequence, put)| {
                let kind = if put {
                    ValueKind::Value
                } else {
                    ValueKind::Delete
                };
                (sequence, kind)
            })
            .collect()
    })
}

proptest! {
    /// Every read a reader can still make answers the same over what is kept, at most one
    /// version stays per stripe at or below the visible sequence, and a delete in the oldest
    /// stripe stays only while a deeper level may hold the key.
    #[test]
    fn a_compaction_keeps_exactly_what_some_reader_can_still_see(
        versions in versions(),
        visible in 1_u64..64,
        snapshots in proptest::collection::vec(1_u64..64, 0..4),
        deeper in prop::bool::ANY,
    ) {
        let retention = Retention::new(visible, &snapshots);
        let mut key = retention.key();
        let kept: Vec<(u64, ValueKind)> = versions
            .iter()
            .copied()
            .filter(|(sequence, kind)| key.keeps(*sequence, *kind, || deeper))
            .collect();
        let mut reads: Vec<u64> = snapshots.iter().copied().filter(|at| *at < visible).collect();
        reads.push(visible);
        // A reader after this compaction may read at any sequence above the visible one.
        reads.extend((visible + 1)..64);
        for at in reads {
            if deeper {
                prop_assert_eq!(newest_at(&kept, at), newest_at(&versions, at), "at {}", at);
            } else {
                prop_assert_eq!(read_at(&kept, at), read_at(&versions, at), "at {}", at);
            }
        }
        let mut stripes: Vec<u64> = snapshots.iter().copied().filter(|at| *at < visible).collect();
        stripes.push(visible);
        stripes.sort_unstable();
        stripes.dedup();
        for (index, top) in stripes.iter().enumerate() {
            let bottom = index.checked_sub(1).map_or(0, |below| stripes[below]);
            let in_stripe = kept.iter().filter(|(sequence, _)| *sequence > bottom && sequence <= top).count();
            prop_assert!(in_stripe <= 1, "{} versions kept in the stripe up to {}", in_stripe, top);
        }
        let oldest = stripes[0];
        if !deeper {
            prop_assert!(
                !kept.iter().any(|(sequence, kind)| *sequence <= oldest && *kind == ValueKind::Delete),
                "a delete with nothing below it was kept in the oldest stripe"
            );
        }
    }
}
