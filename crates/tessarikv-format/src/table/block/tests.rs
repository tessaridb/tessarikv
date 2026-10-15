use std::collections::BTreeMap;

use proptest::prelude::*;

use super::*;
use crate::error::Error;

fn bytewise(left: &[u8], right: &[u8]) -> std::cmp::Ordering {
    left.cmp(right)
}

fn build(entries: &BTreeMap<Vec<u8>, Vec<u8>>, interval: usize) -> Block {
    let mut builder = BlockBuilder::new(interval, bytewise);
    for (key, value) in entries {
        builder.add(key, value).expect("keys arrive in order");
    }
    Block::new(builder.finish(), bytewise).expect("a built block decodes")
}

fn collect(iter: &mut BlockIter<'_>) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut out = Vec::new();
    while iter.valid() {
        out.push((iter.key().to_vec(), iter.value().to_vec()));
        iter.next();
    }
    out
}

fn sample() -> BTreeMap<Vec<u8>, Vec<u8>> {
    (0..100_u32)
        .map(|n| {
            (
                format!("key-{n:04}").into_bytes(),
                format!("value {n}").into_bytes(),
            )
        })
        .collect()
}

#[test]
fn entries_come_back_in_order_at_every_restart_interval() {
    let model = sample();
    for interval in [1, 4, 16, 1000] {
        let block = build(&model, interval);
        let mut iter = block.iter();
        iter.seek_to_first();
        let expected: Vec<_> = model.clone().into_iter().collect();
        assert_eq!(collect(&mut iter), expected, "interval {interval}");
        assert!(iter.status().is_ok());
    }
}

#[test]
fn an_empty_block_has_no_entries() {
    let block = Block::new(BlockBuilder::new(16, bytewise).finish(), bytewise).expect("decodes");
    let mut iter = block.iter();
    iter.seek_to_first();
    assert!(!iter.valid());
    iter.seek(b"anything");
    assert!(!iter.valid());
}

#[test]
fn keys_out_of_order_are_refused() {
    let mut builder = BlockBuilder::new(16, bytewise);
    builder.add(b"b", b"1").expect("first key");
    assert!(matches!(
        builder.add(b"a", b"2"),
        Err(Error::Malformed { .. })
    ));
    assert!(matches!(
        builder.add(b"b", b"3"),
        Err(Error::Malformed { .. })
    ));
}

#[test]
fn a_restart_array_pointing_outside_the_block_is_refused() {
    let mut bytes = BlockBuilder::new(16, bytewise).finish();
    bytes.splice(0..0, [0_u8; 4]);
    let count = bytes.len() - 4;
    bytes[count..].copy_from_slice(&1000_u32.to_le_bytes());
    assert!(matches!(
        Block::new(bytes, bytewise),
        Err(Error::Malformed { .. })
    ));
    assert!(matches!(
        Block::new(vec![1, 2], bytewise),
        Err(Error::Truncated { .. })
    ));
}

proptest! {
    #[test]
    fn seek_lands_on_the_first_key_not_less_than_the_target(
        model in prop::collection::btree_map(
            prop::collection::vec(any::<u8>(), 0..12),
            prop::collection::vec(any::<u8>(), 0..20),
            0..80,
        ),
        targets in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..12), 1..10),
        interval in 1_usize..20,
    ) {
        let block = build(&model, interval);
        for target in targets {
            let mut iter = block.iter();
            iter.seek(&target);
            let expected: Vec<_> = model.range(target.clone()..).map(|(k, v)| (k.clone(), v.clone())).collect();
            prop_assert_eq!(collect(&mut iter), expected);
        }
    }

    #[test]
    fn arbitrary_bytes_never_panic_and_never_loop(bytes in prop::collection::vec(any::<u8>(), 0..300)) {
        if let Ok(block) = Block::new(bytes, bytewise) {
            let mut iter = block.iter();
            iter.seek_to_first();
            let mut steps = 0;
            while iter.valid() && steps < 1000 {
                let _ = (iter.key(), iter.value());
                iter.next();
                steps += 1;
            }
            prop_assert!(steps < 1000);
            iter.seek(b"m");
        }
    }
}

proptest! {
    #[test]
    fn a_damaged_valid_block_never_panics(
        model in prop::collection::btree_map(
            prop::collection::vec(any::<u8>(), 0..12),
            prop::collection::vec(any::<u8>(), 0..20),
            1..40,
        ),
        at in any::<prop::sample::Index>(),
        flip in 1_u8..=255,
    ) {
        let mut builder = BlockBuilder::new(4, bytewise);
        for (key, value) in &model {
            builder.add(key, value).expect("keys arrive in order");
        }
        let mut bytes = builder.finish();
        let index = at.index(bytes.len());
        bytes[index] ^= flip;
        if let Ok(block) = Block::new(bytes, bytewise) {
            let mut iter = block.iter();
            iter.seek_to_first();
            let mut steps = 0;
            while iter.valid() && steps <= model.len() * 2 + 10 {
                iter.next();
                steps += 1;
            }
            prop_assert!(steps <= model.len() * 2 + 10);
            iter.seek(b"m");
        }
    }
}

#[test]
fn a_restart_offset_past_the_entries_or_out_of_order_is_refused() {
    let mut builder = BlockBuilder::new(1, bytewise);
    for key in [&b"a"[..], b"b", b"c"] {
        builder.add(key, b"v").expect("keys arrive in order");
    }
    let bytes = builder.finish();
    let restarts_at = bytes.len() - 4 - 3 * 4;
    let mut past = bytes.clone();
    past[restarts_at + 4..restarts_at + 8].copy_from_slice(
        &u32::try_from(restarts_at)
            .expect("small block")
            .to_le_bytes(),
    );
    assert!(matches!(
        Block::new(past, bytewise),
        Err(Error::Malformed { .. })
    ));
    let mut backwards = bytes;
    let first = backwards[restarts_at..restarts_at + 4].to_vec();
    backwards[restarts_at + 8..restarts_at + 12].copy_from_slice(&first);
    assert!(matches!(
        Block::new(backwards, bytewise),
        Err(Error::Malformed { .. })
    ));
}

#[test]
fn an_entry_sharing_more_than_the_previous_key_is_reported() {
    let mut builder = BlockBuilder::new(16, bytewise);
    builder.add(b"key", b"value").expect("first key");
    let mut bytes = builder.finish();
    bytes[0] = 2;
    let block = Block::new(bytes, bytewise).expect("the trailer is intact");
    let mut iter = block.iter();
    iter.seek_to_first();
    assert!(!iter.valid());
    assert!(matches!(iter.status(), Err(Error::Malformed { .. })));
}
