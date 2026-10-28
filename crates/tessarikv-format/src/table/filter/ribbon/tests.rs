use proptest::prelude::*;

use super::*;
use crate::error::Error;
use crate::table::filter::{BloomBuilder, BloomFilter, hash};

fn hashes(keys: &[Vec<u8>]) -> Vec<u64> {
    keys.iter().map(|key| hash(key)).collect()
}

fn keys(prefix: &str, count: u32) -> Vec<Vec<u8>> {
    (0..count)
        .map(|n| format!("{prefix}-{n}").into_bytes())
        .collect()
}

fn ribbon(keys: &[Vec<u8>], bits_per_key: u32) -> (Vec<u8>, RibbonFilter) {
    let bytes = build(&hashes(keys), bits_per_key).expect("a ribbon solves");
    let filter = RibbonFilter::new(&bytes).expect("a built ribbon decodes");
    (bytes, filter)
}

proptest! {
    #[test]
    fn every_added_key_may_be_present(
        keys in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..24), 1..400),
        bits_per_key in 1_u32..30,
    ) {
        let (_, filter) = ribbon(&keys, bits_per_key);
        for key in &keys {
            prop_assert!(filter.may_contain(hash(key)));
        }
    }
}

#[test]
fn repeated_keys_solve() {
    let mut present = keys("present", 500);
    present.extend(keys("present", 500));
    let (_, filter) = ribbon(&present, 10);
    assert!(present.iter().all(|key| filter.may_contain(hash(key))));
}

#[test]
fn fewer_bytes_than_a_bloom_pass_fewer_absent_keys() {
    let present = keys("present", 20_000);
    let (bytes, filter) = ribbon(&present, 10);
    let mut bloom = BloomBuilder::new(10);
    for key in &present {
        bloom.add(key);
    }
    let bloom_bytes = bloom.finish();
    let bloom = BloomFilter::new(bloom_bytes.clone()).expect("a bloom decodes");
    let absent = keys("absent", 200_000);
    let passed_ribbon = absent
        .iter()
        .filter(|key| filter.may_contain(hash(key)))
        .count();
    let passed_bloom = absent.iter().filter(|key| bloom.may_contain(key)).count();
    // 7 result bits pass 2^-7 of absent keys, about 1 560 of 200 000.
    assert!(
        (1_200..2_000).contains(&passed_ribbon),
        "{passed_ribbon} of 200000 absent keys passed the ribbon"
    );
    assert!(
        passed_ribbon < passed_bloom,
        "{passed_ribbon} vs {passed_bloom}"
    );
    assert!(
        bytes.len().saturating_mul(100) <= bloom_bytes.len().saturating_mul(85),
        "{} ribbon bytes vs {} bloom bytes",
        bytes.len(),
        bloom_bytes.len()
    );
}

#[test]
fn the_derivation_is_fixed_by_the_format() {
    let (bytes, filter) = ribbon(&keys("present", 100), 10);
    assert_eq!(filter.result_bits, 7);
    assert_eq!(bytes.len(), filter.blocks_bytes() + TRAILER_BYTES);
    assert_eq!(bytes[bytes.len() - 5], MARKER);
    assert_eq!(
        row(hash(b"tessarikv"), 0, 128, 7),
        Row {
            start: 36,
            coefficient: 0x_a21d_9282_ab41_c15d,
            fingerprint: 0x2,
        }
    );
}

#[test]
fn a_single_key_and_a_full_block_solve() {
    for count in [1_u32, 63, 64, 65, 130] {
        let present = keys("present", count);
        let (_, filter) = ribbon(&present, 10);
        assert!(
            present.iter().all(|key| filter.may_contain(hash(key))),
            "{count}"
        );
    }
}

#[test]
fn a_damaged_ribbon_is_refused_by_name() {
    let (bytes, _) = ribbon(&keys("present", 300), 10);
    for length in 0..TRAILER_BYTES {
        assert!(matches!(
            RibbonFilter::new(&bytes[bytes.len() - length..]),
            Err(Error::Truncated { what: "filter", .. })
        ));
    }
    let refused = |bytes: Vec<u8>| match RibbonFilter::new(&bytes) {
        Err(Error::Malformed {
            what: "filter",
            reason,
        }) => reason,
        other => panic!("expected a refusal, got {other:?}"),
    };
    let at_end = |offset: usize| bytes.len() - offset;
    let mut marker = bytes.clone();
    marker[at_end(5)] = 0x81;
    assert_eq!(refused(marker), "not a ribbon filter");
    for result_bits in [0_u8, 33, 255] {
        let mut wrong = bytes.clone();
        wrong[at_end(6)] = result_bits;
        assert_eq!(refused(wrong), "result bits out of range");
    }
    let mut blocks = bytes.clone();
    blocks[at_end(4)] ^= 1;
    assert_eq!(
        refused(blocks),
        "block count does not match the filter's size"
    );
    let mut no_blocks = bytes.clone();
    no_blocks[at_end(4)..].copy_from_slice(&0_u32.to_le_bytes());
    assert_eq!(
        refused(no_blocks),
        "block count does not match the filter's size"
    );
    let mut huge = bytes.clone();
    huge[at_end(4)..].copy_from_slice(&u32::MAX.to_le_bytes());
    assert_eq!(
        refused(huge),
        "block count does not match the filter's size"
    );
    let mut short = bytes;
    short.remove(0);
    assert_eq!(
        refused(short),
        "block count does not match the filter's size"
    );
}

#[test]
fn equations_that_contradict_each_other_do_not_solve() {
    // A hundred independent equations over 64 slots: some reduce to 0 = 1, which no solution meets.
    let hashes = hashes(&keys("present", 100));
    assert!(solve(&hashes, 0, 64, 7).is_none());
    assert!(solve(&hashes[..10], 0, 64, 7).is_some());
}
