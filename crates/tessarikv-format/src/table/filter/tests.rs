use proptest::prelude::*;

use super::*;
use crate::error::Error;

fn build(keys: &[Vec<u8>], bits_per_key: u32) -> BloomFilter {
    let mut builder = BloomBuilder::new(bits_per_key);
    for key in keys {
        builder.add(key);
    }
    BloomFilter::new(builder.finish()).expect("a built filter decodes")
}

proptest! {
    #[test]
    fn every_added_key_may_be_present(
        keys in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..24), 0..300),
        bits_per_key in 1_u32..20,
    ) {
        let filter = build(&keys, bits_per_key);
        for key in &keys {
            prop_assert!(filter.may_contain(key));
        }
    }
}

#[test]
fn absent_keys_pass_rarely_at_ten_bits_per_key() {
    let present: Vec<Vec<u8>> = (0..10_000_u32)
        .map(|n| format!("present-{n}").into_bytes())
        .collect();
    let filter = build(&present, 10);
    let false_positives = (0..10_000_u32)
        .filter(|n| filter.may_contain(format!("absent-{n}").as_bytes()))
        .count();
    assert!(
        false_positives < 200,
        "{false_positives} of 10000 absent keys passed"
    );
}

#[test]
fn an_empty_filter_excludes_everything() {
    let filter = build(&[], 10);
    assert!(!filter.may_contain(b"anything"));
}

#[test]
fn a_filter_whose_size_or_probe_count_is_wrong_is_refused() {
    let bytes = BloomBuilder::new(10).finish();
    let mut short = bytes.clone();
    short.remove(0);
    assert!(matches!(
        BloomFilter::new(short),
        Err(Error::Malformed { .. })
    ));
    let mut no_probes = bytes.clone();
    let probes_at = no_probes.len() - 5;
    no_probes[probes_at] = 0;
    assert!(matches!(
        BloomFilter::new(no_probes),
        Err(Error::Malformed { .. })
    ));
    assert!(matches!(
        BloomFilter::new(vec![1, 2]),
        Err(Error::Truncated { .. })
    ));
}

#[test]
fn the_hash_is_fixed_by_the_format() {
    assert_eq!(hash(b""), hash(b""));
    assert_ne!(hash(b"a"), hash(b"b"));
    assert_eq!(hash(b"tessarikv"), 0x_5478_0536_bd47_3e92);
}
