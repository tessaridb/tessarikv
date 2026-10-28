use super::*;
use crate::error::Error;

fn built(policy: FilterPolicy, count: u32) -> (Vec<Vec<u8>>, Vec<u8>) {
    let keys: Vec<Vec<u8>> = (0..count)
        .map(|n| format!("present-{n}").into_bytes())
        .collect();
    let mut builder = FilterBuilder::new(policy, 10);
    for key in &keys {
        builder.add(key);
    }
    (keys, builder.finish())
}

#[test]
fn each_policy_writes_its_kind_and_the_reader_tells_them_apart() {
    for (policy, ribbon) in [(FilterPolicy::Bloom, false), (FilterPolicy::Ribbon, true)] {
        let (keys, bytes) = built(policy, 1_000);
        let filter = TableFilter::decode(bytes, Probing::Remixed).expect("decodes");
        assert_eq!(
            matches!(filter, TableFilter::Ribbon(_)),
            ribbon,
            "{policy:?}"
        );
        assert!(keys.iter().all(|key| filter.may_contain(key)), "{policy:?}");
        assert!(!filter.may_contain(b"absent"), "{policy:?}");
    }
}

#[test]
fn a_ribbon_with_no_keys_is_a_bloom_filter_that_excludes_everything() {
    let (_, bytes) = built(FilterPolicy::Ribbon, 0);
    let filter = TableFilter::decode(bytes, Probing::Remixed).expect("decodes");
    assert!(matches!(filter, TableFilter::Bloom(_)));
    assert!(!filter.may_contain(b"anything"));
}

#[test]
fn a_bloom_filter_whose_probe_byte_is_neither_kind_is_refused() {
    let (_, mut bytes) = built(FilterPolicy::Bloom, 100);
    let at = bytes.len() - 5;
    bytes[at] = 0x81;
    assert!(matches!(
        TableFilter::decode(bytes, Probing::Remixed),
        Err(Error::Malformed {
            what: "filter",
            reason: "probe count out of range"
        })
    ));
}
