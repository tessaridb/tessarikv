//! Offsets of keys in a table, found from its index alone.

use std::collections::BTreeMap;

use super::{Compression, Table, TableBuilder, TableOptions};

fn table(keys: u32) -> (Table, u64) {
    let options = TableOptions {
        block_bytes: 64,
        restart_interval: 4,
        bloom_bits_per_key: 10,
        filter: crate::table::FilterPolicy::Bloom,
        compression: Compression::None,
        prefix_length: None,
    };
    let mut builder = TableBuilder::new(options, |left, right| left.cmp(right), |key| key);
    let model: BTreeMap<Vec<u8>, Vec<u8>> = (0..keys)
        .map(|index| (format!("key{index:04}").into_bytes(), vec![b'v'; 40]))
        .collect();
    for (key, value) in &model {
        builder.add(key, value).expect("keys arrive in order");
    }
    let bytes = builder.finish().expect("finished");
    let len = u64::try_from(bytes.len()).expect("small table");
    let table = Table::open(bytes, |left, right| left.cmp(right), |key| key).expect("opened");
    (table, len)
}

/// The first key starts the data, a key past every key ends it, and offsets grow with keys and
/// split the data in proportion to the keys between them.
#[test]
fn offsets_grow_with_keys_and_split_the_data_in_proportion() {
    let (table, len) = table(200);
    let offset = |key: &[u8]| table.offset_of(key).expect("offset");
    assert_eq!(offset(b""), 0);
    let end = offset(b"zzz");
    assert!(
        end > 0 && end < len,
        "the data ends before the index: {end} of {len}"
    );
    assert!(
        offset(b"key0199") < end,
        "the last key's block starts before the data ends"
    );
    let mut previous = 0;
    for index in (0..200).step_by(10) {
        let at = offset(format!("key{index:04}").as_bytes());
        assert!(at >= previous, "offsets never shrink");
        previous = at;
    }
    let half = offset(b"key0100");
    assert!(
        half > end / 3 && half < end * 2 / 3,
        "half the keys lie near half the data: {half} of {end}"
    );
}

/// An empty table has no data.
#[test]
fn an_empty_table_has_no_data() {
    let (table, _) = table(0);
    assert_eq!(table.offset_of(b"key").expect("offset"), 0);
}
