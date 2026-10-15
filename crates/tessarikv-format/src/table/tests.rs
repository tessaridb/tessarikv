use std::collections::BTreeMap;

use proptest::prelude::*;

use super::*;
use crate::error::{Error, Result};

fn bytewise(left: &[u8], right: &[u8]) -> std::cmp::Ordering {
    left.cmp(right)
}

fn whole(key: &[u8]) -> &[u8] {
    key
}

fn options(compression: Compression) -> TableOptions {
    TableOptions {
        block_bytes: 64,
        restart_interval: 4,
        bloom_bits_per_key: 10,
        compression,
    }
}

fn build(model: &BTreeMap<Vec<u8>, Vec<u8>>, compression: Compression) -> Vec<u8> {
    let mut builder = TableBuilder::new(options(compression), bytewise, whole);
    for (key, value) in model {
        builder.add(key, value).expect("keys arrive in order");
    }
    builder.finish().expect("finished")
}

fn open(bytes: Vec<u8>) -> Result<Table> {
    Table::open(bytes, bytewise, whole)
}

/// Key-value pairs read from a table.
type Entries = Vec<(Vec<u8>, Vec<u8>)>;

/// Every entry from the iterator's position on, and the iterator's final status.
fn drain(iter: &mut TableIter) -> (Entries, Result<()>) {
    let mut entries = Vec::new();
    while iter.valid() {
        entries.push((iter.key().to_vec(), iter.value().to_vec()));
        iter.next();
    }
    (entries, iter.status())
}

fn sample() -> BTreeMap<Vec<u8>, Vec<u8>> {
    (0..60_u32)
        .map(|n| {
            (
                format!("key-{n:03}").into_bytes(),
                format!("value of {n} ").repeat(3).into_bytes(),
            )
        })
        .collect()
}

proptest! {
    #[test]
    fn a_table_reads_back_what_was_written(
        model in prop::collection::btree_map(
            prop::collection::vec(any::<u8>(), 1..16),
            prop::collection::vec(any::<u8>(), 0..40),
            0..120,
        ),
        targets in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..16), 1..8),
        lz4 in any::<bool>(),
    ) {
        let compression = if lz4 { Compression::Lz4 } else { Compression::None };
        let table = open(build(&model, compression)).expect("opens");
        let mut iter = table.iter();
        iter.seek_to_first();
        let (entries, status) = drain(&mut iter);
        prop_assert!(status.is_ok());
        prop_assert_eq!(entries, model.clone().into_iter().collect::<Vec<_>>());
        for target in targets {
            let mut iter = table.iter();
            iter.seek(&target);
            let (entries, status) = drain(&mut iter);
            prop_assert!(status.is_ok());
            let expected: Vec<_> = model.range(target..).map(|(k, v)| (k.clone(), v.clone())).collect();
            prop_assert_eq!(entries, expected);
        }
        for key in model.keys() {
            prop_assert!(table.may_contain(key));
        }
        prop_assert_eq!(table.entries(), u64::try_from(model.len()).expect("fits"));
    }
}

#[test]
fn compression_shrinks_repetitive_blocks() {
    let model = sample();
    let plain = build(&model, Compression::None);
    let compressed = build(&model, Compression::Lz4);
    assert!(
        compressed.len() < plain.len(),
        "{} vs {}",
        compressed.len(),
        plain.len()
    );
}

#[test]
fn every_damaged_byte_is_detected() {
    let model: BTreeMap<_, _> = sample().into_iter().take(12).collect();
    for compression in [Compression::None, Compression::Lz4] {
        let bytes = build(&model, compression);
        for at in 0..bytes.len() {
            let mut damaged = bytes.clone();
            damaged[at] ^= 0x41;
            let Ok(table) = open(damaged) else { continue };
            let mut iter = table.iter();
            iter.seek_to_first();
            let (entries, status) = drain(&mut iter);
            assert!(
                status.is_err(),
                "byte {at} of {} ({compression:?}) damaged unnoticed, {} entries read",
                bytes.len(),
                entries.len()
            );
        }
    }
}

#[test]
fn a_truncated_file_or_an_unknown_version_is_refused() {
    let bytes = build(&sample(), Compression::None);
    assert!(open(bytes[..bytes.len() - 1].to_vec()).is_err());
    assert!(matches!(
        open(bytes[..10].to_vec()),
        Err(Error::Truncated { .. })
    ));
    let mut newer = bytes.clone();
    let version_at = newer.len() - FOOTER_BYTES + 48;
    newer[version_at..version_at + 4].copy_from_slice(&2_u32.to_le_bytes());
    let footer_at = newer.len() - FOOTER_BYTES;
    let checksum = crc32c::crc32c(&newer[footer_at..footer_at + 52]);
    newer[footer_at + 52..footer_at + 56].copy_from_slice(&checksum.to_le_bytes());
    assert!(matches!(
        open(newer),
        Err(Error::UnsupportedVersion { version: 2, .. })
    ));
}

#[test]
fn an_empty_table_has_no_entries() {
    let table = open(build(&BTreeMap::new(), Compression::Lz4)).expect("opens");
    let mut iter = table.iter();
    iter.seek_to_first();
    assert!(!iter.valid());
    assert!(iter.status().is_ok());
    assert!(!table.may_contain(b"anything"));
}

proptest! {
    #[test]
    fn a_table_walks_backward_and_seeks_for_the_previous_key(
        model in prop::collection::btree_map(
            prop::collection::vec(any::<u8>(), 1..16),
            prop::collection::vec(any::<u8>(), 0..40),
            0..120,
        ),
        targets in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..16), 1..8),
        lz4 in any::<bool>(),
        moves in prop::collection::vec(any::<bool>(), 0..60),
        start in any::<prop::sample::Index>(),
    ) {
        let compression = if lz4 { Compression::Lz4 } else { Compression::None };
        let table = open(build(&model, compression)).expect("opens");
        let mut iter = table.iter();
        iter.seek_to_last();
        let mut backward = Vec::new();
        while iter.valid() {
            // A step back that does not move fails here instead of looping forever.
            prop_assert!(backward.len() < model.len(), "prev did not move back");
            backward.push((iter.key().to_vec(), iter.value().to_vec()));
            iter.prev();
        }
        prop_assert!(iter.status().is_ok());
        let entries: Vec<_> = model.clone().into_iter().collect();
        prop_assert_eq!(backward, entries.iter().rev().cloned().collect::<Vec<_>>());
        for target in targets {
            iter.seek_for_prev(&target);
            let expected = model.range(..=target).next_back().map(|(key, _)| key.as_slice());
            prop_assert_eq!(iter.valid().then(|| iter.key()), expected);
        }
        if entries.is_empty() {
            return Ok(());
        }
        let mut at = start.index(entries.len());
        iter.seek(&entries[at].0);
        for forward in moves {
            prop_assert_eq!(iter.key(), entries[at].0.as_slice());
            if forward {
                iter.next();
                if at + 1 == entries.len() {
                    prop_assert!(!iter.valid());
                    break;
                }
                at += 1;
            } else {
                iter.prev();
                if at == 0 {
                    prop_assert!(!iter.valid());
                    break;
                }
                at -= 1;
            }
        }
    }
}
