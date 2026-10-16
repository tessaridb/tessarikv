use std::collections::BTreeMap;

use proptest::prelude::*;

use super::codec_tests::EVERY;
use super::filter::Probing;
use super::handle::Footer;
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
        codec in 0..EVERY.len(),
    ) {
        let compression = EVERY[codec];
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
fn every_codec_shrinks_repetitive_blocks() {
    let model = sample();
    let plain = build(&model, Compression::None);
    for codec in EVERY.into_iter().skip(1) {
        let compressed = build(&model, codec);
        assert!(
            compressed.len() < plain.len(),
            "{codec:?}: {} vs {}",
            compressed.len(),
            plain.len()
        );
    }
}

#[test]
fn every_damaged_byte_is_detected() {
    let model: BTreeMap<_, _> = sample().into_iter().take(12).collect();
    for compression in EVERY {
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
    assert!(matches!(
        open(with_version(&bytes, 4)),
        Err(Error::UnsupportedVersion { version: 4, .. })
    ));
    // The first version, which has no range deletes, still opens.
    assert!(open(with_version(&bytes, 1)).is_ok());
}

#[test]
fn the_format_version_decides_how_the_filter_is_probed() {
    let bytes = build(&sample(), Compression::None);
    let probing = |version| {
        Footer::decode(&with_version(&bytes, version))
            .unwrap()
            .probing
    };
    assert_eq!(probing(3), Probing::Remixed);
    assert_eq!(probing(2), Probing::Stepped);
    assert_eq!(probing(1), Probing::Stepped);
}

/// `bytes` with the footer's format version replaced and the footer resealed.
fn with_version(bytes: &[u8], version: u32) -> Vec<u8> {
    let mut changed = bytes.to_vec();
    let footer_at = changed.len() - FOOTER_BYTES;
    changed[footer_at + 48..footer_at + 52].copy_from_slice(&version.to_le_bytes());
    let checksum = crc32c::crc32c(&changed[footer_at..footer_at + 52]);
    changed[footer_at + 52..footer_at + 56].copy_from_slice(&checksum.to_le_bytes());
    changed
}

#[test]
fn range_deletes_are_kept_apart_and_read_back() {
    let mut builder = TableBuilder::new(options(Compression::Lz4), bytewise, whole);
    builder.add(b"a", b"1").unwrap();
    builder.add_range_delete(b"b", b"d").unwrap();
    builder.add_range_delete(b"c", b"f").unwrap();
    assert!(builder.add_range_delete(b"c", b"g").is_err());
    let table = open(builder.finish().unwrap()).unwrap();
    assert_eq!(table.entries(), 1);
    assert_eq!(
        table.range_deletes().unwrap(),
        [
            (b"b".to_vec(), b"d".to_vec()),
            (b"c".to_vec(), b"f".to_vec())
        ]
    );
    let plain = open(build(&sample(), Compression::None)).unwrap();
    assert!(plain.range_deletes().unwrap().is_empty());
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
        codec in 0..EVERY.len(),
        moves in prop::collection::vec(any::<bool>(), 0..60),
        start in any::<prop::sample::Index>(),
    ) {
        let compression = EVERY[codec];
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

proptest! {
    /// Taking the finished bytes at any points while building yields, concatenated with what
    /// `finish` returns, exactly the bytes of a table built in one piece.
    #[test]
    fn a_table_taken_in_pieces_is_the_table_built_whole(
        takes in proptest::collection::vec(any::<bool>(), 60),
        codec in 0..EVERY.len(),
    ) {
        let compression = EVERY[codec];
        let model = sample();
        let mut builder = TableBuilder::new(options(compression), bytewise, whole);
        let mut pieces = Vec::new();
        for ((key, value), take) in model.iter().zip(&takes) {
            builder.add(key, value).expect("keys arrive in order");
            if *take {
                let taken = builder.drained();
                let piece = builder.take_finished();
                prop_assert_eq!(taken, u64::try_from(pieces.len()).unwrap());
                pieces.extend_from_slice(&piece);
            }
        }
        prop_assert_eq!(builder.drained(), u64::try_from(pieces.len()).unwrap());
        pieces.extend_from_slice(&builder.finish().expect("finished"));
        prop_assert_eq!(&pieces, &build(&model, compression));
        prop_assert!(open(pieces).is_ok());
    }
}
