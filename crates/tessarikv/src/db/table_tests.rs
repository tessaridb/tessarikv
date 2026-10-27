use std::sync::Arc;

use proptest::prelude::*;
use tessarikv_format::{Compression, TableOptions};
use tessarikv_types::{SequenceNumber, ValueKind};

use super::block_cache::BlockCache;
use super::lookup_key::LookupKey;
use super::read_tests::{block_on, built, cache_bytes, table_file};
use super::tables::{TableLookup, lookup};
use crate::memtable::{Lookup, Memtable};

fn options(compression: Compression) -> TableOptions {
    TableOptions {
        block_bytes: 64,
        restart_interval: 2,
        bloom_bits_per_key: 10,
        compression,
    }
}

fn expected(table: &Memtable, key: &[u8], snapshot: SequenceNumber) -> TableLookup {
    match table.get(key, snapshot) {
        Lookup::Value(value) => TableLookup::Value(value.to_vec()),
        Lookup::Deleted => TableLookup::Deleted,
        Lookup::Absent => TableLookup::Absent,
    }
}

proptest! {
    #[test]
    fn a_built_table_answers_every_key_at_every_snapshot_as_its_memtable(
        ops in proptest::collection::vec((0_u8..12, proptest::option::of(any::<u16>())), 1..60),
        lz4 in any::<bool>(),
        capacity in cache_bytes(),
    ) {
        let memtable = Memtable::new();
        for (index, (key, value)) in ops.iter().enumerate() {
            let sequence = SequenceNumber::new(u64::try_from(index).unwrap() + 1).unwrap();
            let user_key = [b'k', *key];
            match value {
                Some(value) => memtable
                    .insert(sequence, ValueKind::Value, &user_key, &value.to_le_bytes())
                    .unwrap(),
                None => memtable.insert(sequence, ValueKind::Delete, &user_key, b"").unwrap(),
            }
        }
        let compression = if lz4 { Compression::Lz4 } else { Compression::None };
        let (built, bytes) = built(&memtable, options(compression));
        prop_assert_eq!(built.min_sequence, 1);
        prop_assert_eq!(built.max_sequence, u64::try_from(ops.len()).unwrap());
        let keys: Vec<u8> = ops.iter().map(|(key, _)| *key).collect();
        let (low, high) = (keys.iter().min().copied(), keys.iter().max().copied());
        prop_assert_eq!(built.smallest.get(..2), Some(&[b'k', low.unwrap()][..]));
        prop_assert_eq!(built.largest.get(..2), Some(&[b'k', high.unwrap()][..]));
        let cache = Arc::new(BlockCache::new(capacity, tessarikv_constants::DEFAULT_BLOCK_BYTES));
        let file = table_file(bytes, &built, 1, &cache);
        for snapshot in 0..=u64::try_from(ops.len()).unwrap() {
            let snapshot = SequenceNumber::new(snapshot).unwrap();
            for key in 0_u8..13 {
                let user_key = [b'k', key];
                prop_assert_eq!(
                    block_on(lookup(&file, &LookupKey::new(&user_key, snapshot), 0, true, None)).unwrap(),
                    expected(&memtable, &user_key, snapshot),
                    "key {} at snapshot {}", key, snapshot.get()
                );
            }
        }
    }
}

#[test]
fn a_table_larger_than_a_write_chunk_is_written_in_pieces_and_reads_back_whole() {
    let memtable = Memtable::new();
    let value = vec![7_u8; 1024];
    for id in 0_u32..3 * 1024 {
        let sequence = SequenceNumber::new(u64::from(id) + 1).unwrap();
        memtable
            .insert(sequence, ValueKind::Value, &id.to_be_bytes(), &value)
            .unwrap();
    }
    let mut pieces = 0;
    let mut bytes = Vec::new();
    let built = super::tables::build(&memtable, options(Compression::None), None, &mut |piece| {
        pieces += 1;
        bytes.extend_from_slice(piece);
        Ok(())
    })
    .unwrap();
    assert!(pieces > 2, "3 MiB went out in {pieces} pieces");
    assert_eq!(built.size(), u64::try_from(bytes.len()).unwrap());
    assert_eq!(
        built.tail.as_slice(),
        &bytes[bytes.len() - built.tail.len()..]
    );
    let cache = Arc::new(BlockCache::new(
        1 << 20,
        tessarikv_constants::DEFAULT_BLOCK_BYTES,
    ));
    let file = table_file(bytes, &built, 1, &cache);
    let snapshot = SequenceNumber::new(u64::MAX >> 8).unwrap();
    for id in (0_u32..3 * 1024).step_by(97) {
        assert_eq!(
            block_on(lookup(
                &file,
                &LookupKey::new(&id.to_be_bytes(), snapshot),
                0,
                true,
                None
            ))
            .unwrap(),
            TableLookup::Value(value.clone()),
            "key {id}"
        );
    }
}
