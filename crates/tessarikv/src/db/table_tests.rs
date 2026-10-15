use proptest::prelude::*;
use tessarikv_format::{Compression, TableOptions};
use tessarikv_types::{SequenceNumber, ValueKind};

use super::tables::{TableLookup, build, lookup, open};
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
        let built = build(&memtable, options(compression)).unwrap();
        prop_assert_eq!(built.min_sequence, 1);
        prop_assert_eq!(built.max_sequence, u64::try_from(ops.len()).unwrap());
        let keys: Vec<u8> = ops.iter().map(|(key, _)| *key).collect();
        let (low, high) = (keys.iter().min().copied(), keys.iter().max().copied());
        prop_assert_eq!(built.smallest.get(..2), Some(&[b'k', low.unwrap()][..]));
        prop_assert_eq!(built.largest.get(..2), Some(&[b'k', high.unwrap()][..]));
        let table = open(built.bytes, None).unwrap();
        for snapshot in 0..=u64::try_from(ops.len()).unwrap() {
            let snapshot = SequenceNumber::new(snapshot).unwrap();
            for key in 0_u8..13 {
                let user_key = [b'k', key];
                prop_assert_eq!(
                    lookup(&table, &user_key, snapshot).unwrap(),
                    expected(&memtable, &user_key, snapshot),
                    "key {} at snapshot {}", key, snapshot.get()
                );
            }
        }
    }
}
