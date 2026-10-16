use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, LazyLock};

use proptest::prelude::*;
use tessarikv_format::{Compression, TableMeta, TableOptions};
use tessarikv_types::{KeyspaceId, SequenceNumber, ValueKind};

use super::block_cache::BlockCache;
use super::read::{ScanRange, get_in, scan_in};
use super::table_blocks::open_table;
use super::tables::build;
use super::view::{TableFile, View};
use crate::Error;
use crate::memtable::Memtable;
use crate::storage::{MemoryStorage, Storage};

/// One write: a key and its new value, or `None` for a delete.
pub(super) type Op = (u8, Option<u16>);

pub(super) fn user_key(key: u8) -> [u8; 2] {
    [b'k', key]
}

fn options() -> TableOptions {
    TableOptions {
        block_bytes: 64,
        restart_interval: 2,
        bloom_bits_per_key: 10,
        compression: Compression::None,
    }
}

/// Writes `ops` into `table`, the first at sequence `first`.
fn fill(table: &Memtable, first: usize, ops: &[Op]) {
    for (offset, (key, value)) in ops.iter().enumerate() {
        let sequence = SequenceNumber::new(u64::try_from(first + offset).unwrap()).unwrap();
        match value {
            Some(value) => table
                .insert(
                    sequence,
                    ValueKind::Value,
                    &user_key(*key),
                    &value.to_le_bytes(),
                )
                .unwrap(),
            None => table
                .insert(sequence, ValueKind::Delete, &user_key(*key), b"")
                .unwrap(),
        }
    }
}

/// Runs `future` to completion on a runtime shared by the tests: reads of table files load
/// their blocks on blocking threads.
pub(super) fn block_on<T>(future: impl Future<Output = T>) -> T {
    static RUNTIME: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .build()
            .unwrap()
    });
    RUNTIME.block_on(future)
}

/// Block cache capacities worth reading through: none, a few blocks (so blocks are dropped and
/// read again), and more than any test table.
pub(super) fn cache_bytes() -> impl Strategy<Value = u64> {
    prop_oneof![Just(0), Just(300), Just(1 << 20)]
}

/// A table file holding `bytes`, built from a table whose bounds `built` describes, written to
/// storage and opened through a cache of `cache` bytes.
pub(super) fn table_file(
    bytes: Vec<u8>,
    built: &super::tables::BuiltTable,
    id: u64,
    cache: &Arc<BlockCache>,
) -> TableFile {
    table_file_in(&MemoryStorage::new(), bytes, built, id, cache)
}

/// As [`table_file`], written to and read from `storage`.
pub(super) fn table_file_in(
    storage: &dyn Storage,
    bytes: Vec<u8>,
    built: &super::tables::BuiltTable,
    id: u64,
    cache: &Arc<BlockCache>,
) -> TableFile {
    let meta = TableMeta {
        keyspace: 0,
        level: 0,
        id,
        size: u64::try_from(bytes.len()).unwrap(),
        smallest: built.smallest.clone(),
        largest: built.largest.clone(),
        min_sequence: built.min_sequence,
        max_sequence: built.max_sequence,
    };
    let path = PathBuf::from(format!("tables/{id}.sst"));
    let mut written = storage.create(&path).unwrap();
    written.append(&bytes).unwrap();
    drop(written);
    let (table, blocks) = open_table(storage, &path, id, cache, None).unwrap();
    TableFile {
        meta: Arc::new(meta),
        table,
        blocks,
    }
}

/// The table built from `table`, and the bytes of its file.
pub(super) fn built(
    table: &Memtable,
    options: tessarikv_format::TableOptions,
) -> (super::tables::BuiltTable, Vec<u8>) {
    let mut bytes = Vec::new();
    let built = build(table, options, &mut |piece| {
        bytes.extend_from_slice(piece);
        Ok(())
    })
    .unwrap();
    (built, bytes)
}

/// The table file built from `table`, numbered `id`, read through `cache`.
pub(super) fn file(table: &Memtable, id: u64, cache: &Arc<BlockCache>) -> TableFile {
    file_in(&MemoryStorage::new(), table, id, cache)
}

/// As [`file`], written to and read from `storage`.
pub(super) fn file_in(
    storage: &dyn Storage,
    table: &Memtable,
    id: u64,
    cache: &Arc<BlockCache>,
) -> TableFile {
    let (built, bytes) = built(table, options());
    table_file_in(storage, bytes, &built, id, cache)
}

/// A scan page's range: `[start, ..)` with up to `limit` entries.
pub(super) fn from(start: &[u8], limit: usize) -> ScanRange<'_> {
    ScanRange {
        start,
        end: None,
        limit,
    }
}

/// What every key holds once the first `count` writes are applied.
pub(super) fn model(ops: &[Op], count: usize) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let mut state = BTreeMap::new();
    for (key, value) in ops.iter().take(count) {
        match value {
            Some(value) => state.insert(user_key(*key).to_vec(), value.to_le_bytes().to_vec()),
            None => state.remove(user_key(*key).as_slice()),
        };
    }
    state
}

/// Three in-memory tables holding `ops` split at `first` and `second`, newest first: the view
/// before any flush.
pub(super) fn unflushed(ops: &[Op], first: usize, second: usize) -> View {
    let view = View::new(0, Memtable::new());
    fill(view.table(0, 0).unwrap(), 1, &ops[..first]);
    let view = view.with_next_table();
    fill(view.table(0, 1).unwrap(), first + 1, &ops[first..second]);
    let view = view.with_next_table();
    fill(view.table(0, 2).unwrap(), second + 1, &ops[second..]);
    view
}

proptest! {
    #[test]
    fn reads_through_table_files_answer_as_the_writes_say(
        ops in proptest::collection::vec((0_u8..12, proptest::option::of(any::<u16>())), 3..60),
        cuts in (any::<prop::sample::Index>(), any::<prop::sample::Index>()),
        capacity in cache_bytes(),
    ) {
        let cache = Arc::new(BlockCache::new(capacity, tessarikv_constants::DEFAULT_BLOCK_BYTES));
        // Both older tables hold at least one write: an empty table is never flushed.
        let first = 1 + cuts.0.index(ops.len() - 2);
        let second = first + 1 + cuts.1.index(ops.len() - first - 1);
        let before = unflushed(&ops, first, second);
        let oldest = file(before.table(0, 0).unwrap(), 1, &cache);
        let older = file(before.table(0, 1).unwrap(), 2, &cache);
        let after = before.after_flush(0, Some(oldest)).unwrap().after_flush(1, Some(older)).unwrap();
        // The flushed writes are reachable only through the files.
        prop_assert_eq!(after.memtables(0).count(), 1);
        prop_assert_eq!(after.files(0).count(), 2);
        let default = KeyspaceId::DEFAULT;
        for snapshot in 0..=ops.len() {
            let at = SequenceNumber::new(u64::try_from(snapshot).unwrap()).unwrap();
            let expected = model(&ops, snapshot);
            for key in 0_u8..13 {
                let key = user_key(key);
                prop_assert_eq!(
                    block_on(get_in(&after, default, &key, at, true)).unwrap(),
                    expected.get(key.as_slice()).cloned(),
                    "key {:?} at {}", key, snapshot
                );
            }
            let listed: Vec<_> = expected.into_iter().collect();
            let whole = block_on(scan_in(&after, default, from(b"", usize::MAX), at, true)).unwrap();
            prop_assert_eq!(&whole.entries, &listed);
            // Pages of three, each starting where the last one stopped, list the same entries.
            let mut paged = Vec::new();
            let mut start = Vec::new();
            loop {
                let page = block_on(scan_in(&after, default, from(&start, 3), at, true)).unwrap();
                paged.extend(page.entries);
                match page.next {
                    Some(next) => start = next,
                    None => break,
                }
            }
            prop_assert_eq!(paged, listed);
        }
    }
}

#[tokio::test]
async fn a_damaged_table_file_fails_the_scan_instead_of_shortening_it() {
    let table = Memtable::new();
    let ops: Vec<Op> = (0..12).map(|key| (key, Some(u16::from(key)))).collect();
    fill(&table, 1, &ops);
    let (built, mut bytes) = built(&table, options());
    // The first data block starts the file; the footer, index and filter stay whole.
    bytes[1] ^= 0xff;
    let view = View::new(0, Memtable::new())
        .with_next_table()
        .after_flush(
            0,
            Some(table_file(
                bytes,
                &built,
                1,
                &Arc::new(BlockCache::new(0, tessarikv_constants::DEFAULT_BLOCK_BYTES)),
            )),
        )
        .unwrap();
    let at = SequenceNumber::new(100).unwrap();
    let scanned = scan_in(&view, KeyspaceId::DEFAULT, from(b"", usize::MAX), at, true).await;
    assert!(
        matches!(scanned, Err(Error::Corruption { what: "table", .. })),
        "{scanned:?}"
    );
}
