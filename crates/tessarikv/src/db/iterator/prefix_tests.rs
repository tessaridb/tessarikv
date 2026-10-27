//! Reads of a keyspace that filters its tables by key prefix: every scan and iterator move reads
//! what the same reads of an unfiltered keyspace read, and a read inside one absent prefix reads
//! fewer blocks.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use tessarikv_types::KeyspaceId;

use crate::db::Db;
use crate::db::flush_tests::PATH;
use crate::storage::counted::CountedStorage;
use crate::{DbOptions, KeyspaceOptions, ReadOptions, WriteBatch, WriteOptions};

const PREFIXES: u32 = 24;

fn prefix(index: u32) -> Vec<u8> {
    format!("p{index:03}").into_bytes()
}

fn key(prefix_index: u32, index: u32) -> Vec<u8> {
    let mut key = prefix(prefix_index);
    key.extend_from_slice(format!("-{index:03}").as_bytes());
    key
}

/// No block cache and no compaction of its own; tables filtered by four-byte prefixes when
/// `filtered`.
fn options(filtered: bool) -> DbOptions {
    let keyspace = KeyspaceOptions::builder()
        .memtable_bytes(64 * 1024)
        .disable_auto_compactions(true);
    let keyspace = if filtered {
        keyspace.prefix_length(4)
    } else {
        keyspace
    };
    DbOptions::builder()
        .default_keyspace(keyspace.build().expect("valid keyspace"))
        .block_cache_bytes(0)
        .build()
        .expect("valid options")
}

/// Six tables, each holding the even prefixes of one stripe, with deletes, a range delete and
/// keys shorter than a prefix; compacted into sorted levels when `compact`.
async fn filled(filtered: bool, compact: bool) -> (Db, CountedStorage) {
    let storage = CountedStorage::new(false);
    let db = Db::open_with_storage(PATH, options(filtered), Arc::new(storage.clone()))
        .await
        .expect("opened");
    let keyspace = KeyspaceId::DEFAULT;
    for table in 0..6 {
        let mut batch = WriteBatch::new();
        for prefix_index in (0..PREFIXES).filter(|index| index % 6 == table && index % 2 == 0) {
            for index in 0..40 {
                batch
                    .put(keyspace, &key(prefix_index, index), &[7; 40])
                    .expect("entry");
            }
            batch
                .delete(keyspace, &key(prefix_index, 3))
                .expect("entry");
        }
        batch.put(keyspace, b"p0", b"short").expect("entry");
        if table == 2 {
            batch
                .delete_range(keyspace, &key(2, 10), &key(2, 20))
                .expect("range delete");
        }
        db.write(&batch, &WriteOptions::default())
            .await
            .expect("written");
        db.flush().await.expect("flushed");
    }
    if compact {
        db.compact_range(keyspace, b"", None)
            .await
            .expect("compacted");
    }
    (db, storage)
}

type Entries = Vec<(Vec<u8>, Vec<u8>)>;

/// What a scan, a forward walk and a backward walk inside `options`' bounds read.
async fn reads(db: &Db, options: &ReadOptions) -> (Entries, Entries, Entries) {
    let low = options.lower_bound.clone().unwrap_or_default();
    let page = db
        .scan(
            KeyspaceId::DEFAULT,
            &low,
            options.upper_bound.as_deref(),
            usize::MAX,
            options,
        )
        .await
        .expect("scanned");
    let mut iterator = db.iter(KeyspaceId::DEFAULT, options).expect("iterator");
    let (mut forward, mut backward) = (Vec::new(), Vec::new());
    iterator.seek_to_first().await.expect("moved");
    while iterator.valid() {
        forward.push((iterator.key().to_vec(), iterator.value().to_vec()));
        iterator.next().await.expect("moved");
    }
    iterator.seek_to_last().await.expect("moved");
    while iterator.valid() {
        backward.push((iterator.key().to_vec(), iterator.value().to_vec()));
        iterator.prev().await.expect("moved");
    }
    backward.reverse();
    (page.entries, forward, backward)
}

/// Every prefix, present and absent, a range spanning prefixes and an unbounded read.
fn read_options() -> Vec<ReadOptions> {
    let mut all: Vec<ReadOptions> = (0..PREFIXES + 4)
        .map(|index| ReadOptions::default().with_prefix(&prefix(index)))
        .collect();
    all.push(ReadOptions::default().with_prefix(&key(4, 1)[..6]));
    all.push(ReadOptions {
        lower_bound: Some(prefix(3)),
        upper_bound: Some(prefix(9)),
        ..ReadOptions::default()
    });
    all.push(ReadOptions::default().with_prefix(b"p"));
    all.push(ReadOptions::default());
    all
}

/// Block reads of storage made by `run`.
async fn block_reads(storage: &CountedStorage, run: impl AsyncFnOnce()) -> u64 {
    // Relaxed: read on this task before and after the reads it counts.
    let before = storage.reads.blocking.load(Ordering::Relaxed);
    run().await;
    storage.reads.blocking.load(Ordering::Relaxed) - before
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prefix_filtered_reads_read_what_unfiltered_reads_read() {
    for compact in [false, true] {
        let (plain, _) = filled(false, compact).await;
        let (filtered, _) = filled(true, compact).await;
        for options in read_options() {
            let (expected, actual) = (
                reads(&plain, &options).await,
                reads(&filtered, &options).await,
            );
            assert_eq!(actual, expected, "compact {compact}, {options:?}");
        }
        plain.close().await.expect("closed");
        filtered.close().await.expect("closed");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_read_inside_an_absent_prefix_passes_over_the_tables() {
    for compact in [false, true] {
        let (plain, plain_storage) = filled(false, compact).await;
        let (filtered, filtered_storage) = filled(true, compact).await;
        // Odd prefixes are in no table.
        let absent = async |db: &Db| {
            for index in (1..PREFIXES).step_by(2) {
                reads(db, &ReadOptions::default().with_prefix(&prefix(index))).await;
            }
        };
        let unfiltered = block_reads(&plain_storage, async || absent(&plain).await).await;
        let skipped = block_reads(&filtered_storage, async || absent(&filtered).await).await;
        assert!(
            unfiltered > 0,
            "the control read no block: compact {compact}"
        );
        assert_eq!(
            skipped, 0,
            "compact {compact}: {skipped} of {unfiltered} block reads left"
        );
        plain.close().await.expect("closed");
        filtered.close().await.expect("closed");
    }
}
