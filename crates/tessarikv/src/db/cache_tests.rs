//! Reads through the block cache: table files read on demand with no cache, a cache too small
//! to keep much, and a large one; reads that leave the cache alone; read futures that move
//! between threads; and an iterator move dropped while it waits for storage.

use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::task::{Context, Waker};

use tessarikv_types::{KeyspaceId, SequenceNumber, ValueKind};

use super::Db;
use super::block_cache::BlockCache;
use super::flush_tests::{PATH, key, until, value};
use super::iterator::DbIterator;
use super::read::{get_in, scan_in};
use super::read_tests::{file_in, from, user_key};
use super::view::View;
use crate::log::fault::FaultStorage;
use crate::memtable::Memtable;
use crate::storage::counted::CountedStorage;
use crate::{DbOptions, KeyspaceOptions, ReadOptions, WriteBatch, WriteOptions};

const WRITES: u32 = 300;

fn options(cache_bytes: u64) -> DbOptions {
    let keyspace = KeyspaceOptions::builder()
        .memtable_bytes(64 * 1024)
        .build()
        .unwrap();
    DbOptions::builder()
        .default_keyspace(keyspace)
        .block_cache_bytes(cache_bytes)
        .build()
        .unwrap()
}

/// Every key, read back by point reads, one scan and a forward walk.
async fn assert_all_readable(db: &Db) {
    let options = ReadOptions::default();
    for index in 0..WRITES {
        let read = db.get(KeyspaceId::DEFAULT, &key(index), &options).await;
        assert_eq!(read.unwrap(), Some(value(index)), "key {index}");
    }
    let page = db
        .scan(KeyspaceId::DEFAULT, b"", None, usize::MAX, &options)
        .await
        .unwrap();
    let expected: Vec<_> = (0..WRITES)
        .map(|index| (key(index), value(index)))
        .collect();
    assert_eq!(page.entries, expected);
    let mut iterator = db.iter(KeyspaceId::DEFAULT, &options).unwrap();
    iterator.seek_to_first().await.unwrap();
    let mut walked = 0;
    while iterator.valid() {
        assert_eq!(iterator.key(), key(walked).as_slice());
        assert_eq!(iterator.value(), value(walked).as_slice());
        walked += 1;
        iterator.next().await.unwrap();
    }
    assert_eq!(walked, WRITES);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn table_files_answer_with_no_cache_a_tiny_one_and_a_large_one() {
    for cache_bytes in [0, 8 * 1024, 64 << 20] {
        let storage = FaultStorage::default();
        let open = || Db::open_with_storage(PATH, options(cache_bytes), Arc::new(storage.clone()));
        let db = open().await.unwrap();
        for index in 0..WRITES {
            let mut batch = WriteBatch::new();
            batch
                .put(KeyspaceId::DEFAULT, &key(index), &value(index))
                .unwrap();
            db.write(&batch, &WriteOptions::default()).await.unwrap();
        }
        until("two table files", || db.view.load().files(0).count() >= 2).await;
        assert_all_readable(&db).await;
        db.close().await.unwrap();
        // Reopened, the table files are opened from storage and read through the cache again.
        let db = open().await.unwrap();
        assert!(db.view.load().files(0).count() >= 2);
        assert_all_readable(&db).await;
        db.close().await.unwrap();
    }
}

/// A view of 40 keys flushed to one table file read through `cache`.
fn flushed(cache: &Arc<BlockCache>) -> View {
    let table = Memtable::new();
    for index in 0..40_u8 {
        let sequence = SequenceNumber::new(u64::from(index) + 1).unwrap();
        table
            .insert(sequence, ValueKind::Value, &user_key(index), &[index; 30])
            .unwrap();
    }
    // Storage that never reads without blocking, so a block not in the cache makes a move wait.
    let file = file_in(&CountedStorage::new(false), &table, 1, cache);
    View::new(0, Memtable::new())
        .with_next_table()
        .after_flush(0, vec![file])
        .unwrap()
}

#[tokio::test]
async fn a_read_that_asks_not_to_fill_the_cache_leaves_it_as_it_was() {
    let cache = Arc::new(BlockCache::new(
        1 << 20,
        tessarikv_constants::DEFAULT_BLOCK_BYTES,
    ));
    let view = flushed(&cache);
    let (default, at) = (KeyspaceId::DEFAULT, SequenceNumber::new(100).unwrap());
    let options = ReadOptions {
        fill_cache: false,
        ..ReadOptions::default()
    };
    let read = get_in(&view, default, &user_key(7), at, false).await;
    assert_eq!(read.unwrap(), Some(vec![7; 30]));
    let page = scan_in(&view, default, from(b"", usize::MAX), at, false).await;
    assert_eq!(page.unwrap().entries.len(), 40);
    let mut iterator = DbIterator::new(&view, default, at, &options, None).unwrap();
    iterator.seek_to_first().await.unwrap();
    while iterator.valid() {
        iterator.next().await.unwrap();
    }
    assert_eq!(cache.used_bytes(), 0);
    // The same reads that may fill it do.
    get_in(&view, default, &user_key(7), at, true)
        .await
        .unwrap();
    assert!(cache.used_bytes() > 0);
}

fn assert_send<T: Send>(value: T) -> T {
    value
}

#[tokio::test]
async fn read_futures_can_move_between_threads() {
    let db = Db::open("cache-send", DbOptions::in_memory())
        .await
        .unwrap();
    let options = ReadOptions::default();
    assert_send(db.get(KeyspaceId::DEFAULT, b"k", &options))
        .await
        .unwrap();
    assert_send(db.scan(KeyspaceId::DEFAULT, b"", None, 10, &options))
        .await
        .unwrap();
    let mut iterator = db.iter(KeyspaceId::DEFAULT, &options).unwrap();
    assert_send(iterator.seek_to_first()).await.unwrap();
    assert_send(iterator.next()).await.unwrap();
    assert_send(iterator.prev()).await.unwrap();
    drop(iterator);
    db.close().await.unwrap();
}

// One thread: only there does a block read that must block still suspend the move (a worker of
// a multi-thread runtime reads it in place).
#[tokio::test]
async fn a_move_dropped_while_it_waits_for_storage_leaves_the_iterator_not_valid() {
    // No cache: every block a move reaches is read from storage.
    let cache = Arc::new(BlockCache::new(0, tessarikv_constants::DEFAULT_BLOCK_BYTES));
    let view = flushed(&cache);
    let at = SequenceNumber::new(100).unwrap();
    let options = ReadOptions::default();
    let mut iterator = DbIterator::new(&view, KeyspaceId::DEFAULT, at, &options, None).unwrap();
    let mut abandoned = false;
    for _ in 0..100 {
        iterator.seek_to_first().await.unwrap();
        assert!(iterator.valid());
        // The last key is in another block: the move waits for it, and is dropped waiting.
        let mut last = pin!(iterator.seek_to_last());
        if last
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
        {
            abandoned = true;
            break;
        }
    }
    assert!(abandoned, "every move finished without waiting");
    assert!(!iterator.valid());
    assert!(iterator.key().is_empty());
    iterator.seek_to_first().await.unwrap();
    assert_eq!(iterator.key(), user_key(0).as_slice());
    iterator.seek_to_last().await.unwrap();
    assert_eq!(iterator.key(), user_key(39).as_slice());
}

/// Reads of every key after a reopen with no block cache, through `storage`.
async fn reopened_reads(storage: CountedStorage) -> (u64, u64) {
    let db = Db::open_with_storage(PATH, options(1024 * 1024), Arc::new(storage.clone()))
        .await
        .unwrap();
    for index in 0..WRITES {
        let mut batch = WriteBatch::new();
        batch
            .put(KeyspaceId::DEFAULT, &key(index), &value(index))
            .unwrap();
        db.write(&batch, &WriteOptions::default()).await.unwrap();
    }
    until("tables flushed", || db.view.load().files(0).count() > 2).await;
    db.close().await.unwrap();
    let db = Db::open_with_storage(PATH, options(0), Arc::new(storage.clone()))
        .await
        .unwrap();
    // Relaxed: the counters are read on this task, after the reads they count.
    let reads = &storage.reads;
    let inline = reads.inline.load(Ordering::Relaxed);
    let blocking = reads.blocking.load(Ordering::Relaxed);
    assert_all_readable(&db).await;
    let made = (
        reads.inline.load(Ordering::Relaxed) - inline,
        reads.blocking.load(Ordering::Relaxed) - blocking,
    );
    db.close().await.unwrap();
    made
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_block_that_can_be_read_without_blocking_is_read_inline_and_any_other_is_handed_off() {
    let (inline, blocking) = reopened_reads(CountedStorage::new(true)).await;
    assert!(inline > 0, "no block read inline");
    assert_eq!(
        blocking, 0,
        "{blocking} blocks handed off with inline reads available"
    );
    let (inline, blocking) = reopened_reads(CountedStorage::new(false)).await;
    assert_eq!(inline, 0);
    assert!(blocking > 0, "no block read where blocking is allowed");
}

#[tokio::test]
async fn blocks_that_must_be_read_where_blocking_is_allowed_are_read_on_one_thread_too() {
    let (inline, blocking) = reopened_reads(CountedStorage::new(false)).await;
    assert_eq!(inline, 0);
    assert!(blocking > 0, "no block read where blocking is allowed");
}
