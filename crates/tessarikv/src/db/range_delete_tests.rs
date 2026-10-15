//! Range deletes against a model: point reads, scans and iterators in both directions, at every
//! snapshot, across in-memory tables and table files.

use std::collections::BTreeMap;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

use proptest::prelude::*;
use tessarikv_types::{KeyspaceId, SequenceNumber, ValueKind};

use super::iterator::DbIterator;
use super::read::{get_in, scan_in};
use super::read_tests::{file, user_key};
use super::view::View;
use crate::ReadOptions;
use crate::memtable::Memtable;

/// One write: a put, a delete, or a range delete `[from, to)`.
#[derive(Debug, Clone)]
enum Write {
    Put(u8, u16),
    Delete(u8),
    DeleteRange(u8, u8),
}

fn write() -> impl Strategy<Value = Write> {
    prop_oneof![
        3 => (0_u8..12, any::<u16>()).prop_map(|(key, value)| Write::Put(key, value)),
        1 => (0_u8..12).prop_map(Write::Delete),
        1 => (0_u8..12, 1_u8..6).prop_map(|(from, width)| Write::DeleteRange(from, from + width)),
    ]
}

/// Writes `writes` into `table`, the first at sequence `first`.
fn fill(table: &Memtable, first: usize, writes: &[Write]) {
    for (offset, write) in writes.iter().enumerate() {
        let sequence = SequenceNumber::new(u64::try_from(first + offset).unwrap()).unwrap();
        match write {
            Write::Put(key, value) => table
                .insert(
                    sequence,
                    ValueKind::Value,
                    &user_key(*key),
                    &value.to_le_bytes(),
                )
                .unwrap(),
            Write::Delete(key) => table
                .insert(sequence, ValueKind::Delete, &user_key(*key), b"")
                .unwrap(),
            Write::DeleteRange(from, to) => table
                .insert(
                    sequence,
                    ValueKind::RangeDelete,
                    &user_key(*from),
                    &user_key(*to),
                )
                .unwrap(),
        }
    }
}

/// What every key holds once the first `count` writes are applied.
fn model(writes: &[Write], count: usize) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let mut state = BTreeMap::new();
    for write in writes.iter().take(count) {
        match write {
            Write::Put(key, value) => {
                state.insert(user_key(*key).to_vec(), value.to_le_bytes().to_vec());
            }
            Write::Delete(key) => {
                state.remove(user_key(*key).as_slice());
            }
            Write::DeleteRange(from, to) => {
                state.retain(|key: &Vec<u8>, _| {
                    key.as_slice() < user_key(*from).as_slice()
                        || key.as_slice() >= user_key(*to).as_slice()
                });
            }
        }
    }
    state
}

/// Three in-memory tables holding `writes` split at `first` and `second`, the oldest `flushed`
/// of them moved to table files.
fn view(writes: &[Write], first: usize, second: usize, flushed: usize) -> View {
    let mut view = View::new(0, Memtable::new());
    fill(view.table(0).unwrap(), 1, &writes[..first]);
    view = view.with_next_table();
    fill(view.table(1).unwrap(), first + 1, &writes[first..second]);
    view = view.with_next_table();
    fill(view.table(2).unwrap(), second + 1, &writes[second..]);
    for epoch in 0..u8::try_from(flushed).unwrap() {
        let table = file(view.table(epoch).unwrap(), u64::from(epoch) + 1);
        view = view.after_flush(epoch, Some(table)).unwrap();
    }
    view
}

/// Runs a future that must not wait.
fn run<T>(future: impl Future<Output = T>) -> T {
    let mut future = pin!(future);
    match future
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("an iterator over memory waited"),
    }
}

proptest! {
    #[test]
    fn range_deletes_hide_exactly_the_older_versions_in_their_range(
        writes in proptest::collection::vec(write(), 3..40),
        cuts in (any::<prop::sample::Index>(), any::<prop::sample::Index>()),
        flushed in 0_usize..=2,
    ) {
        let first = 1 + cuts.0.index(writes.len() - 2);
        let second = first + 1 + cuts.1.index(writes.len() - first - 1);
        let view = view(&writes, first, second, flushed);
        let default = KeyspaceId::DEFAULT;
        for snapshot in 0..=writes.len() {
            let at = SequenceNumber::new(u64::try_from(snapshot).unwrap()).unwrap();
            let state = model(&writes, snapshot);
            let listed: Vec<_> = state.clone().into_iter().collect();
            for key in 0_u8..13 {
                let key = user_key(key);
                prop_assert_eq!(
                    get_in(&view, default, &key, at).unwrap(),
                    state.get(key.as_slice()).cloned(),
                    "key {:?} at {}", key, snapshot
                );
            }
            prop_assert_eq!(&scan_in(&view, default, b"", None, usize::MAX, at).unwrap().entries, &listed);
            let options = ReadOptions::default();
            let mut iterator = DbIterator::new(&view, default, at, &options, None).unwrap();
            let (forward, backward) = run(async {
                let mut forward = Vec::new();
                iterator.seek_to_first().await.unwrap();
                while iterator.valid() && forward.len() <= listed.len() {
                    forward.push((iterator.key().to_vec(), iterator.value().to_vec()));
                    iterator.next().await.unwrap();
                }
                let mut backward = Vec::new();
                iterator.seek_to_last().await.unwrap();
                while iterator.valid() && backward.len() <= listed.len() {
                    backward.push((iterator.key().to_vec(), iterator.value().to_vec()));
                    iterator.prev().await.unwrap();
                }
                (forward, backward)
            });
            prop_assert_eq!(&forward, &listed);
            prop_assert_eq!(backward, listed.iter().rev().cloned().collect::<Vec<_>>());
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_range_delete_survives_reopening_from_the_log_and_from_table_files() {
    use std::path::Path;
    use std::sync::Arc;

    use super::Db;
    use super::flush_tests::{PATH, key, until, value};
    use crate::log::fault::FaultStorage;
    use crate::storage::Storage as _;
    use crate::{DbOptions, KeyspaceOptions, WriteBatch, WriteOptions};

    let keyspace = KeyspaceOptions::builder()
        .memtable_bytes(64 * 1024)
        .build()
        .unwrap();
    let options = DbOptions::builder()
        .default_keyspace(keyspace)
        .build()
        .unwrap();
    let open = |storage: &FaultStorage| {
        Db::open_with_storage(PATH, options.clone(), Arc::new(storage.clone()))
    };
    let put = |from: u32, to: u32| {
        let mut batch = WriteBatch::new();
        for index in from..to {
            batch
                .put(KeyspaceId::DEFAULT, &key(index), &value(index))
                .unwrap();
        }
        batch
    };
    // `later`: whether the writes after the range delete are expected (they may be in no table).
    let check = |db: Db, later: bool| async move {
        let later_keys = if later { 100..140 } else { 0..0 };
        for index in (0..40).chain(later_keys) {
            let read = db
                .get(KeyspaceId::DEFAULT, &key(index), &ReadOptions::default())
                .await
                .unwrap();
            let deleted = (5..15).contains(&index);
            assert_eq!(read.is_none(), deleted, "key {index}");
        }
        db.close().await.unwrap();
    };

    let storage = FaultStorage::default();
    let db = open(&storage).await.unwrap();
    for start in (0..40).step_by(10) {
        db.write(&put(start, start + 10), &WriteOptions::default())
            .await
            .unwrap();
    }
    let mut batch = WriteBatch::new();
    batch
        .delete_range(KeyspaceId::DEFAULT, &key(5), &key(15))
        .unwrap();
    let deleted_at = db.write(&batch, &WriteOptions::default()).await.unwrap();
    // Enough later writes that the table holding the range delete is flushed.
    for start in (100..140).step_by(10) {
        db.write(&put(start, start + 10), &WriteOptions::default())
            .await
            .unwrap();
    }
    until("the range delete's table is flushed", || {
        db.view
            .load()
            .files(0)
            .any(|file| file.meta.max_sequence >= deleted_at.get())
    })
    .await;
    db.close().await.unwrap();
    check(open(&storage).await.unwrap(), true).await;

    // Without the log, the range delete can only come from a table file. Tables install oldest
    // first, so every write before it is in a table file too.
    for name in storage.list(Path::new(PATH)).unwrap() {
        if name.ends_with(".log") {
            storage.remove(&Path::new(PATH).join(name)).unwrap();
        }
    }
    check(open(&storage).await.unwrap(), false).await;
}
