//! Loom models of every operation pair at 2-3 threads (`LOOM_MAX_PREEMPTIONS` bounds the search).

use loom::sync::Arc;
use loom::thread;

use super::{BytewiseComparator, Insert, SkipList};

fn list() -> Arc<SkipList<BytewiseComparator>> {
    Arc::new(SkipList::with_chunk_size(BytewiseComparator, 128).unwrap())
}

fn keys(list: &SkipList<BytewiseComparator>) -> Vec<Vec<u8>> {
    let mut cursor = list.cursor();
    cursor.seek_to_first();
    let mut out = Vec::new();
    while cursor.valid() {
        out.push(cursor.copy_key().unwrap());
        cursor.next();
    }
    out
}

/// insert ∥ insert ∥ reader: both keys end up in order; the reader sees a sorted subset whose
/// values are whole.
#[test]
fn two_inserts_and_a_reader() {
    loom::model(|| {
        let list = list();
        let inserters: Vec<_> = [(b"b", 2_usize), (b"a", 1)]
            .into_iter()
            .map(|(key, height)| {
                let list = Arc::clone(&list);
                thread::spawn(move || list.insert_with_height(key, key, height).unwrap())
            })
            .collect();
        let reader = {
            let list = Arc::clone(&list);
            thread::spawn(move || {
                let mut cursor = list.cursor();
                cursor.seek(b"a");
                let mut seen = Vec::new();
                while cursor.valid() {
                    let key = cursor.copy_key().unwrap();
                    assert_eq!(cursor.copy_value().unwrap(), key, "torn entry");
                    seen.push(key);
                    cursor.next();
                }
                seen
            })
        };
        for inserter in inserters {
            assert_eq!(inserter.join().unwrap(), Insert::Inserted);
        }
        let seen = reader.join().unwrap();
        assert!(
            seen.windows(2).all(|pair| pair[0] < pair[1]),
            "reader saw keys out of order"
        );
        assert_eq!(keys(&list), vec![b"a".to_vec(), b"b".to_vec()]);
    });
}

/// insert(k) ∥ insert(k): exactly one wins and its value is the one kept.
#[test]
fn racing_inserts_of_one_key() {
    loom::model(|| {
        let list = list();
        let handles: Vec<_> = [b"1", b"2"]
            .into_iter()
            .map(|value| {
                let list = Arc::clone(&list);
                thread::spawn(move || (list.insert_with_height(b"k", value, 1).unwrap(), value))
            })
            .collect();
        let results: Vec<_> = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect();
        let winners: Vec<_> = results
            .iter()
            .filter(|(outcome, _)| *outcome == Insert::Inserted)
            .collect();
        assert_eq!(winners.len(), 1, "{results:?}");
        let mut cursor = list.cursor();
        cursor.seek(b"k");
        assert_eq!(cursor.copy_value().unwrap(), winners[0].1.to_vec());
        assert_eq!(keys(&list), vec![b"k".to_vec()]);
    });
}

/// Adjacent tall inserts race on the same upper-level link while a reader searches through level 1.
#[test]
fn upper_level_link_races() {
    loom::model(|| {
        let list = list();
        list.insert_with_height(b"m", b"m", 2).unwrap();
        let inserters: Vec<_> = [b"p", b"q"]
            .into_iter()
            .map(|key| {
                let list = Arc::clone(&list);
                thread::spawn(move || list.insert_with_height(key, key, 2).unwrap())
            })
            .collect();
        let reader = {
            let list = Arc::clone(&list);
            thread::spawn(move || {
                let mut cursor = list.cursor();
                cursor.seek(b"q");
                cursor.valid().then(|| cursor.copy_key().unwrap())
            })
        };
        for inserter in inserters {
            assert_eq!(inserter.join().unwrap(), Insert::Inserted);
        }
        if let Some(found) = reader.join().unwrap() {
            assert_eq!(found, b"q".to_vec(), "seek landed on the wrong key");
        }
        assert_eq!(
            keys(&list),
            vec![b"m".to_vec(), b"p".to_vec(), b"q".to_vec()]
        );
        let mut cursor = list.cursor();
        cursor.seek_to_last();
        assert_eq!(cursor.copy_key().unwrap(), b"q".to_vec());
        cursor.prev();
        assert_eq!(cursor.copy_key().unwrap(), b"p".to_vec());
    });
}
