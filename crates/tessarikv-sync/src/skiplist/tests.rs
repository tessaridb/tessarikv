use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use super::{BytewiseComparator, Insert, KeyComparator, SkipList};

fn list() -> SkipList<BytewiseComparator> {
    SkipList::with_chunk_sizes(BytewiseComparator, 256, 64).unwrap()
}

fn forward(list: &SkipList<impl KeyComparator>) -> Vec<Vec<u8>> {
    let mut cursor = list.cursor();
    cursor.seek_to_first();
    let mut keys = Vec::new();
    while cursor.valid() {
        keys.push(cursor.key().to_vec());
        cursor.next();
    }
    keys
}

#[test]
fn an_empty_list_finds_nothing() {
    let list = list();
    assert!(list.get(b"a").is_none());
    let mut cursor = list.cursor();
    cursor.seek_to_first();
    assert!(!cursor.valid());
    cursor.seek_to_last();
    assert!(!cursor.valid());
}

#[test]
fn inserted_entries_are_found_with_their_values() {
    let list = list();
    assert_eq!(list.insert(b"b", b"2").unwrap(), Insert::Inserted);
    assert_eq!(list.insert(b"a", b"1").unwrap(), Insert::Inserted);
    assert_eq!(list.insert(b"c", b"").unwrap(), Insert::Inserted);
    assert_eq!(list.get(b"a"), Some(&b"1"[..]));
    assert_eq!(list.get(b"b"), Some(&b"2"[..]));
    assert_eq!(list.get(b"c"), Some(&b""[..]));
    assert!(list.get(b"bb").is_none());
}

#[test]
fn an_equal_key_is_refused_and_keeps_the_first_value() {
    let list = list();
    assert_eq!(list.insert(b"k", b"first").unwrap(), Insert::Inserted);
    assert_eq!(
        list.insert(b"k", b"second").unwrap(),
        Insert::AlreadyPresent
    );
    assert_eq!(list.get(b"k"), Some(&b"first"[..]));
    assert_eq!(forward(&list), vec![b"k".to_vec()]);
}

#[test]
fn iteration_is_sorted_both_ways_for_every_height() {
    let list = list();
    let keys: Vec<Vec<u8>> = (0..300_u32)
        .map(|n| (n * 7919 % 1000).to_be_bytes().to_vec())
        .collect();
    for (index, key) in keys.iter().enumerate() {
        list.insert_with_height(key, b"v", index % 12 + 1).unwrap();
    }
    let expected: Vec<Vec<u8>> = keys
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    assert_eq!(forward(&list), expected);

    let mut cursor = list.cursor();
    cursor.seek_to_last();
    let mut backward = Vec::new();
    while cursor.valid() {
        backward.push(cursor.key().to_vec());
        cursor.prev();
    }
    backward.reverse();
    assert_eq!(backward, expected);
}

#[test]
fn seek_lands_on_the_least_key_not_less_than_the_target() {
    let list = list();
    for key in [b"b", b"d", b"f"] {
        list.insert(key, b"v").unwrap();
    }
    let mut cursor = list.cursor();
    for (target, expected) in [
        (&b"a"[..], Some(&b"b"[..])),
        (b"b", Some(b"b")),
        (b"c", Some(b"d")),
        (b"f", Some(b"f")),
        (b"g", None),
    ] {
        cursor.seek(target);
        assert_eq!(
            cursor.valid().then(|| cursor.key()),
            expected,
            "seek {target:?}"
        );
    }
}

#[test]
fn a_custom_comparator_orders_the_list() {
    struct Reverse;
    impl KeyComparator for Reverse {
        fn compare(&self, a: &[u8], b: &[u8]) -> std::cmp::Ordering {
            b.cmp(a)
        }
    }
    let list = SkipList::new(Reverse);
    for key in [b"a", b"c", b"b"] {
        list.insert(key, b"").unwrap();
    }
    assert_eq!(
        forward(&list),
        vec![b"c".to_vec(), b"b".to_vec(), b"a".to_vec()]
    );
}

#[test]
fn large_keys_and_values_round_trip() {
    let list = list();
    let key = vec![7_u8; 5000];
    let value = vec![9_u8; 100_000];
    list.insert(&key, &value).unwrap();
    list.insert(b"small", b"x").unwrap();
    assert_eq!(list.get(&key), Some(&value[..]));
    assert_eq!(list.get(b"small"), Some(&b"x"[..]));
}

#[test]
fn concurrent_inserters_lose_nothing_and_duplicate_nothing() {
    let list = Arc::new(list());
    let threads: Vec<_> = (0..8_u32)
        .map(|thread| {
            let list = Arc::clone(&list);
            std::thread::spawn(move || {
                let mut won = Vec::new();
                let rounds = if cfg!(miri) { 40 } else { 2000_u32 };
                for n in 0..rounds {
                    // Half the keys are shared between pairs of threads, half are private.
                    let key = if n % 2 == 0 {
                        (n * 31 + thread / 2).to_be_bytes()
                    } else {
                        (1_000_000 + n * 8 + thread).to_be_bytes()
                    };
                    if list.insert(&key, &thread.to_be_bytes()).unwrap() == Insert::Inserted {
                        won.push((key.to_vec(), thread));
                    }
                }
                won
            })
        })
        .collect();
    let mut model = BTreeMap::new();
    for thread in threads {
        for (key, writer) in thread.join().unwrap() {
            assert!(
                model.insert(key, writer).is_none(),
                "a key was inserted twice"
            );
        }
    }
    assert_eq!(forward(&list), model.keys().cloned().collect::<Vec<_>>());
    for (key, writer) in &model {
        assert_eq!(list.get(key), Some(&writer.to_be_bytes()[..]));
    }
}
