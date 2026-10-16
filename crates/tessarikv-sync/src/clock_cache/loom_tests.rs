use loom::sync::Arc;

use super::{Admission, ClockCache};

/// The charge of every entry in these models: one entry fills a cache of `ONE`.
const ONE: u64 = 10;

fn cache(capacity: u64) -> Arc<ClockCache<u8, u64>> {
    Arc::new(ClockCache::new(capacity, ONE))
}

/// The charge counted equals what the slots hold, and stays within the capacity.
fn assert_counted(cache: &ClockCache<u8, u64>, capacity: u64) {
    let held: u64 = cache.entries().iter().map(|(_, charge)| charge).sum();
    assert_eq!(cache.used(), held);
    assert!(
        held <= capacity,
        "{held} held over a capacity of {capacity}"
    );
}

#[test]
fn a_read_racing_an_insert_sees_nothing_or_the_inserted_value() {
    loom::model(|| {
        let cache = cache(ONE);
        let writer = Arc::clone(&cache);
        let insert = loom::thread::spawn(move || writer.insert(1, 7, ONE));
        let read = cache.get(&1);
        assert!(matches!(read, None | Some(7)), "read {read:?}");
        assert_eq!(insert.join().unwrap(), Admission::Admitted);
        assert_eq!(cache.get(&1), Some(7));
        assert_counted(&cache, ONE);
    });
}

#[test]
fn an_eviction_never_takes_an_entry_from_under_a_reader() {
    loom::model(|| {
        let cache = cache(ONE);
        assert_eq!(cache.insert(1, 7, ONE), Admission::Admitted);
        let writer = Arc::clone(&cache);
        let insert = loom::thread::spawn(move || writer.insert(2, 8, ONE));
        let read = cache.get(&1);
        assert!(matches!(read, None | Some(7)), "read {read:?}");
        let admission = insert.join().unwrap();
        assert!(matches!(
            admission,
            Admission::Admitted | Admission::Refused
        ));
        if admission == Admission::Admitted {
            assert_eq!(cache.get(&2), Some(8));
        }
        assert_counted(&cache, ONE);
    });
}

#[test]
fn two_inserts_of_one_key_leave_it_readable_and_counted() {
    loom::model(|| {
        let cache = cache(2 * ONE);
        let writer = Arc::clone(&cache);
        let other = loom::thread::spawn(move || writer.insert(1, 7, ONE));
        let mine = cache.insert(1, 7, ONE);
        let theirs = other.join().unwrap();
        for admission in [mine, theirs] {
            assert!(matches!(
                admission,
                Admission::Admitted | Admission::AlreadyCached
            ));
        }
        assert_eq!(cache.get(&1), Some(7));
        assert_counted(&cache, 2 * ONE);
    });
}

#[test]
fn two_inserts_competing_for_room_keep_the_count_exact() {
    loom::model(|| {
        let cache = cache(ONE);
        assert_eq!(cache.insert(1, 7, ONE), Admission::Admitted);
        let writer = Arc::clone(&cache);
        let other = loom::thread::spawn(move || writer.insert(2, 8, ONE));
        let mine = cache.insert(3, 9, ONE);
        let theirs = other.join().unwrap();
        for (key, value, admission) in [(3, 9, mine), (2, 8, theirs)] {
            assert!(matches!(
                admission,
                Admission::Admitted | Admission::Refused
            ));
            if let Some(read) = cache.get(&key) {
                assert_eq!(read, value);
            }
        }
        assert_counted(&cache, ONE);
    });
}
