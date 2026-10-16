use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use proptest::prelude::*;

use super::{Admission, ClockCache};

/// A value that counts its drops, so a test can see every value dropped exactly once.
#[derive(Debug)]
struct Counted {
    id: u64,
    drops: Arc<AtomicU64>,
}

impl Drop for Counted {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
fn an_admitted_entry_is_read_back() {
    let cache = ClockCache::new(1000, 10);
    assert_eq!(cache.insert(7_u32, "seven", 10), Admission::Admitted);
    assert_eq!(cache.get(&7), Some("seven"));
    assert_eq!(cache.get(&8), None);
    assert_eq!(cache.insert(7, "again", 10), Admission::AlreadyCached);
    assert_eq!(cache.get(&7), Some("seven"));
    assert_eq!(cache.used(), 10);
}

#[test]
fn a_cache_of_no_bytes_keeps_nothing_and_an_oversized_entry_is_refused() {
    let empty = ClockCache::new(0, 10);
    assert_eq!(empty.insert(1_u32, 1_u32, 0), Admission::Refused);
    assert_eq!(empty.get(&1), None);
    let small = ClockCache::new(100, 10);
    assert_eq!(small.insert(1_u32, 1_u32, 101), Admission::Refused);
    assert_eq!(small.used(), 0);
    assert_eq!(small.insert(2, 2, 100), Admission::Admitted);
    assert_eq!(small.get(&2), Some(2));
}

#[test]
fn an_entry_read_since_the_last_sweep_outlives_one_that_was_not() {
    for _ in 0..50 {
        // Room for two entries: the third evicts the one not read since it was inserted.
        let cache = ClockCache::new(20, 10);
        assert_eq!(cache.insert(1_u32, 1_u32, 10), Admission::Admitted);
        assert_eq!(cache.insert(2, 2, 10), Admission::Admitted);
        assert_eq!(cache.get(&1), Some(1));
        assert_eq!(cache.insert(3, 3, 10), Admission::Admitted);
        assert_eq!(cache.get(&1), Some(1), "the entry read was evicted");
        assert_eq!(cache.get(&2), None, "the entry not read was kept");
        assert_eq!(cache.get(&3), Some(3));
    }
}

#[test]
fn every_value_is_dropped_exactly_once_and_a_reader_keeps_its_clone() {
    let drops = Arc::new(AtomicU64::new(0));
    let cache = ClockCache::new(30, 10);
    let mut created = 0;
    let mut offer = |id: u64| {
        created += 1;
        let value = Arc::new(Counted {
            id,
            drops: Arc::clone(&drops),
        });
        cache.insert(id, value, 10);
    };
    (0..20).for_each(&mut offer);
    let held = cache.get(&19).unwrap();
    (20..40).for_each(&mut offer);
    assert_eq!(held.id, 19, "a value read must survive its eviction");
    drop(cache);
    assert_eq!(drops.load(Ordering::Relaxed), created - 1);
    drop(held);
    assert_eq!(drops.load(Ordering::Relaxed), created);
}

#[derive(Debug, Clone)]
enum Step {
    Get(u8),
    Insert(u8, u64),
}

fn steps() -> impl Strategy<Value = Vec<Step>> {
    let step = prop_oneof![
        (0_u8..24).prop_map(Step::Get),
        (0_u8..24, 1_u64..60).prop_map(|(key, charge)| Step::Insert(key, charge)),
    ];
    proptest::collection::vec(step, 1..200)
}

proptest! {
    /// Against a model of what was ever inserted: no wrong value, the charge bound, exact charge
    /// accounting, and in one thread every insert that fits is admitted and read back.
    #[test]
    fn a_single_thread_sees_the_sequential_specification(
        steps in steps(),
        capacity in prop_oneof![Just(0_u64), Just(1), Just(60), Just(200), Just(5000)],
        typical in 1_u64..64,
    ) {
        let cache = ClockCache::new(capacity, typical);
        let mut inserted: BTreeMap<u8, BTreeSet<u64>> = BTreeMap::new();
        for (version, step) in (0_u64..).zip(&steps) {
            let held: BTreeMap<u8, u64> = cache.entries().into_iter().collect();
            match *step {
                Step::Get(key) => {
                    let got = cache.get(&key);
                    if let Some(value) = got {
                        prop_assert!(inserted.get(&key).is_some_and(|values| values.contains(&value)));
                    }
                    prop_assert_eq!(got.is_some(), held.contains_key(&key));
                }
                Step::Insert(key, charge) => {
                    let admission = cache.insert(key, version, charge);
                    let expected = if held.contains_key(&key) {
                        Admission::AlreadyCached
                    } else if capacity == 0 || charge > capacity {
                        Admission::Refused
                    } else {
                        Admission::Admitted
                    };
                    prop_assert_eq!(admission, expected);
                    if admission == Admission::Admitted {
                        inserted.entry(key).or_default().insert(version);
                        prop_assert_eq!(cache.get(&key), Some(version));
                    }
                }
            }
            let entries = cache.entries();
            let total: u64 = entries.iter().map(|(_, charge)| charge).sum();
            prop_assert_eq!(cache.used(), total);
            prop_assert!(total <= capacity);
            let keys: BTreeSet<u8> = entries.iter().map(|(key, _)| *key).collect();
            prop_assert_eq!(keys.len(), entries.len(), "one thread never leaves two copies");
        }
    }
}
