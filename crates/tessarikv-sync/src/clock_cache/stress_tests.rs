//! Real-parallelism checks with more threads than cores: every value read is one inserted for
//! that key by an insert that began before the read returned, the charge stays bounded while
//! threads race, the count is exact once they stop, and every value is dropped exactly once.
//! `TESSARIKV_STRESS_MILLIS` sets the recording time (default 300 ms).
//!
//! The workers are plain threads on purpose: the point is preemption at arbitrary instructions
//! with more threads than cores, which a work-stealing pool would avoid.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use super::{Admission, ClockCache};

/// A value naming the key and the insert it was offered by, counting its drops.
struct Value {
    key: u16,
    id: u64,
    drops: Arc<AtomicU64>,
}

impl Drop for Value {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::Relaxed);
    }
}

/// What the cache holds: a shared value whose clone dawdles before taking its reference, so a
/// slot emptied under a reader frees the value mid-clone and shows up as a second drop.
struct Slow(Arc<Value>);

impl Clone for Slow {
    fn clone(&self) -> Self {
        for _ in 0..64 {
            std::hint::spin_loop();
        }
        Self(Arc::clone(&self.0))
    }
}

/// An insert as recorded: its key and the time it began.
struct Offered {
    key: u16,
    invoked: Instant,
}

/// A hit as recorded: the key asked for, the value's own key and id, and when the read returned.
struct Hit {
    key: u16,
    value_key: u16,
    id: u64,
    returned: Instant,
}

const CAPACITY: u64 = 256;
const MAX_CHARGE: u64 = 8;

fn duration() -> Duration {
    let millis = std::env::var("TESSARIKV_STRESS_MILLIS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(300);
    Duration::from_millis(millis)
}

type Recorded = (HashMap<u64, Offered>, Vec<Hit>, u64);

/// One worker: a random mix of reads and inserts over a key space larger than the cache holds.
fn work(
    thread: u64,
    cache: &ClockCache<u16, Slow>,
    drops: &Arc<AtomicU64>,
    until: Instant,
) -> Recorded {
    let mut state = thread.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    let (mut offered, mut hits, mut created) = (HashMap::new(), Vec::new(), 0);
    while Instant::now() < until {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let key = u16::try_from(state % 1024).unwrap();
        if state & 0x30_0000 == 0 {
            let id = thread << 40 | created;
            created += 1;
            let value = Slow(Arc::new(Value {
                key,
                id,
                drops: Arc::clone(drops),
            }));
            let invoked = Instant::now();
            let charge = 1 + (state >> 24) % MAX_CHARGE;
            let admission = cache.insert(key, value, charge);
            assert_ne!(
                admission,
                Admission::Refused,
                "nothing pins every entry here"
            );
            offered.insert(id, Offered { key, invoked });
        } else if let Some(Slow(value)) = cache.get(&key) {
            hits.push(Hit {
                key,
                value_key: value.key,
                id: value.id,
                returned: Instant::now(),
            });
        }
    }
    (offered, hits, created)
}

#[test]
fn racing_readers_and_inserters_keep_every_promise() {
    let cores = std::thread::available_parallelism().map_or(4, |count| count.get());
    let threads = u64::try_from(cores * 2).unwrap();
    let cache = Arc::new(ClockCache::new(CAPACITY, 4));
    let drops = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let barrier = Arc::new(Barrier::new(usize::try_from(threads).unwrap() + 1));
    let until = Instant::now() + duration();
    let workers: Vec<_> = (0..threads)
        .map(|thread| {
            let (cache, drops) = (Arc::clone(&cache), Arc::clone(&drops));
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                work(thread, &cache, &drops, until)
            })
        })
        .collect();
    let monitor = {
        let (cache, stop) = (Arc::clone(&cache), Arc::clone(&stop));
        std::thread::spawn(move || {
            let mut highest = 0;
            while !stop.load(Ordering::Relaxed) {
                highest = highest.max(cache.used());
                std::thread::yield_now();
            }
            highest
        })
    };
    barrier.wait();
    let recorded: Vec<Recorded> = workers.into_iter().map(|w| w.join().unwrap()).collect();
    stop.store(true, Ordering::Relaxed);
    let highest = monitor.join().unwrap();
    assert!(
        highest <= CAPACITY + threads * MAX_CHARGE,
        "{highest} counted while racing"
    );
    let held: u64 = cache.entries().iter().map(|(_, charge)| charge).sum();
    assert_eq!(cache.used(), held, "the count is exact once threads stop");
    assert!(held <= CAPACITY);

    let mut offered = HashMap::new();
    let (mut hits, mut created) = (Vec::new(), 0);
    for (thread_offered, thread_hits, thread_created) in recorded {
        offered.extend(thread_offered);
        hits.extend(thread_hits);
        created += thread_created;
    }
    assert!(!hits.is_empty(), "no read ever hit");
    for hit in &hits {
        assert_eq!(
            hit.value_key, hit.key,
            "a read returned another key's value"
        );
        let insert = offered
            .get(&hit.id)
            .expect("a read returned a value never offered");
        assert_eq!(insert.key, hit.key);
        assert!(
            insert.invoked <= hit.returned,
            "a value read before it was offered"
        );
    }
    drop(Arc::into_inner(cache).expect("every worker has finished"));
    assert_eq!(
        drops.load(Ordering::Relaxed),
        created,
        "every value dropped exactly once"
    );
}
