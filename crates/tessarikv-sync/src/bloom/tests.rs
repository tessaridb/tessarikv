use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};

use super::ConcurrentBloom;

/// A well-spread 64-bit hash of `n`, as a key hash would be.
fn hash(n: u64) -> u64 {
    let mut value = n.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

#[test]
fn an_inserted_hash_is_always_reported_present() {
    let bloom = ConcurrentBloom::new(64 * 1024);
    for n in 0..20_000 {
        bloom.insert(hash(n));
    }
    assert!((0..20_000).all(|n| bloom.may_contain(hash(n))));
}

#[test]
fn an_empty_filter_reports_nothing_present() {
    let bloom = ConcurrentBloom::new(4096);
    assert!((0..1000).all(|n| !bloom.may_contain(hash(n))));
}

#[test]
fn absent_hashes_pass_rarely_at_the_planned_density() {
    // 26 bits per key, as an 8 MiB table of 200-byte entries gets. A 512-bit line holding a
    // Poisson-distributed share of the keys passes an absent hash with probability
    // sum over n of P(n) * (1 - (1 - 1/512)^(6n))^6, about 1.3e-4 here: some 13 of 100 000.
    let keys = 10_000_u64;
    let bloom = ConcurrentBloom::new(26 * 10_000);
    for n in 0..keys {
        bloom.insert(hash(n));
    }
    let passed = (keys..keys + 100_000)
        .filter(|n| bloom.may_contain(hash(*n)))
        .count();
    assert!(passed < 40, "{passed} of 100000 absent hashes passed");
}

#[test]
fn a_filter_smaller_than_a_line_still_has_one() {
    let bloom = ConcurrentBloom::new(0);
    bloom.insert(hash(7));
    assert!(bloom.may_contain(hash(7)));
}

#[test]
fn concurrent_inserts_are_all_seen_once_published() {
    let threads = 4_u64;
    let per_thread = 20_000_u64;
    let bloom = Arc::new(ConcurrentBloom::new(256 * 1024));
    let start = Arc::new(Barrier::new(usize::try_from(threads).unwrap()));
    let workers: Vec<_> = (0..threads)
        .map(|thread| {
            let (bloom, start) = (Arc::clone(&bloom), Arc::clone(&start));
            std::thread::spawn(move || {
                start.wait();
                for n in 0..per_thread {
                    bloom.insert(hash(thread * per_thread + n));
                }
            })
        })
        .collect();
    for worker in workers {
        worker.join().unwrap();
    }
    assert!((0..threads * per_thread).all(|n| bloom.may_contain(hash(n))));
}

#[test]
fn a_reader_that_sees_a_publication_sees_the_hashes_inserted_before_it() {
    let bloom = Arc::new(ConcurrentBloom::new(64 * 1024));
    let published = Arc::new(AtomicBool::new(false));
    let writer = {
        let (bloom, published) = (Arc::clone(&bloom), Arc::clone(&published));
        std::thread::spawn(move || {
            for n in 0..10_000 {
                bloom.insert(hash(n));
            }
            published.store(true, Ordering::Release);
        })
    };
    let mut checked = false;
    while !checked {
        if published.load(Ordering::Acquire) {
            assert!((0..10_000).all(|n| bloom.may_contain(hash(n))));
            checked = true;
        } else {
            std::hint::spin_loop();
        }
    }
    writer.join().unwrap();
}
