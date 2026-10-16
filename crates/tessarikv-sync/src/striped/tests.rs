use super::{Line, STRIPES, StripedCounters};

#[test]
fn counters_start_at_zero_and_add_exactly() {
    let counters = StripedCounters::new(3);
    assert_eq!(counters.len(), 3);
    assert_eq!(counters.sum(0), 0);
    counters.add(0, 5);
    counters.add(0, 2);
    counters.add(2, 9);
    assert_eq!(counters.sum(0), 7);
    assert_eq!(counters.sum(1), 0);
    assert_eq!(counters.sum(2), 9);
}

/// Every counter of a set spanning several lines per stripe, on every stripe, lands in its own
/// slot: a distinct amount per counter and per stripe is read back exactly.
#[test]
fn every_counter_on_every_stripe_is_its_own_slot() {
    let len = 40;
    let counters = StripedCounters::new(len);
    let wide = |value: usize| u64::try_from(value).expect("a usize fits u64 here");
    for stripe in 0..STRIPES {
        for counter in 0..len {
            counters.add_at(stripe, counter, (wide(counter) + 1) * 1_000 + wide(stripe));
        }
    }
    for counter in 0..len {
        let per_stripe: u64 = (0..wide(STRIPES)).sum();
        let expected = (wide(counter) + 1) * 1_000 * wide(STRIPES) + per_stripe;
        assert_eq!(counters.sum(counter), expected, "counter {counter}");
    }
}

#[test]
fn take_returns_the_total_and_resets_it() {
    let counters = StripedCounters::new(2);
    counters.add_at(0, 1, 4);
    counters.add_at(7, 1, 6);
    counters.add(0, 1);
    assert_eq!(counters.take(1), 10);
    assert_eq!(counters.sum(1), 0);
    assert_eq!(counters.take(1), 0);
    assert_eq!(counters.sum(0), 1, "another counter is untouched");
}

#[test]
fn a_counter_out_of_range_is_ignored() {
    let counters = StripedCounters::new(2);
    counters.add(2, 1);
    counters.add_at(STRIPES, 0, 1);
    assert_eq!(counters.sum(2), 0);
    assert_eq!(counters.take(2), 0);
    assert_eq!(counters.sum(0) + counters.sum(1), 0);
}

#[test]
fn sums_wrap_like_the_counters_they_add() {
    let counters = StripedCounters::new(1);
    counters.add_at(0, 0, u64::MAX);
    counters.add_at(1, 0, 2);
    assert_eq!(counters.sum(0), 1);
}

#[test]
fn an_empty_set_holds_no_counter() {
    let counters = StripedCounters::new(0);
    assert_eq!(counters.len(), 0);
    counters.add(0, 1);
    assert_eq!(counters.sum(0), 0);
}

/// Each line is a whole 128-byte block, so two stripes never share a cache line pair.
#[test]
fn a_line_is_one_padded_block() {
    assert_eq!(std::mem::size_of::<Line>(), 128);
    assert_eq!(std::mem::align_of::<Line>(), 128);
}

/// Threads take their stripes in turn, so the first threads of a process spread over stripes.
#[test]
fn threads_spread_over_stripes() {
    let stripes: std::collections::HashSet<usize> = (0..STRIPES)
        // Plain threads: the subject is the stripe each operating-system thread is given.
        .map(|_| std::thread::spawn(super::thread_stripe).join().expect("no panic"))
        .collect();
    assert!(stripes.len() > 1, "{stripes:?}");
    assert!(stripes.iter().all(|stripe| *stripe < STRIPES));
}
