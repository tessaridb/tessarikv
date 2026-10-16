use loom::sync::Arc;

use super::StripedCounters;

/// Two adders on one stripe: both increments land (the read-modify-write is atomic).
#[test]
fn adders_sharing_a_stripe_lose_nothing() {
    loom::model(|| {
        let counters = Arc::new(StripedCounters::new(2));
        let other = Arc::clone(&counters);
        let adder = loom::thread::spawn(move || other.add_at(3, 1, 2));
        counters.add_at(3, 1, 5);
        adder.join().expect("no adder panics");
        assert_eq!(counters.sum(1), 7);
        assert_eq!(counters.sum(0), 0);
    });
}

/// An interval reader racing two adders on different stripes: every increment is either in one
/// take or in what remains after, never in both and never in neither.
#[test]
fn takes_racing_adders_count_each_increment_once() {
    loom::model(|| {
        let counters = Arc::new(StripedCounters::new(1));
        let adders: Vec<_> = [(0, 1), (1, 10)]
            .into_iter()
            .map(|(stripe, amount)| {
                let counters = Arc::clone(&counters);
                loom::thread::spawn(move || counters.add_at(stripe, 0, amount))
            })
            .collect();
        let first = counters.take(0);
        for adder in adders {
            adder.join().expect("no adder panics");
        }
        let second = counters.take(0);
        assert_eq!(first + second, 11);
        assert_eq!(counters.sum(0), 0);
    });
}

/// A sum read while adders run sees each completed add or not, never a torn or invented value.
#[test]
fn a_racing_sum_sees_a_subset_of_the_adds() {
    loom::model(|| {
        let counters = Arc::new(StripedCounters::new(1));
        let other = Arc::clone(&counters);
        let adder = loom::thread::spawn(move || {
            other.add_at(0, 0, 1);
            other.add_at(5, 0, 10);
        });
        let seen = counters.sum(0);
        assert!([0, 1, 10, 11].contains(&seen), "{seen}");
        adder.join().expect("no adder panics");
        assert_eq!(counters.sum(0), 11);
    });
}
