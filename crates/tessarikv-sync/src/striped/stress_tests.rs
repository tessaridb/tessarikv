//! Real-parallelism check with more threads than cores: adders race an interval reader that
//! takes the counters while they grow, and nothing is lost or counted twice: the takes plus what
//! is left equal everything added, per counter.
//! `TESSARIKV_STRESS_MILLIS` sets the running time (default 300 ms).
//!
//! The workers are plain threads on purpose: the point is preemption at arbitrary instructions
//! with more threads than cores, which a work-stealing pool would avoid.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use super::StripedCounters;

const COUNTERS: usize = 20;

fn duration() -> Duration {
    let millis = std::env::var("TESSARIKV_STRESS_MILLIS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(300);
    Duration::from_millis(millis)
}

#[test]
fn takes_and_what_remains_equal_everything_added() {
    let adders = std::thread::available_parallelism().map_or(4, usize::from) * 2;
    let counters = Arc::new(StripedCounters::new(COUNTERS));
    let stop = Arc::new(AtomicBool::new(false));
    let start = Arc::new(Barrier::new(adders + 1));
    let workers: Vec<_> = (0..adders)
        .map(|worker| {
            let (counters, stop, start) = (counters.clone(), stop.clone(), start.clone());
            // A plain thread: preemption with more threads than cores (see the module documentation).
            std::thread::spawn(move || {
                let mut added = [0_u64; COUNTERS];
                start.wait();
                let mut step = worker;
                while !stop.load(Ordering::Relaxed) {
                    let counter = step % COUNTERS;
                    let amount = u64::try_from(step % 7 + 1).expect("at most 7");
                    counters.add(counter, amount);
                    added[counter] += amount;
                    step += 1;
                }
                added
            })
        })
        .collect();
    start.wait();
    let mut taken = [0_u64; COUNTERS];
    let deadline = Instant::now() + duration();
    while Instant::now() < deadline {
        for (counter, total) in taken.iter_mut().enumerate() {
            *total += counters.take(counter);
        }
    }
    stop.store(true, Ordering::Relaxed);
    let mut added = [0_u64; COUNTERS];
    for worker in workers {
        for (total, part) in added
            .iter_mut()
            .zip(worker.join().expect("no adder panics"))
        {
            *total += part;
        }
    }
    assert!(added.iter().sum::<u64>() > 0, "the adders ran");
    for counter in 0..COUNTERS {
        assert_eq!(
            taken[counter] + counters.sum(counter),
            added[counter],
            "counter {counter}"
        );
    }
}
