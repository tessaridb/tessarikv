use std::time::{Duration, Instant};

use super::rate_limiter::RateLimiter;

const SECOND: Duration = Duration::from_secs(1);

#[test]
fn a_second_of_bytes_goes_at_once_and_more_waits_for_the_rate() {
    let origin = Instant::now();
    let limiter = RateLimiter::starting(1000, SECOND, origin).unwrap();
    assert_eq!(
        limiter.reserve(1000, origin),
        Duration::ZERO,
        "a second's worth at once"
    );
    assert_eq!(
        limiter.reserve(500, origin),
        Duration::from_millis(500),
        "half a second over"
    );
    assert_eq!(
        limiter.reserve(500, origin),
        Duration::from_secs(1),
        "what is owed adds up"
    );
    let later = origin + Duration::from_secs(3);
    assert_eq!(
        limiter.reserve(1000, later),
        Duration::ZERO,
        "idle time gives the second back"
    );
    assert_eq!(limiter.charged(), 3000);
}

#[test]
fn a_rate_of_zero_paces_nothing_and_a_huge_reservation_saturates() {
    assert!(RateLimiter::new(0, SECOND).is_none());
    let origin = Instant::now();
    let limiter = RateLimiter::starting(1, SECOND, origin).unwrap();
    let wait = limiter.reserve(u64::MAX, origin);
    // The wait saturates at the nanoseconds a u64 holds, some 584 years.
    assert!(wait > Duration::from_secs(1 << 33), "{wait:?}");
    assert!(limiter.reserve(1, origin) >= wait, "still owed");
}

#[test]
fn concurrent_reservations_each_take_their_own_slot_and_none_is_lost() {
    const THREADS: u64 = 8;
    const EACH: u64 = 2000;
    let origin = Instant::now();
    // One byte a second: the n-th reservation, in the order they take effect, waits n - 1 seconds.
    let limiter = RateLimiter::starting(1, SECOND, origin).unwrap();
    let mut waits: Vec<u64> = std::thread::scope(|scope| {
        let workers: Vec<_> = (0..THREADS)
            .map(|_| {
                scope.spawn(|| {
                    (0..EACH)
                        .map(|_| limiter.reserve(1, origin).as_secs())
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        workers
            .into_iter()
            .flat_map(|worker| worker.join().unwrap())
            .collect()
    });
    waits.sort_unstable();
    let expected: Vec<u64> = (0..THREADS * EACH).collect();
    assert_eq!(waits, expected, "every slot taken exactly once");
    assert_eq!(limiter.charged(), THREADS * EACH);
}
