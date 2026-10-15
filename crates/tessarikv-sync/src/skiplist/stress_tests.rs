//! Real-parallelism checks: per-key linearizability of recorded histories and a differential
//! comparison with a sequential model. Each round records 300 ms on a fresh list, which keeps a
//! history small enough to check; `TESSARIKV_STRESS_MILLIS` sets the total time across rounds.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Barrier;
use std::time::{Duration, Instant};

use super::{BytewiseComparator, Insert, SkipList};

/// One recorded operation on one key.
struct Event {
    key: u16,
    invoked: Instant,
    returned: Instant,
    /// `Some(value)` when the operation observed the key present with that value.
    observed: Option<u32>,
    /// `Some(value)` when this operation inserted the key.
    inserted: Option<u32>,
}

/// One round's recording time.
const ROUND: Duration = Duration::from_millis(300);

/// How many rounds the configured total time buys (at least one).
fn rounds() -> u64 {
    let millis: u64 = std::env::var("TESSARIKV_STRESS_MILLIS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(300);
    (millis / 300).max(1)
}

/// Records a history of inserts and gets over a small key space from `threads` threads.
fn record(list: &Arc<SkipList<BytewiseComparator>>, threads: u32) -> Vec<Event> {
    let barrier = Arc::new(Barrier::new(usize::try_from(threads).unwrap()));
    let deadline = ROUND;
    let workers: Vec<_> = (0..threads)
        .map(|thread| {
            let (list, barrier) = (Arc::clone(list), Arc::clone(&barrier));
            std::thread::spawn(move || {
                let mut state = u64::from(thread).wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
                let mut events = Vec::new();
                barrier.wait();
                let start = Instant::now();
                while start.elapsed() < deadline {
                    state ^= state << 13;
                    state ^= state >> 7;
                    state ^= state << 17;
                    let key = u16::try_from(state % 512).unwrap();
                    let value = thread << 16 | u32::try_from(events.len() % 65_536).unwrap();
                    let invoked = Instant::now();
                    let (observed, inserted) = if state & 0x100 == 0 {
                        match list
                            .insert(&key.to_be_bytes(), &value.to_be_bytes())
                            .unwrap()
                        {
                            Insert::Inserted => (None, Some(value)),
                            Insert::AlreadyPresent => {
                                (list.get(&key.to_be_bytes()).map(decode), None)
                            }
                        }
                    } else {
                        (list.get(&key.to_be_bytes()).map(decode), None)
                    };
                    events.push(Event {
                        key,
                        invoked,
                        returned: Instant::now(),
                        observed,
                        inserted,
                    });
                }
                events
            })
        })
        .collect();
    workers
        .into_iter()
        .flat_map(|worker| worker.join().unwrap())
        .collect()
}

fn decode(bytes: &[u8]) -> u32 {
    u32::from_be_bytes(bytes.try_into().unwrap())
}

/// Checks one key's history against "inserted once, then present forever with that value".
fn check_key(key: u16, events: &[&Event]) -> Result<(), String> {
    let inserts: Vec<_> = events
        .iter()
        .filter(|event| event.inserted.is_some())
        .collect();
    if inserts.len() > 1 {
        return Err(format!("key {key} inserted {} times", inserts.len()));
    }
    let present: Vec<_> = events
        .iter()
        .filter(|event| event.observed.is_some())
        .collect();
    let Some(insert) = inserts.first() else {
        return if present.is_empty() {
            Ok(())
        } else {
            Err(format!("key {key} observed but never inserted"))
        };
    };
    let value = insert.inserted;
    if let Some(wrong) = present.iter().find(|event| event.observed != value) {
        return Err(format!(
            "key {key} read {:?}, inserted {:?}",
            wrong.observed, value
        ));
    }
    // The insert takes effect at one instant inside its interval, after every operation that saw
    // the key absent began and before every operation that saw it present returned.
    let absent_started = events
        .iter()
        .filter(|event| event.observed.is_none() && event.inserted.is_none())
        .map(|event| event.invoked)
        .max();
    let present_returned = present.iter().map(|event| event.returned).min();
    let earliest = absent_started.map_or(insert.invoked, |started| started.max(insert.invoked));
    let latest = present_returned.map_or(insert.returned, |returned| returned.min(insert.returned));
    if earliest > latest {
        return Err(format!(
            "key {key}: no instant linearizes the insert ({earliest:?} > {latest:?})"
        ));
    }
    Ok(())
}

fn histories_are_linearizable(threads: u32) {
    for _ in 0..rounds() {
        one_round(threads);
    }
}

fn one_round(threads: u32) {
    let list = Arc::new(SkipList::new(BytewiseComparator));
    let events = record(&list, threads);
    let mut by_key: BTreeMap<u16, Vec<&Event>> = BTreeMap::new();
    for event in &events {
        by_key.entry(event.key).or_default().push(event);
    }
    for (key, key_events) in &by_key {
        check_key(*key, key_events).unwrap();
    }
    // Differential: the list holds exactly the inserted keys, in order, with the inserted values.
    let model: BTreeMap<Vec<u8>, u32> = events
        .iter()
        .filter_map(|event| Some((event.key.to_be_bytes().to_vec(), event.inserted?)))
        .collect();
    let mut cursor = list.cursor();
    cursor.seek_to_first();
    let mut actual = BTreeMap::new();
    let mut previous: Option<Vec<u8>> = None;
    while cursor.valid() {
        let key = cursor.key().to_vec();
        assert!(
            previous.as_ref().is_none_or(|before| *before < key),
            "out of order"
        );
        actual.insert(key.clone(), decode(cursor.value()));
        previous = Some(key);
        cursor.next();
    }
    assert_eq!(actual, model);
    assert!(
        events.len() > usize::try_from(threads).unwrap(),
        "the run recorded too little to check"
    );
}

#[test]
fn histories_are_linearizable_at_core_count() {
    let cores = std::thread::available_parallelism().map_or(4, |count| count.get());
    histories_are_linearizable(u32::try_from(cores).unwrap());
}

#[test]
fn histories_are_linearizable_oversubscribed() {
    let cores = std::thread::available_parallelism().map_or(4, |count| count.get());
    histories_are_linearizable(u32::try_from(cores * 2).unwrap());
}
