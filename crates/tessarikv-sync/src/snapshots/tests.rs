use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};

use super::SnapshotRegistry;

#[test]
fn a_registered_snapshot_is_in_every_later_horizon_until_released() {
    let registry = SnapshotRegistry::new();
    let first = registry.register(|| 5).unwrap();
    let second = registry.register(|| 9).unwrap();
    assert_eq!((first.sequence(), second.sequence()), (5, 9));
    assert_eq!(registry.horizon(12).snapshots(), &[5, 9]);
    assert_eq!(registry.horizon(12).earliest(), 5);
    registry.release(first);
    assert_eq!(registry.horizon(12).snapshots(), &[9]);
    registry.release(second);
    let horizon = registry.horizon(12);
    assert!(horizon.snapshots().is_empty());
    assert_eq!(
        horizon.earliest(),
        12,
        "with no snapshot the visible sequence bounds what is kept"
    );
}

#[test]
fn a_released_slot_is_reused() {
    let registry = SnapshotRegistry::new();
    for round in 0..100_u64 {
        let snapshot = registry.register(|| round).unwrap();
        registry.release(snapshot);
    }
    assert_eq!(registry.slots_in_use(), 1, "one slot served every round");
}

#[test]
fn a_registration_never_takes_a_sequence_below_an_announced_horizon() {
    let registry = SnapshotRegistry::new();
    let _ = registry.horizon(40);
    let reads = AtomicU64::new(0);
    // The first read is stale (below the announced horizon); the retry reads the current value.
    let snapshot = registry
        .register(|| {
            if reads.fetch_add(1, Ordering::Relaxed) == 0 {
                30
            } else {
                41
            }
        })
        .unwrap();
    assert_eq!(snapshot.sequence(), 41);
    assert_eq!(reads.load(Ordering::Relaxed), 2);
}

#[test]
fn concurrent_registrations_are_never_missed_by_a_horizon() {
    let rounds: u64 = if cfg!(miri) { 50 } else { 20_000 };
    let registry = Arc::new(SnapshotRegistry::new());
    let visible = Arc::new(AtomicU64::new(1));
    let stop = Arc::new(AtomicBool::new(false));
    // Every thread starts together: a compactor scheduled after the readers finish takes no
    // horizon while they register, and the run checks nothing.
    let start = Arc::new(Barrier::new(4));
    let compactor = {
        let (registry, visible, stop, start) = (
            Arc::clone(&registry),
            Arc::clone(&visible),
            Arc::clone(&stop),
            Arc::clone(&start),
        );
        std::thread::spawn(move || {
            start.wait();
            let mut horizons = Vec::new();
            while !stop.load(Ordering::Relaxed) {
                let horizon = registry.horizon(visible.load(Ordering::Acquire));
                horizons.push((horizon.visible(), horizon.snapshots().to_vec()));
            }
            horizons
        })
    };
    let readers: Vec<_> = (0..3)
        .map(|_| {
            let (registry, visible, start) = (
                Arc::clone(&registry),
                Arc::clone(&visible),
                Arc::clone(&start),
            );
            std::thread::spawn(move || {
                start.wait();
                for _ in 0..rounds {
                    visible.fetch_add(1, Ordering::AcqRel);
                    let snapshot = registry
                        .register(|| visible.load(Ordering::Acquire))
                        .unwrap();
                    registry.release(snapshot);
                }
            })
        })
        .collect();
    for reader in readers {
        reader.join().unwrap();
    }
    stop.store(true, Ordering::Relaxed);
    let horizons = compactor.join().unwrap();
    assert!(!horizons.is_empty());
    let last = visible.load(Ordering::Acquire);
    for (_, snapshots) in horizons {
        assert!(
            snapshots
                .iter()
                .all(|snapshot| (1..=last).contains(snapshot)),
            "a horizon returned a sequence no reader registered: {snapshots:?}"
        );
    }
    // A reader takes a new slot only when every slot it scanned was held, and the others hold
    // two at most; a stale count can add a few, never one per registration.
    assert!(
        registry.slots_in_use() < 16,
        "released slots are reused: {} slots for {} registrations",
        registry.slots_in_use(),
        rounds * 3
    );
}
