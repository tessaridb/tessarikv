use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::SlotDirectory;

#[test]
fn an_absent_slot_reads_as_none_and_an_installed_one_as_its_value() {
    let directory = SlotDirectory::<u64>::new();
    assert!(directory.get(5).is_none());
    assert_eq!(directory.get_or_install(5, || 50).copied(), Some(50));
    assert_eq!(directory.get(5).copied(), Some(50));
    assert!(directory.get(4).is_none());
    assert!(directory.get(6).is_none());
}

#[test]
fn indices_across_block_boundaries_keep_their_own_values() {
    let directory = SlotDirectory::<u64>::new();
    let indices = [0, 1, 2, 3, 6, 7, 8, 1023, 1024, 65_535, 1 << 20];
    for index in indices {
        directory.get_or_install(index, || index.wrapping_mul(3));
    }
    for index in indices {
        assert_eq!(
            directory.get(index).copied(),
            Some(index.wrapping_mul(3)),
            "index {index}"
        );
    }
}

#[test]
fn an_index_with_no_block_is_refused() {
    let directory = SlotDirectory::<u64>::new();
    assert!(directory.get_or_install(u64::MAX, || 1).is_none());
}

/// Counts drops so a test can see that every installed value, and only those, is freed once.
struct Counted(Arc<AtomicUsize>);

impl Drop for Counted {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
fn racing_installers_agree_on_one_value_and_losers_free_their_own() {
    let drops = Arc::new(AtomicUsize::new(0));
    let directory = Arc::new(SlotDirectory::<(usize, Counted)>::new());
    let threads: Vec<_> = (0..8)
        .map(|id| {
            let directory = Arc::clone(&directory);
            let drops = Arc::clone(&drops);
            std::thread::spawn(move || {
                (0..64_u64)
                    .map(|index| {
                        directory
                            .get_or_install(index, || (id, Counted(Arc::clone(&drops))))
                            .unwrap()
                            .0
                    })
                    .collect::<Vec<_>>()
            })
        })
        .collect();
    let seen: Vec<Vec<usize>> = threads
        .into_iter()
        .map(|thread| thread.join().unwrap())
        .collect();
    for index in 0..64 {
        let winner = directory.get(u64::try_from(index).unwrap()).unwrap().0;
        assert!(
            seen.iter().all(|values| values[index] == winner),
            "slot {index} disagrees"
        );
    }
    let losers_freed = drops.load(Ordering::Relaxed);
    drop(directory);
    assert_eq!(
        drops.load(Ordering::Relaxed) - losers_freed,
        64,
        "every installed value freed once"
    );
}
