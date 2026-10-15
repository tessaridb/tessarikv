use loom::sync::Arc;
use loom::thread;

use super::SlotDirectory;

#[test]
fn racing_installers_and_a_reader_agree_on_one_value() {
    loom::model(|| {
        let directory = Arc::new(SlotDirectory::<u64>::new());
        let installers: Vec<_> = [1_u64, 2]
            .into_iter()
            .map(|value| {
                let directory = Arc::clone(&directory);
                thread::spawn(move || *directory.get_or_install(2, || value).unwrap())
            })
            .collect();
        let reader = {
            let directory = Arc::clone(&directory);
            thread::spawn(move || directory.get(2).copied())
        };
        let seen: Vec<u64> = installers
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect();
        assert_eq!(seen[0], seen[1], "installers disagree");
        if let Some(read) = reader.join().unwrap() {
            assert_eq!(read, seen[0], "reader saw a value nobody kept");
        }
    });
}

/// A value created by one thread and read by another through `get`; loom checks that the
/// lookup's Acquire orders the value's creation before the read.
#[test]
fn a_value_read_through_get_is_fully_constructed() {
    loom::model(|| {
        let directory = Arc::new(SlotDirectory::<loom::cell::UnsafeCell<u64>>::new());
        let installer = {
            let directory = Arc::clone(&directory);
            thread::spawn(move || {
                directory.get_or_install(0, || loom::cell::UnsafeCell::new(9));
            })
        };
        if let Some(cell) = directory.get(0) {
            // SAFETY: the value is never written after construction.
            assert_eq!(cell.with(|value| unsafe { *value }), 9);
        }
        installer.join().unwrap();
    });
}
