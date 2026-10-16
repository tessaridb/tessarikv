use loom::sync::Arc;
use loom::sync::atomic::{AtomicU64, Ordering};
use loom::thread;

use super::ByteArena;

/// A writer publishes a filled allocation through a Release store; a reader that sees the
/// reference must see every byte (the publication the skiplist's linking CAS performs).
#[test]
fn a_published_allocation_is_read_whole() {
    loom::model(|| {
        let arena = Arc::new(ByteArena::new(64).unwrap());
        let published = Arc::new(AtomicU64::new(0));
        let writer = {
            let (arena, published) = (Arc::clone(&arena), Arc::clone(&published));
            thread::spawn(move || {
                let (reference, cells, offset) = arena.allocate(8).unwrap();
                // SAFETY: fresh allocation, ours until the store below.
                unsafe { cells.write(offset, &[7; 8]) }.unwrap();
                published.store(reference, Ordering::Release);
            })
        };
        let reference = published.load(Ordering::Acquire);
        if reference != 0 {
            let (cells, offset) = arena.resolve(reference).unwrap();
            let mut out = [0; 8];
            // SAFETY: the Acquire load above synchronized with the writer's Release store.
            unsafe { cells.read(offset, &mut out) }.unwrap();
            assert_eq!(out, [7; 8]);
        }
        writer.join().unwrap();
    });
}

/// Two allocators racing for the first chunk get disjoint ranges in the same installed chunk.
#[test]
fn racing_allocations_are_disjoint() {
    loom::model(|| {
        let arena = Arc::new(ByteArena::new(64).unwrap());
        let handles: Vec<_> = [1_u8, 2]
            .into_iter()
            .map(|fill| {
                let arena = Arc::clone(&arena);
                thread::spawn(move || {
                    let (reference, cells, offset) = arena.allocate(8).unwrap();
                    // SAFETY: fresh allocation, exclusively ours.
                    unsafe { cells.write(offset, &[fill; 8]) }.unwrap();
                    (reference, fill)
                })
            })
            .collect();
        let results: Vec<(u64, u8)> = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect();
        assert_ne!(results[0].0, results[1].0);
        for (reference, fill) in results {
            let (cells, offset) = arena.resolve(reference).unwrap();
            let mut out = [0; 8];
            // SAFETY: both writers were joined.
            unsafe { cells.read(offset, &mut out) }.unwrap();
            assert_eq!(out, [fill; 8]);
        }
    });
}
