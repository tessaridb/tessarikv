use std::collections::BTreeSet;
use std::sync::Arc;

use super::ByteArena;
use crate::shim::Ordering;

fn write(arena: &ByteArena, len: usize, fill: u8) -> u64 {
    let (reference, cells, offset) = arena.allocate(len).unwrap();
    // SAFETY: a fresh allocation is exclusively ours until published.
    unsafe { cells.write(offset, &vec![fill; len]) }.unwrap();
    reference
}

fn read(arena: &ByteArena, reference: u64, len: usize) -> Vec<u8> {
    let (cells, offset) = arena.resolve(reference).unwrap();
    let mut out = vec![0; len];
    // SAFETY: written above on this thread.
    unsafe { cells.read(offset, &mut out) }.unwrap();
    out
}

#[test]
fn allocations_are_nonzero_aligned_and_keep_their_bytes() {
    let arena = ByteArena::new(256).unwrap();
    let sizes = [1, 7, 8, 24, 63, 64, 65, 100, 200, 1000];
    let written: Vec<(u64, usize, u8)> = sizes
        .iter()
        .zip(1_u8..)
        .map(|(&len, fill)| (write(&arena, len, fill), len, fill))
        .collect();
    for &(reference, len, fill) in &written {
        assert_ne!(reference, 0);
        assert_eq!(read(&arena, reference, len), vec![fill; len], "len {len}");
        let (_, offset) = arena.resolve(reference).unwrap();
        assert_eq!(offset % 8, 0, "len {len} misaligned");
    }
}

#[test]
fn a_small_allocation_never_crosses_a_chunk() {
    let arena = ByteArena::new(64).unwrap();
    for _ in 0..200 {
        let (_, cells, offset) = arena.allocate(16).unwrap();
        // SAFETY: a fresh allocation, written by this thread only.
        let written = unsafe { cells.write(offset, &[1; 16]) };
        assert!(written.is_some(), "allocation crosses its chunk");
    }
}

#[test]
fn an_oversize_allocation_gets_its_own_bytes() {
    let arena = ByteArena::new(64).unwrap();
    let small = write(&arena, 8, 1);
    let large = write(&arena, 1000, 2);
    assert_eq!(read(&arena, small, 8), vec![1; 8]);
    assert_eq!(read(&arena, large, 1000), vec![2; 1000]);
    assert!(arena.allocated_bytes() >= 1008);
}

#[test]
fn a_chunk_size_that_is_not_a_power_of_two_is_refused() {
    assert!(ByteArena::new(100).is_err());
    assert!(ByteArena::new(32).is_err());
}

#[test]
fn concurrent_allocations_are_disjoint() {
    let arena = Arc::new(ByteArena::new(256).unwrap());
    let threads: Vec<_> = (0..8_u8)
        .map(|id| {
            let arena = Arc::clone(&arena);
            std::thread::spawn(move || {
                (0..if cfg!(miri) { 20 } else { 500 })
                    .map(|round| {
                        let len = 8 + (round % 5) * 8;
                        (write(&arena, len, id), len, id)
                    })
                    .collect::<Vec<_>>()
            })
        })
        .collect();
    let all: Vec<_> = threads
        .into_iter()
        .flat_map(|thread| thread.join().unwrap())
        .collect();
    let references: BTreeSet<u64> = all.iter().map(|entry| entry.0).collect();
    assert_eq!(
        references.len(),
        all.len(),
        "a reference was handed out twice"
    );
    for (reference, len, id) in all {
        assert_eq!(read(&arena, reference, len), vec![id; len]);
    }
}

#[test]
fn an_allocation_exposes_aligned_atomic_words_beside_its_bytes() {
    let arena = ByteArena::new(64).unwrap();
    let (_, cells, offset) = arena.allocate(32).unwrap();
    // SAFETY: the first two words of this fresh allocation are only ever accessed atomically.
    let (first, second) = unsafe {
        (
            cells.atomic_word(offset).unwrap(),
            cells.atomic_word(offset + 8).unwrap(),
        )
    };
    assert_eq!(first.load(Ordering::Relaxed), 0, "a fresh word is zero");
    first.store(0x0102_0304_0506_0708, Ordering::Relaxed);
    second.store(u64::MAX, Ordering::Relaxed);
    // SAFETY: the bytes after the two words are this thread's alone.
    unsafe { cells.write(offset + 16, &[9; 16]) }.unwrap();
    assert_eq!(first.load(Ordering::Relaxed), 0x0102_0304_0506_0708);
    assert_eq!(second.load(Ordering::Relaxed), u64::MAX);
    let mut tail = [0; 16];
    // SAFETY: written above on this thread.
    unsafe { cells.read(offset + 16, &mut tail) }.unwrap();
    assert_eq!(tail, [9; 16], "words and bytes overlap");
    // SAFETY: refused before any access.
    unsafe {
        assert!(cells.atomic_word(offset + 3).is_none(), "misaligned word");
        assert!(cells.atomic_word(64).is_none(), "word past the end");
    }
}
