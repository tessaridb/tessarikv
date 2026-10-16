use std::thread;

use super::*;

/// The byte the writer puts at position `at`: content is a pure function of position, so any
/// reader can check what it read without coordinating with the writer.
fn expected(at: u64) -> u8 {
    (at.wrapping_mul(31).wrapping_add(7) % 251) as u8
}

fn pattern(from: u64, len: usize) -> Vec<u8> {
    (0..len as u64).map(|i| expected(from + i)).collect()
}

#[test]
fn reads_back_every_range_across_chunk_boundaries() {
    let (mut writer, reader) = append_buffer(4).unwrap();
    for len in [1_usize, 3, 4, 5, 9] {
        let from = writer.len();
        writer.append(&pattern(from, len)).unwrap();
    }
    let total = writer.len();
    assert_eq!(total, 22);
    assert_eq!(reader.len(), total);
    for start in 0..total {
        for end in start..=total {
            let mut buf = vec![0; (end - start) as usize];
            reader.read_at(start, &mut buf).unwrap();
            assert_eq!(buf, pattern(start, buf.len()), "range {start}..{end}");
        }
    }
}

#[test]
fn a_read_past_the_published_length_names_both_ends() {
    let (mut writer, reader) = append_buffer(8).unwrap();
    writer.append(&[1, 2, 3]).unwrap();
    let mut buf = [0; 2];
    assert_eq!(
        reader.read_at(2, &mut buf),
        Err(BufferError::OutOfRange {
            requested_end: 4,
            available: 3
        })
    );
}

#[test]
fn a_zero_chunk_size_is_refused() {
    assert!(matches!(append_buffer(0), Err(BufferError::Overflow)));
}

/// Readers on other threads only ever see published, correct bytes while the writer appends.
/// The writer goes on until every reader has checked at least once, so a reader scheduled late
/// still reads while bytes are being appended.
#[test]
fn concurrent_readers_never_see_unpublished_or_torn_bytes() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

    const READERS: usize = 4;
    let (mut writer, reader) = append_buffer(64).unwrap();
    let checks: Arc<[AtomicU64; READERS]> = Arc::new(std::array::from_fn(|_| AtomicU64::new(0)));
    let done = Arc::new(AtomicBool::new(false));
    let readers: Vec<_> = (0..READERS)
        .map(|index| {
            let (reader, checks, done) = (reader.clone(), Arc::clone(&checks), Arc::clone(&done));
            thread::spawn(move || {
                let mut state = index as u64 + 1;
                // Relaxed: the flag only ends the loop; the bytes are ordered by the buffer.
                while !done.load(Ordering::Relaxed) {
                    let len = reader.len();
                    if len == 0 {
                        continue;
                    }
                    state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
                    let start = state % len;
                    let size = ((state >> 32) % 300).min(len - start) as usize;
                    let mut buf = vec![0; size];
                    reader.read_at(start, &mut buf).unwrap();
                    assert_eq!(buf, pattern(start, size));
                    // Relaxed: a progress count read on its own.
                    checks[index].fetch_add(1, Ordering::Relaxed);
                }
            })
        })
        .collect();
    let unchecked = || {
        checks
            .iter()
            .any(|count| count.load(Ordering::Relaxed) == 0)
    };
    // A reader that stopped early, by a failed check, ends the writing too.
    while (writer.len() < 200_000 || unchecked())
        && !readers.iter().any(thread::JoinHandle::is_finished)
    {
        let from = writer.len();
        let size = 1 + (from % 97) as usize;
        writer.append(&pattern(from, size)).unwrap();
    }
    done.store(true, Ordering::Relaxed);
    for handle in readers {
        handle.join().unwrap();
    }
}
