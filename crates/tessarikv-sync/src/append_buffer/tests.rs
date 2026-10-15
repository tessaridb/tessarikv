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
#[test]
fn concurrent_readers_never_see_unpublished_or_torn_bytes() {
    let (mut writer, reader) = append_buffer(64).unwrap();
    let readers: Vec<_> = (0..4)
        .map(|seed| {
            let reader = reader.clone();
            thread::spawn(move || {
                let mut checked = 0_u64;
                let mut state = seed + 1_u64;
                while reader.len() < 200_000 {
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
                    checked += 1;
                }
                checked
            })
        })
        .collect();
    while writer.len() < 200_000 {
        let from = writer.len();
        let size = 1 + (from % 97) as usize;
        writer.append(&pattern(from, size)).unwrap();
    }
    for handle in readers {
        assert!(handle.join().unwrap() > 0);
    }
}
