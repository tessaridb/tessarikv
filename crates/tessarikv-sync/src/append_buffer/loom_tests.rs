use loom::thread;

use super::*;

/// Every interleaving of an append that crosses a chunk boundary with a concurrent reader:
/// the reader sees either nothing, the first append, or both, and never a torn or unpublished byte.
#[test]
fn a_reader_sees_a_published_prefix_in_every_interleaving() {
    loom::model(|| {
        let (mut writer, reader) = append_buffer(2).unwrap();
        let observer = thread::spawn(move || {
            let len = reader.len();
            let mut buf = vec![0; usize::try_from(len).unwrap()];
            reader.read_at(0, &mut buf).unwrap();
            assert!(len == 0 || len == 3 || len == 4, "len {len}");
            assert_eq!(buf, [10, 11, 12, 13][..buf.len()]);
        });
        writer.append(&[10, 11, 12]).unwrap();
        writer.append(&[13]).unwrap();
        observer.join().unwrap();
    });
}

/// Two readers of one buffer agree with the writer's order.
#[test]
fn two_readers_never_disagree_with_the_writer() {
    loom::model(|| {
        let (mut writer, reader) = append_buffer(1).unwrap();
        let other = reader.clone();
        let first = thread::spawn(move || {
            let mut buf = [0; 1];
            if reader.read_at(1, &mut buf).is_ok() {
                assert_eq!(buf, [6]);
            }
        });
        let second = thread::spawn(move || {
            let mut buf = [0; 1];
            if other.read_at(0, &mut buf).is_ok() {
                assert_eq!(buf, [5]);
            }
        });
        writer.append(&[5]).unwrap();
        writer.append(&[6]).unwrap();
        first.join().unwrap();
        second.join().unwrap();
    });
}
