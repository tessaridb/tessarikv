use loom::sync::Arc;
use loom::sync::atomic::{AtomicBool, Ordering};

use super::ConcurrentBloom;

/// The engine's use: bits set, then a Release publication; a reader that acquires the
/// publication finds the bits.
#[test]
fn a_hash_inserted_before_a_publication_is_found_by_its_reader() {
    loom::model(|| {
        let bloom = Arc::new(ConcurrentBloom::new(64));
        let published = Arc::new(AtomicBool::new(false));
        let (writer_bloom, writer_published) = (Arc::clone(&bloom), Arc::clone(&published));
        let writer = loom::thread::spawn(move || {
            writer_bloom.insert(0x0123_4567_89ab_cdef);
            writer_published.store(true, Ordering::Release);
        });
        if published.load(Ordering::Acquire) {
            assert!(bloom.may_contain(0x0123_4567_89ab_cdef));
        }
        writer.join().unwrap();
    });
}

/// Two writers touching one line: neither's bits are lost.
#[test]
fn concurrent_inserts_into_one_line_keep_every_bit() {
    loom::model(|| {
        let bloom = Arc::new(ConcurrentBloom::new(64));
        let other = Arc::clone(&bloom);
        let writer = loom::thread::spawn(move || other.insert(0x1111_2222_3333_4444));
        bloom.insert(0x5555_6666_7777_8888);
        writer.join().unwrap();
        assert!(bloom.may_contain(0x1111_2222_3333_4444));
        assert!(bloom.may_contain(0x5555_6666_7777_8888));
    });
}
