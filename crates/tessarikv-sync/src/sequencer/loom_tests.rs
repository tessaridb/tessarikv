use loom::cell::UnsafeCell;
use loom::sync::Arc;

use super::Sequencer;

/// Two batches completed by two threads in either order are both visible afterwards: neither
/// completer may leave the other's slot behind (the store-then-load pair).
#[test]
fn concurrent_completions_never_lose_an_advance() {
    loom::model(|| {
        let sequencer = Arc::new(Sequencer::new(0, 4));
        let first = sequencer.claim(1).unwrap();
        let second = sequencer.claim(2).unwrap();
        let other = Arc::clone(&sequencer);
        let completer = loom::thread::spawn(move || other.complete(second));
        sequencer.complete(first);
        completer.join().unwrap();
        assert_eq!(sequencer.visible(), 3);
    });
}

/// A reader that sees a batch's sequence as visible also sees what the batch's writer did
/// before completing it.
#[test]
fn a_visible_batch_publishes_its_writes() {
    loom::model(|| {
        let sequencer = Arc::new(Sequencer::new(0, 2));
        let payload = Arc::new(UnsafeCell::new(0_u64));
        let (writer_sequencer, writer_payload) = (Arc::clone(&sequencer), Arc::clone(&payload));
        let writer = loom::thread::spawn(move || {
            let claim = writer_sequencer.claim(1).unwrap();
            // SAFETY: only the writer writes, before completing; the reader reads after seeing
            // the completion through `visible`.
            writer_payload.with_mut(|value| unsafe { *value = 7 });
            writer_sequencer.complete(claim);
        });
        if sequencer.visible() >= 1 {
            // SAFETY: as above; loom reports the read if it is not ordered after the write.
            let value = payload.with(|value| unsafe { *value });
            assert_eq!(value, 7);
        }
        writer.join().unwrap();
    });
}

/// Three single-sequence batches through a two-slot ring: the third waits for its slot and the
/// prefix still ends complete.
#[test]
fn a_full_ring_waits_and_then_completes() {
    loom::model(|| {
        let sequencer = Arc::new(Sequencer::new(0, 2));
        let claims: Vec<_> = (0..3).map(|_| sequencer.claim(1).unwrap()).collect();
        let mut claims = claims.into_iter();
        let (first, second, third) = (
            claims.next().unwrap(),
            claims.next().unwrap(),
            claims.next().unwrap(),
        );
        let other = Arc::clone(&sequencer);
        let late = loom::thread::spawn(move || other.complete(third));
        sequencer.complete(first);
        sequencer.complete(second);
        late.join().unwrap();
        assert_eq!(sequencer.visible(), 3);
    });
}

/// A batch made visible by ANOTHER thread's advance still publishes its writer's writes to a
/// reader: the advancing compare-and-swap must carry them.
#[test]
fn an_advance_by_another_thread_publishes_the_batch() {
    loom::model(|| {
        let sequencer = Arc::new(Sequencer::new(0, 4));
        let payload = Arc::new(UnsafeCell::new(0_u64));
        let first = sequencer.claim(1).unwrap();
        let second = sequencer.claim(1).unwrap();
        let (writer_sequencer, writer_payload) = (Arc::clone(&sequencer), Arc::clone(&payload));
        let writer = loom::thread::spawn(move || {
            // SAFETY: only this thread writes, before completing its batch.
            writer_payload.with_mut(|value| unsafe { *value = 7 });
            writer_sequencer.complete(second);
        });
        let advancer_sequencer = Arc::clone(&sequencer);
        let advancer = loom::thread::spawn(move || advancer_sequencer.complete(first));
        if sequencer.visible() >= 2 {
            // SAFETY: as above; loom reports the read if it is not ordered after the write.
            let value = payload.with(|value| unsafe { *value });
            assert_eq!(value, 7);
        }
        writer.join().unwrap();
        advancer.join().unwrap();
    });
}
