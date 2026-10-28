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

/// A writer whose claim lands in the epoch a switch started sees everything the switcher did
/// before switching: the read view naming that epoch's table is published first, so the writer
/// always finds the table its sequences belong to.
#[test]
fn a_claim_in_a_new_epoch_sees_what_preceded_the_switch() {
    loom::model(|| {
        let sequencer = Arc::new(Sequencer::new(0, 4));
        let published = Arc::new(loom::sync::atomic::AtomicU64::new(0));
        let (switcher_sequencer, switcher_published) =
            (Arc::clone(&sequencer), Arc::clone(&published));
        let switcher = loom::thread::spawn(move || {
            // Relaxed on purpose: only the switch and the claim may order this store.
            switcher_published.store(1, loom::sync::atomic::Ordering::Relaxed);
            switcher_sequencer.switch()
        });
        let claim = sequencer.claim(1).unwrap();
        if claim.epoch() == 1 {
            assert_eq!(
                published.load(loom::sync::atomic::Ordering::Relaxed),
                1,
                "a claim in the new epoch found the old view"
            );
        }
        let switch = switcher.join().unwrap();
        let in_new_epoch = claim.epoch() == switch.epoch();
        assert_eq!(in_new_epoch, claim.first() >= switch.boundary());
        sequencer.complete(claim);
    });
}

/// A claiming switch owns its boundary: a racing writer's sequences lie below it in the old
/// epoch or above it in the new one, never on it.
#[test]
fn a_claiming_switch_never_shares_its_boundary() {
    loom::model(|| {
        let sequencer = Arc::new(Sequencer::new(0, 4));
        let switcher_sequencer = Arc::clone(&sequencer);
        let switcher = loom::thread::spawn(move || switcher_sequencer.switch_claiming().unwrap());
        let claim = sequencer.claim(2).unwrap();
        let (switch, own) = switcher.join().unwrap();
        assert_eq!(
            (own.first(), own.last()),
            (switch.boundary(), switch.boundary())
        );
        assert_eq!(own.epoch(), switch.epoch());
        let below = claim.last() < switch.boundary() && claim.epoch() != switch.epoch();
        let above = claim.first() > switch.boundary() && claim.epoch() == switch.epoch();
        assert!(below || above, "a writer shared the boundary");
        sequencer.complete(own);
        sequencer.complete(claim);
    });
}

/// A watcher and the completer that reaches its target never miss each other: either the watch
/// sees the target reached, or the completion reports reaching it (the store-then-load pair).
#[test]
fn a_reached_target_is_never_missed() {
    loom::model(|| {
        let sequencer = Arc::new(Sequencer::new(0, 4));
        let claim = sequencer.claim(1).unwrap();
        let other = Arc::clone(&sequencer);
        let completer = loom::thread::spawn(move || other.complete(claim));
        let seen = sequencer.watch(1);
        let reported = completer.join().unwrap();
        assert!(seen || reported, "the reached target was missed");
    });
}

/// The same when the batch reaching the target is made visible by another completer's advance.
#[test]
fn a_target_reached_by_another_threads_advance_is_never_missed() {
    loom::model(|| {
        let sequencer = Arc::new(Sequencer::new(0, 4));
        let first = sequencer.claim(1).unwrap();
        let second = sequencer.claim(1).unwrap();
        let (late, early) = (Arc::clone(&sequencer), Arc::clone(&sequencer));
        let second_completer = loom::thread::spawn(move || late.complete(second));
        let first_completer = loom::thread::spawn(move || early.complete(first));
        let seen = sequencer.watch(2);
        let reported = second_completer.join().unwrap() | first_completer.join().unwrap();
        assert!(seen || reported, "the reached target was missed");
    });
}

/// A reader that waited for a claim's publication sees what the writer did before publishing,
/// whether or not the claim has completed.
#[test]
fn a_published_claim_carries_its_writers_writes() {
    loom::model(|| {
        let sequencer = Arc::new(Sequencer::new(0, 2));
        let payload = Arc::new(UnsafeCell::new(0_u64));
        let claim = sequencer.claim(1).unwrap();
        let (writer_sequencer, writer_payload) = (Arc::clone(&sequencer), Arc::clone(&payload));
        let writer = loom::thread::spawn(move || {
            // SAFETY: only this thread writes, before publishing.
            writer_payload.with_mut(|value| unsafe { *value = 7 });
            writer_sequencer.publish(&claim);
            writer_sequencer.complete(claim);
        });
        sequencer.wait_published(0, 1);
        // SAFETY: as above; loom reports the read if it is not ordered after the write.
        let value = payload.with(|value| unsafe { *value });
        assert_eq!(value, 7);
        writer.join().unwrap();
    });
}

/// A writer waiting for its batch to become visible and the completer of the earlier batch never
/// miss each other: either the waiter's check sees its batch covered, or the completer sees it
/// waiting and wakes it (the store-then-load pair).
#[test]
fn a_waiting_writer_is_never_missed() {
    loom::model(|| {
        let sequencer = Arc::new(Sequencer::new(0, 4));
        let earlier = sequencer.claim(1).unwrap();
        let later = sequencer.claim(1).unwrap();
        let other = Arc::clone(&sequencer);
        let completer = loom::thread::spawn(move || {
            other.complete(earlier);
            other.has_waiters()
        });
        sequencer.complete(later);
        sequencer.add_waiter();
        let covered = sequencer.covers(2);
        let woken = completer.join().unwrap();
        sequencer.remove_waiter();
        assert!(covered || woken, "the waiting writer was missed");
    });
}
