use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use super::Sequencer;

#[test]
fn claims_are_consecutive_and_start_after_the_last_sequence() {
    let sequencer = Sequencer::new(41, 8);
    let first = sequencer.claim(3).unwrap();
    let second = sequencer.claim(1).unwrap();
    assert_eq!((first.first(), first.last()), (42, 44));
    assert_eq!((second.first(), second.last()), (45, 45));
    assert_eq!(sequencer.visible(), 41, "nothing completed yet");
}

#[test]
fn a_later_batch_becomes_visible_only_after_every_earlier_one_completes() {
    let sequencer = Sequencer::new(0, 8);
    let first = sequencer.claim(2).unwrap();
    let second = sequencer.claim(3).unwrap();
    sequencer.complete(second);
    assert_eq!(sequencer.visible(), 0, "the first batch is still in flight");
    sequencer.complete(first);
    assert_eq!(sequencer.visible(), 5, "both batches are visible together");
}

#[test]
fn a_slot_is_reused_once_the_ring_wraps() {
    let sequencer = Sequencer::new(0, 2);
    for round in 0..10_u64 {
        let claim = sequencer.claim(1).unwrap();
        sequencer.complete(claim);
        assert_eq!(sequencer.visible(), round + 1);
    }
}

#[test]
fn claiming_past_the_last_sequence_is_refused() {
    let sequencer = Sequencer::new(tessarikv_constants::MAX_SEQUENCE - 1, 8);
    assert!(sequencer.claim(1).is_some());
    assert!(sequencer.claim(1).is_none());
}

#[test]
fn a_switch_starts_a_new_epoch_at_the_next_sequence() {
    let sequencer = Sequencer::new(41, 8);
    let before = sequencer.claim(3).unwrap();
    let switch = sequencer.switch();
    let after = sequencer.claim(1).unwrap();
    assert_eq!((before.epoch(), before.first(), before.last()), (0, 42, 44));
    assert_eq!((switch.epoch(), switch.boundary()), (1, 45));
    assert_eq!((after.epoch(), after.first(), after.last()), (1, 45, 45));
}

#[test]
fn epochs_wrap_and_sequences_keep_counting() {
    let sequencer = Sequencer::new(0, 8);
    for _ in 0..127 {
        let _ = sequencer.switch();
    }
    let last = sequencer.switch();
    let claim = sequencer.claim(2).unwrap();
    assert_eq!(
        (last.epoch(), last.boundary()),
        (0, 1),
        "128 switches wrap to epoch 0"
    );
    assert_eq!((claim.epoch(), claim.first(), claim.last()), (0, 1, 2));
}

#[test]
fn claiming_past_the_last_sequence_is_refused_in_a_later_epoch() {
    let sequencer = Sequencer::new(tessarikv_constants::MAX_SEQUENCE - 1, 8);
    let _ = sequencer.switch();
    let claim = sequencer.claim(1).unwrap();
    assert_eq!(
        (claim.epoch(), claim.first()),
        (1, tessarikv_constants::MAX_SEQUENCE)
    );
    assert!(sequencer.claim(1).is_none());
}

#[test]
fn every_visible_sequence_belongs_to_a_completed_batch() {
    let total: u64 = if cfg!(miri) { 60 } else { 60_000 };
    let sequencer = Arc::new(Sequencer::new(0, 16));
    let done: Arc<Vec<AtomicBool>> =
        Arc::new((0..=total + 64).map(|_| AtomicBool::new(false)).collect());
    let stop = Arc::new(AtomicBool::new(false));
    let reader = {
        let (sequencer, done, stop) =
            (Arc::clone(&sequencer), Arc::clone(&done), Arc::clone(&stop));
        std::thread::spawn(move || {
            let mut previous = 0;
            while !stop.load(Ordering::Relaxed) {
                let visible = sequencer.visible();
                assert!(visible >= previous, "visible went backwards");
                for sequence in previous + 1..=visible {
                    assert!(
                        done[usize::try_from(sequence).unwrap()].load(Ordering::Relaxed),
                        "sequence {sequence} visible before its batch completed"
                    );
                }
                previous = visible;
            }
        })
    };
    let writers: Vec<_> = (0..4_u64)
        .map(|thread| {
            let (sequencer, done) = (Arc::clone(&sequencer), Arc::clone(&done));
            std::thread::spawn(move || {
                let mut claimed = 0;
                let mut size = thread + 1;
                while claimed < total / 4 {
                    let claim = sequencer.claim(size).unwrap();
                    for sequence in claim.first()..=claim.last() {
                        done[usize::try_from(sequence).unwrap()].store(true, Ordering::Relaxed);
                    }
                    sequencer.complete(claim);
                    claimed += size;
                    size = size % 5 + 1;
                }
                claimed
            })
        })
        .collect();
    let claimed: u64 = writers
        .into_iter()
        .map(|writer| writer.join().unwrap())
        .sum();
    stop.store(true, Ordering::Relaxed);
    reader.join().unwrap();
    assert_eq!(
        sequencer.visible(),
        claimed,
        "every completed batch is visible"
    );
}
