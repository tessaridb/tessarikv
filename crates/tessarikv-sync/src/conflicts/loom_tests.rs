use loom::sync::Arc;

use super::ConflictTable;
use crate::Sequencer;

/// The commit-time check: a writer records its key and then publishes its claim; a validator
/// that waited for that publication reads the record. A validator that reads the key as
/// unwritten after the wait would let a lost update commit.
#[test]
fn a_published_writer_is_seen_by_a_later_validator() {
    loom::model(|| {
        let sequencer = Arc::new(Sequencer::new(0, 2));
        let table = Arc::new(ConflictTable::new(4));
        let claim = sequencer.claim(1).unwrap();
        let (writer_sequencer, writer_table) = (Arc::clone(&sequencer), Arc::clone(&table));
        let writer = loom::thread::spawn(move || {
            writer_table.record(7, 1);
            writer_sequencer.publish(&claim);
            writer_sequencer.complete(claim);
        });
        sequencer.wait_published(0, 1);
        assert!(table.newest(7) >= 1);
        writer.join().unwrap();
    });
}

/// Through a two-slot ring a writer's later claim reuses the slot of its first; a validator
/// waiting over all three claims, from before the first, still sees every record.
#[test]
fn a_validator_sees_writers_whose_slots_were_reused() {
    loom::model(|| {
        let sequencer = Arc::new(Sequencer::new(0, 2));
        let table = Arc::new(ConflictTable::new(4));
        let first = sequencer.claim(1).unwrap();
        let second = sequencer.claim(1).unwrap();
        let third = sequencer.claim(1).unwrap();
        let (writer_sequencer, writer_table) = (Arc::clone(&sequencer), Arc::clone(&table));
        let writer = loom::thread::spawn(move || {
            writer_table.record(7, 1);
            writer_sequencer.publish(&first);
            writer_sequencer.complete(first);
            writer_table.record_range(3);
            writer_sequencer.publish(&third);
            writer_sequencer.complete(third);
        });
        table.record(9, 2);
        sequencer.publish(&second);
        sequencer.complete(second);
        sequencer.wait_published(0, 3);
        assert!(table.newest(7) >= 1);
        assert!(table.newest_range() >= 3);
        writer.join().unwrap();
    });
}

/// A slot reused by a later claim must not let the validator skip an earlier claim that has not
/// published: the claim between them belongs to a third thread.
#[test]
fn a_reused_slot_never_skips_another_writers_claim() {
    loom::model(|| {
        let sequencer = Arc::new(Sequencer::new(0, 2));
        let table = Arc::new(ConflictTable::new(4));
        let first = sequencer.claim(1).unwrap();
        let second = sequencer.claim(1).unwrap();
        let third = sequencer.claim(1).unwrap();
        let (one_sequencer, one_table) = (Arc::clone(&sequencer), Arc::clone(&table));
        let one = loom::thread::spawn(move || {
            one_sequencer.publish(&first);
            one_sequencer.complete(first);
            one_table.record(5, 3);
            one_sequencer.publish(&third);
            one_sequencer.complete(third);
        });
        let (two_sequencer, two_table) = (Arc::clone(&sequencer), Arc::clone(&table));
        let two = loom::thread::spawn(move || {
            two_table.record(9, 2);
            two_sequencer.publish(&second);
            two_sequencer.complete(second);
        });
        sequencer.wait_published(0, 3);
        assert!(table.newest(9) >= 2);
        one.join().unwrap();
        two.join().unwrap();
    });
}
