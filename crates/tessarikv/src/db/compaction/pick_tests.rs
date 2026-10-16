use std::sync::Arc;

use tessarikv_constants::LEVELS;
use tessarikv_format::TableMeta;
use tessarikv_types::{SequenceNumber, Trailer, ValueKind, encode_internal};

use super::pick::{Shape, pick};

const SHAPE: Shape = Shape {
    level0_trigger: 4,
    base_bytes: 100,
    multiplier: 10,
};

/// A table numbered `id` holding user keys `smallest..=largest` in `size` bytes.
fn table(id: u64, level: u8, smallest: &[u8], largest: &[u8], size: u64) -> Arc<TableMeta> {
    let internal = |key: &[u8]| {
        let mut out = Vec::new();
        encode_internal(
            key,
            Trailer::new(SequenceNumber::new(id).unwrap(), ValueKind::Value),
            &mut out,
        );
        out
    };
    Arc::new(TableMeta {
        keyspace: 0,
        level,
        id,
        size,
        smallest: internal(smallest),
        largest: internal(largest),
        min_sequence: id,
        max_sequence: id,
    })
}

fn levels(tables: Vec<Arc<TableMeta>>) -> [Vec<Arc<TableMeta>>; LEVELS] {
    let mut levels: [Vec<Arc<TableMeta>>; LEVELS] = Default::default();
    for table in tables {
        levels[usize::from(table.level)].push(table);
    }
    levels
}

fn ids(tables: &[Arc<TableMeta>]) -> Vec<u64> {
    tables.iter().map(|table| table.id).collect()
}

#[test]
fn level_zero_waits_for_its_trigger_then_goes_whole_to_the_bottom_of_an_empty_keyspace() {
    let three = levels(vec![
        table(3, 0, b"c", b"f", 10),
        table(2, 0, b"a", b"b", 10),
        table(1, 0, b"m", b"p", 10),
    ]);
    assert!(
        pick(&three, SHAPE).is_none(),
        "three tables are below a trigger of four"
    );
    let mut four = three;
    four[0].insert(0, table(4, 0, b"x", b"z", 10));
    four[6].push(table(9, 6, b"a", b"c", 30));
    four[6].push(table(10, 6, b"g", b"l", 30));
    four[6].push(table(11, 6, b"n", b"o", 30));
    let chosen = pick(&four, SHAPE).expect("four tables reach the trigger");
    assert_eq!((chosen.inputs[0].level, chosen.output_level), (0, 6));
    assert_eq!(ids(&chosen.inputs), [4, 3, 2, 1]);
    assert_eq!(
        ids(&chosen.overlaps),
        [9, 10, 11],
        "every bottom table the range a..z touches"
    );
    assert!(!chosen.is_move());
}

#[test]
fn the_base_level_rises_as_the_bottom_grows_and_a_level_over_its_target_compacts() {
    // Bottom 5000: 5000 → 500 (L5) → 50 (L4) ≤ 100, so L4 is the base with target 50, L5 500.
    let mut shape = levels(vec![
        table(1, 4, b"a", b"c", 30),
        table(2, 4, b"k", b"m", 30),
        table(3, 5, b"a", b"b", 200),
        table(4, 5, b"c", b"d", 200),
        table(5, 5, b"l", b"l", 10),
        table(6, 6, b"a", b"z", 5000),
    ]);
    let chosen = pick(&shape, SHAPE).expect("L4 holds 60 against a target of 50");
    assert_eq!((chosen.inputs[0].level, chosen.output_level), (4, 5));
    assert_eq!(
        ids(&chosen.inputs),
        [2],
        "k..m touches 10 bytes per 30, a..c touches 400 per 30"
    );
    assert_eq!(ids(&chosen.overlaps), [5]);
    shape[4].pop();
    assert!(
        pick(&shape, SHAPE).is_none(),
        "L4 at 30 of 50 and L5 at 410 of 500"
    );
}

#[test]
fn level_zero_stops_at_a_non_empty_level_above_the_base_and_that_level_drains_first() {
    let mut tables = vec![table(9, 2, b"a", b"b", 5), table(10, 6, b"a", b"z", 50)];
    tables.extend((1..=4).map(|id| table(id, 0, b"c", b"d", 1)));
    let mut shape = levels(tables);
    shape[0].reverse();
    let chosen = pick(&shape, SHAPE).expect("a non-empty level above the base scores highest");
    assert_eq!(
        (chosen.inputs[0].level, chosen.output_level),
        (2, 3),
        "level 2 is above the base, level 6"
    );
    assert!(chosen.is_move(), "nothing in level 3 touches it");
    shape[2].clear();
    shape[1].push(table(11, 1, b"q", b"r", 5));
    let drains = pick(&shape, SHAPE).expect("level 1 now");
    assert_eq!(drains.inputs[0].level, 1);
    shape[1].clear();
    shape[3].push(table(12, 3, b"a", b"b", 5));
    let level_zero = pick(&shape, SHAPE).expect("level 3 above the base still drains first");
    assert_eq!(level_zero.inputs[0].level, 3);
    shape[3].clear();
    let whole = pick(&shape, SHAPE).expect("level 0 at its trigger");
    assert_eq!((whole.inputs[0].level, whole.output_level), (0, 6));
}
