//! The steps a range request takes over given levels.

use tokio::sync::oneshot;

use super::super::pick::Busy;
use super::super::pick_tests::{SHAPE, ids, levels, table};
use super::{Active, Request, Step};
use crate::CompactionStyle;

fn request(start: Option<&[u8]>, end: Option<&[u8]>, fresh_from: u64) -> Active {
    let (done, _) = oneshot::channel();
    let request = Request {
        start: start.map(<[u8]>::to_vec),
        end: end.map(<[u8]>::to_vec),
        done,
    };
    Active::new(request, fresh_from)
}

/// Level 0 is newer than every level below it: it lands at the first level holding data when
/// that is above the base level, never below it.
#[test]
fn level_zero_lands_no_lower_than_the_first_level_holding_data() {
    let tables = levels(vec![
        table(9, 0, b"a", b"z", 10),
        table(5, 3, b"a", b"m", 1000),
        table(1, 6, b"a", b"z", 10_000),
    ]);
    let active = request(None, None, 10);
    let Step::Run(pick, next) =
        active.next(&tables, CompactionStyle::Leveled, SHAPE, &Busy::default())
    else {
        panic!("level 0 touches the range");
    };
    assert_eq!((pick.output_level, next), (3, 3));
    assert_eq!((ids(&pick.inputs), ids(&pick.overlaps)), (vec![9], vec![5]));
}

/// The bottom tables a range touches are rewritten in place, one table included, unless they
/// were written after the request began; tables outside the range stay.
#[test]
fn bottom_tables_in_the_range_are_rewritten_in_place() {
    let tables = levels(vec![
        table(1, 6, b"a", b"c", 100),
        table(2, 6, b"d", b"f", 100),
        table(7, 6, b"g", b"i", 100),
    ]);
    let active = request(Some(b"b"), Some(b"e"), 5);
    let Step::Run(pick, _) =
        active.next(&tables, CompactionStyle::Leveled, SHAPE, &Busy::default())
    else {
        panic!("bottom tables touch the range");
    };
    assert_eq!((pick.output_level, ids(&pick.inputs)), (6, vec![1, 2]));
    let one = request(Some(b"a"), Some(b"b"), 5);
    let Step::Run(single, _) = one.next(&tables, CompactionStyle::Leveled, SHAPE, &Busy::default())
    else {
        panic!("one bottom table touches the range");
    };
    assert!(
        !single.is_move(),
        "a table rewritten in its level is not a move"
    );
    let fresh = request(Some(b"g"), None, 5);
    assert!(matches!(
        fresh.next(&tables, CompactionStyle::Leveled, SHAPE, &Busy::default()),
        Step::Done
    ));
}

/// A step whose tables a running compaction holds waits.
#[test]
fn a_step_waits_while_its_tables_are_busy() {
    let tables = levels(vec![table(1, 6, b"a", b"c", 100)]);
    let busy = Busy {
        tables: vec![1],
        ranges: Vec::new(),
    };
    let active = request(None, None, 5);
    assert!(matches!(
        active.next(&tables, CompactionStyle::Leveled, SHAPE, &busy),
        Step::Blocked
    ));
}
