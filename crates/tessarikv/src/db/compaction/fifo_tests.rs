use super::fifo::pick;
use super::pick::Busy;
use super::pick_tests::{ids, levels, table};

/// Five level-0 tables of ten bytes, newest (5) first.
fn five() -> [Vec<std::sync::Arc<tessarikv_format::TableMeta>>; tessarikv_constants::LEVELS] {
    levels(
        (1..=5)
            .rev()
            .map(|id| table(id, 0, b"a", b"z", 10))
            .collect(),
    )
}

#[test]
fn the_oldest_tables_go_until_the_rest_fit_and_nothing_goes_while_they_do() {
    assert!(
        pick(&five(), 50, &Busy::default()).is_none(),
        "fifty bytes fit"
    );
    let dropped = pick(&five(), 35, &Busy::default()).expect("fifteen bytes over");
    assert_eq!(ids(&dropped.inputs), [1, 2], "the two oldest");
    assert!(dropped.drop && !dropped.is_move(), "a drop, not a move");
    let one = pick(&five(), 40, &Busy::default()).expect("ten bytes over");
    assert_eq!(ids(&one.inputs), [1]);
    assert!(!one.is_move(), "dropping one table is still a drop");
    assert_eq!(
        ids(&pick(&five(), 9, &Busy::default()).expect("over").inputs),
        [1, 2, 3, 4, 5]
    );
    let mut busy = Busy::default();
    busy.tables.push(1);
    assert!(
        pick(&five(), 35, &busy).is_none(),
        "the oldest is being read"
    );
}
