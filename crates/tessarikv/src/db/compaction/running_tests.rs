//! Several compactions at once: what running jobs hold is what the next pick avoids, and no
//! more start than the keyspace allows.

use super::Compactions;
use super::pick::pick_with;
use super::pick_tests::{SHAPE, ids, levels, table};
use crate::KeyspaceOptions;

#[test]
fn each_job_started_beside_running_ones_reads_and_writes_what_none_of_them_holds() {
    let shape = levels(vec![
        table(1, 4, b"a", b"c", 30),
        table(2, 4, b"k", b"m", 30),
        table(7, 4, b"t", b"u", 30),
        table(3, 5, b"a", b"b", 200),
        table(5, 5, b"l", b"l", 10),
        table(8, 5, b"t", b"t", 40),
        table(6, 6, b"a", b"z", 5000),
    ]);
    let mut compactions = Compactions::default();
    let mut started = Vec::new();
    for number in 0..3 {
        let pick = pick_with(&shape, SHAPE, &compactions.busy()).expect("level 4 over target");
        started.push(ids(&pick.inputs));
        compactions.running.push((number, pick));
    }
    assert_eq!(started, [[2], [7], [1]]);
    let busy = compactions.busy();
    let mut tables = busy.tables.clone();
    tables.sort_unstable();
    assert_eq!(
        tables,
        [1, 2, 3, 5, 7, 8],
        "inputs and the tables they overlap"
    );
    assert_eq!(
        busy.ranges,
        [
            (5, b"k".to_vec(), b"m".to_vec()),
            (5, b"t".to_vec(), b"u".to_vec()),
            (5, b"a".to_vec(), b"c".to_vec()),
        ],
        "the range each job writes in level 5"
    );
    assert!(
        pick_with(&shape, SHAPE, &busy).is_none(),
        "every table over target is taken"
    );
}

#[test]
fn no_more_jobs_start_than_the_keyspace_allows() {
    let shape = levels(vec![
        table(1, 4, b"a", b"c", 30),
        table(2, 4, b"k", b"m", 30),
        table(6, 6, b"a", b"z", 5000),
    ]);
    let mut compactions = Compactions::default();
    for number in 0..2 {
        let pick = pick_with(&shape, SHAPE, &compactions.busy()).expect("level 4 over target");
        compactions.running.push((number, pick));
    }
    let allowing = |most| {
        KeyspaceOptions::builder()
            .max_compactions(most)
            .build()
            .unwrap()
    };
    assert!(!compactions.has_room(&allowing(2)), "two run, two allowed");
    assert!(compactions.has_room(&allowing(3)), "two run, three allowed");
    assert!(
        Compactions::default().has_room(&allowing(1)),
        "none runs, one allowed"
    );
}
