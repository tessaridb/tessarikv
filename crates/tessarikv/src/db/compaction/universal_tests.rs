use super::below::deeper;
use super::pick::{Busy, Pick};
use super::pick_tests::{ids, levels, table};
use super::universal::{Settings, pick};

const SETTINGS: Settings = Settings {
    level0_trigger: 4,
    size_ratio_percent: 1,
    min_merge: 2,
    max_size_amplification_percent: 200,
};

/// Level-0 runs of `sizes`, newest first (the highest id), above a bottom of `bottom` bytes.
fn runs(sizes: &[u64], bottom: u64) -> [Vec<std::sync::Arc<tessarikv_format::TableMeta>>; 7] {
    let count = u64::try_from(sizes.len()).unwrap();
    let mut tables: Vec<_> = sizes
        .iter()
        .zip((1..=count).rev())
        .map(|(size, id)| table(id, 0, b"a", b"z", *size))
        .collect();
    if bottom > 0 {
        tables.push(table(100, 6, b"a", b"z", bottom));
    }
    levels(tables)
}

#[test]
fn nothing_merges_under_the_trigger_or_while_a_run_is_busy() {
    assert!(pick(&runs(&[10, 10, 10], 1000), SETTINGS, &Busy::default()).is_none());
    let mut busy = Busy::default();
    busy.tables.push(4);
    assert!(pick(&runs(&[10, 10, 10, 10], 1000), SETTINGS, &busy).is_none());
}

#[test]
fn everything_merges_into_the_bottom_past_the_amplification_bound_or_onto_an_empty_one() {
    let first = pick(&runs(&[10, 10, 10, 10], 0), SETTINGS, &Busy::default()).unwrap();
    assert_eq!((first.output_level, ids(&first.inputs).len()), (6, 4));
    // 210 above a bottom of 100 is 210 %, past the 200 % bound.
    let full = pick(&runs(&[150, 30, 20, 10], 100), SETTINGS, &Busy::default()).unwrap();
    assert_eq!(full.output_level, 6);
    assert_eq!(ids(&full.inputs), [4, 3, 2, 1]);
    assert_eq!(ids(&full.overlaps), [100]);
}

#[test]
fn the_newest_runs_merge_by_size_or_else_enough_of_them_to_go_under_the_trigger() {
    // By size four runs; by count it would be 5 - 4 + 2 = 3.
    let similar = pick(
        &runs(&[10, 10, 10, 10, 50], 10_000),
        SETTINGS,
        &Busy::default(),
    )
    .unwrap();
    assert_eq!(similar.output_level, 0);
    assert_eq!(
        ids(&similar.inputs),
        [5, 4, 3, 2],
        "within one percent of those before"
    );
    // Each run is ten times the last: by size only one; by count 5 - 4 + 2 = 3 of them.
    let growing = pick(
        &runs(&[1, 10, 100, 1000, 1000], 100_000),
        SETTINGS,
        &Busy::default(),
    )
    .unwrap();
    assert_eq!(growing.output_level, 0);
    assert_eq!(ids(&growing.inputs), [5, 4, 3]);
}

#[test]
fn the_count_rule_merges_the_cheapest_adjacent_runs_not_always_the_newest() {
    // By size only the newest run; by count two of them. The newest pair costs 37 bytes, the
    // oldest pair 22: merging the newest would rewrite the large second run on every flush.
    let pick = pick(&runs(&[7, 30, 10, 12], 10_000), SETTINGS, &Busy::default()).unwrap();
    assert_eq!(pick.output_level, 0);
    assert_eq!(ids(&pick.inputs), [2, 1]);
}

#[test]
fn a_merge_within_level_zero_counts_the_runs_it_leaves_out_as_deeper() {
    let levels = runs(&[10, 10, 10, 10], 1000);
    let merge = Pick {
        output_level: 0,
        inputs: levels[0][..2].to_vec(),
        overlaps: Vec::new(),
        drop: false,
    };
    let below: Vec<Vec<u64>> = deeper(&merge, &levels, &[4, 3])
        .iter()
        .map(|tables| ids(tables))
        .collect();
    assert_eq!(
        below,
        [
            vec![2],
            vec![1],
            vec![],
            vec![],
            vec![],
            vec![],
            vec![],
            vec![100]
        ],
        "the two older runs, then levels 1 to 6"
    );
}
