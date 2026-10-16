use super::split::boundaries;

fn keys(keys: &[&[u8]]) -> Vec<Vec<u8>> {
    keys.iter().map(|key| key.to_vec()).collect()
}

#[test]
fn a_compaction_splits_where_its_input_tables_start_and_no_more_often_than_asked() {
    let starts = || keys(&[b"g", b"a", b"c", b"c", b"e", b"k"]);
    assert_eq!(
        boundaries(starts(), 4),
        keys(&[b"e", b"g", b"k"]),
        "three cuts among c, e, g, k"
    );
    assert_eq!(
        boundaries(starts(), 8),
        keys(&[b"c", b"e", b"g", b"k"]),
        "every start but the least"
    );
    assert_eq!(boundaries(starts(), 2), keys(&[b"g"]));
    assert!(boundaries(starts(), 1).is_empty(), "one part, no cut");
    assert!(
        boundaries(keys(&[b"c", b"c"]), 4).is_empty(),
        "tables that all start at one key are not split"
    );
}
