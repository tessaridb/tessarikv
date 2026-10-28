//! The blob options of a keyspace: their defaults and the bounds each accepts.

use tessarikv_constants as limits;

use super::KeyspaceOptions;
use super::tests::{Edge, assert_refused, narrow};

#[test]
fn blob_options_default_to_values_in_their_tables() {
    let options = KeyspaceOptions::default();
    assert_eq!(options.blob_threshold(), None, "values stay in the tables");
    assert_eq!(
        options.blob_gc_age_cutoff_percent(),
        limits::DEFAULT_BLOB_GC_AGE_CUTOFF_PERCENT
    );
}

#[test]
fn blob_bounds_accept_the_edges_and_refuse_past_them() {
    let edges: [Edge<KeyspaceOptions>; 2] = [
        (
            "keyspace.blob_threshold",
            limits::MIN_BLOB_THRESHOLD,
            u64::try_from(limits::MAX_VALUE_BYTES).unwrap_or(u64::MAX),
            |value| KeyspaceOptions::builder().blob_threshold(value).build(),
        ),
        ("keyspace.blob_gc_age_cutoff_percent", 0, 100, |value| {
            KeyspaceOptions::builder()
                .blob_gc_age_cutoff_percent(narrow(value))
                .build()
        }),
    ];
    for (option, min, max, build) in edges {
        assert!(build(min).is_ok(), "{option} refused its minimum {min}");
        assert!(build(max).is_ok(), "{option} refused its maximum {max}");
        if min > 0 {
            assert_refused(build(min - 1), option);
        }
        assert_refused(build(max + 1), option);
    }
}
