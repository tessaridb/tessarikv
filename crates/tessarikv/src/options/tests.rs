use tessarikv_constants as limits;

use super::{DbOptions, Durability, KeyspaceOptions, ReadOptions, StorageMode, WriteOptions};
use crate::{Error, ErrorCategory};

/// Asserts that `result` is the refusal of exactly `option`.
pub(super) fn assert_refused<T: std::fmt::Debug>(result: crate::Result<T>, option: &str) {
    match result {
        Err(error @ Error::InvalidOptions { .. }) => {
            assert_eq!(error.category(), ErrorCategory::Validation);
            assert_eq!(error.code(), "invalid_options");
            let Error::InvalidOptions { option: named, .. } = error else {
                unreachable!()
            };
            assert_eq!(named, option);
        }
        other => panic!("expected `{option}` to be refused, got {other:?}"),
    }
}

/// One bounded option: its field path, its minimum and maximum, and a build with a given value.
pub(super) type Edge<T> = (&'static str, u64, u64, fn(u64) -> crate::Result<T>);

/// The u32 bounds under test are all far below `u32::MAX`, so saturating loses nothing.
pub(super) fn narrow(value: u64) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

#[test]
fn db_defaults_are_the_documented_constants() {
    let options = DbOptions::default();
    assert!(options.create_if_missing());
    assert!(!options.error_if_exists());
    assert_eq!(options.storage(), StorageMode::Os);
    assert_eq!(
        options.wal_segment_bytes(),
        limits::DEFAULT_WAL_SEGMENT_BYTES
    );
    assert_eq!(
        options.block_cache_bytes(),
        limits::DEFAULT_BLOCK_CACHE_BYTES
    );
    assert_eq!(
        options.background_threads(),
        limits::DEFAULT_BACKGROUND_THREADS
    );
    assert_eq!(options.compaction_bytes_per_second(), 0, "not paced");
}

#[test]
fn db_bounds_accept_the_edges_and_refuse_past_them() {
    let edges: [Edge<DbOptions>; 3] = [
        (
            "db.wal_segment_bytes",
            limits::MIN_WAL_SEGMENT_BYTES,
            limits::MAX_WAL_SEGMENT_BYTES,
            |value| DbOptions::builder().wal_segment_bytes(value).build(),
        ),
        (
            "db.background_threads",
            1,
            u64::from(limits::MAX_BACKGROUND_THREADS),
            |value| {
                DbOptions::builder()
                    .background_threads(narrow(value))
                    .build()
            },
        ),
        (
            "db.compaction_bytes_per_second",
            limits::MIN_COMPACTION_BYTES_PER_SECOND,
            limits::MAX_COMPACTION_BYTES_PER_SECOND,
            |value| {
                DbOptions::builder()
                    .compaction_bytes_per_second(value)
                    .build()
            },
        ),
    ];
    for (option, min, max, build) in edges {
        assert!(build(min).is_ok(), "{option} refused its minimum {min}");
        assert!(build(max).is_ok(), "{option} refused its maximum {max}");
        assert_refused(build(min - 1), option);
        assert_refused(build(max + 1), option);
    }
}

#[test]
fn a_database_that_must_not_exist_cannot_be_forbidden_from_being_created() {
    let result = DbOptions::builder()
        .create_if_missing(false)
        .error_if_exists(true)
        .build();
    assert_refused(result, "db.error_if_exists");
}

#[test]
fn presets_are_valid_and_differ_from_the_default_where_they_say() -> crate::Result<()> {
    for preset in [
        KeyspaceOptions::point_lookup(),
        KeyspaceOptions::write_heavy(),
        KeyspaceOptions::bulk_load(),
    ] {
        assert_eq!(preset.to_builder().build()?, preset);
    }
    assert!(
        KeyspaceOptions::point_lookup().bloom_bits_per_key() > limits::DEFAULT_BLOOM_BITS_PER_KEY
    );
    assert!(KeyspaceOptions::write_heavy().memtable_bytes() > limits::DEFAULT_MEMTABLE_BYTES);
    assert!(
        KeyspaceOptions::bulk_load().memtable_bytes()
            > KeyspaceOptions::write_heavy().memtable_bytes()
    );

    let in_memory = DbOptions::in_memory();
    assert_eq!(in_memory.storage(), StorageMode::Memory);
    assert_eq!(in_memory.to_builder().build()?, in_memory);
    Ok(())
}

#[test]
fn a_preset_can_be_adjusted_through_its_builder() -> crate::Result<()> {
    let adjusted = KeyspaceOptions::write_heavy()
        .to_builder()
        .block_bytes(16 * 1024)
        .build()?;
    assert_eq!(adjusted.block_bytes(), 16 * 1024);
    assert_eq!(
        adjusted.memtable_bytes(),
        KeyspaceOptions::write_heavy().memtable_bytes()
    );
    Ok(())
}

#[test]
fn per_call_defaults_are_safe() {
    let write = WriteOptions::default();
    assert_eq!(write.durability, Durability::Sync);
    assert!(!write.no_slowdown);
    let read = ReadOptions::default();
    assert!(read.verify_checksums);
    assert!(read.fill_cache);
}
