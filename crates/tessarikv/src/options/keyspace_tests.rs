//! Keyspace options: defaults, the bounds each field accepts, and the order of the level-0
//! triggers.

use tessarikv_constants::{self as limits, LEVELS};
use tessarikv_format::Compression;

use super::KeyspaceOptions;
use super::tests::{Edge, assert_refused, narrow};

#[test]
fn keyspace_defaults_are_the_documented_constants() {
    let options = KeyspaceOptions::default();
    assert_eq!(options.memtable_bytes(), limits::DEFAULT_MEMTABLE_BYTES);
    assert_eq!(
        options.max_immutable_memtables(),
        limits::DEFAULT_MAX_IMMUTABLE_MEMTABLES
    );
    assert_eq!(options.block_bytes(), limits::DEFAULT_BLOCK_BYTES);
    assert_eq!(
        options.block_restart_interval(),
        limits::DEFAULT_BLOCK_RESTART_INTERVAL
    );
    assert_eq!(
        options.bloom_bits_per_key(),
        limits::DEFAULT_BLOOM_BITS_PER_KEY
    );
    assert_eq!(options.prefix_length(), None, "no prefix filter");
    assert_eq!(
        options.level0_compaction_trigger(),
        limits::DEFAULT_LEVEL0_COMPACTION_TRIGGER
    );
    assert_eq!(options.level_base_bytes(), limits::DEFAULT_LEVEL_BASE_BYTES);
    assert_eq!(options.max_compactions(), limits::DEFAULT_MAX_COMPACTIONS);
    assert_eq!(
        options.max_subcompactions(),
        limits::DEFAULT_MAX_SUBCOMPACTIONS
    );
    assert_eq!(
        options.soft_pending_compaction_bytes(),
        limits::DEFAULT_SOFT_PENDING_COMPACTION_BYTES
    );
    assert_eq!(
        options.hard_pending_compaction_bytes(),
        limits::DEFAULT_HARD_PENDING_COMPACTION_BYTES
    );
    assert_eq!(
        options.level_size_multiplier(),
        limits::DEFAULT_LEVEL_SIZE_MULTIPLIER
    );
    assert_eq!(
        options.target_file_bytes(),
        limits::DEFAULT_TARGET_FILE_BYTES
    );
}

#[test]
fn keyspace_bounds_accept_the_edges_and_refuse_past_them() {
    let edges: [Edge<KeyspaceOptions>; 14] = [
        (
            "keyspace.memtable_bytes",
            limits::MIN_MEMTABLE_BYTES,
            limits::MAX_MEMTABLE_BYTES,
            |value| KeyspaceOptions::builder().memtable_bytes(value).build(),
        ),
        (
            "keyspace.max_immutable_memtables",
            1,
            u64::from(limits::MAX_IMMUTABLE_MEMTABLES),
            |value| {
                KeyspaceOptions::builder()
                    .max_immutable_memtables(narrow(value))
                    .build()
            },
        ),
        (
            "keyspace.block_bytes",
            limits::MIN_BLOCK_BYTES,
            limits::MAX_BLOCK_BYTES,
            |value| KeyspaceOptions::builder().block_bytes(value).build(),
        ),
        (
            "keyspace.block_restart_interval",
            1,
            u64::from(limits::MAX_BLOCK_RESTART_INTERVAL),
            |value| {
                KeyspaceOptions::builder()
                    .block_restart_interval(narrow(value))
                    .build()
            },
        ),
        (
            "keyspace.bloom_bits_per_key",
            0,
            u64::from(limits::MAX_BLOOM_BITS_PER_KEY),
            |value| {
                KeyspaceOptions::builder()
                    .bloom_bits_per_key(narrow(value))
                    .build()
            },
        ),
        (
            "keyspace.prefix_length",
            1,
            u64::from(limits::MAX_PREFIX_LENGTH),
            |value| {
                KeyspaceOptions::builder()
                    .prefix_length(narrow(value))
                    .build()
            },
        ),
        (
            "keyspace.level0_compaction_trigger",
            1,
            u64::from(limits::MAX_LEVEL0_COMPACTION_TRIGGER),
            |value| {
                KeyspaceOptions::builder()
                    .level0_compaction_trigger(narrow(value))
                    // Above the compaction trigger at any value it takes.
                    .level0_slowdown_trigger(limits::MAX_LEVEL0_STALL_TRIGGER)
                    .level0_stop_trigger(limits::MAX_LEVEL0_STALL_TRIGGER)
                    .build()
            },
        ),
        (
            "keyspace.max_compactions",
            1,
            u64::from(limits::MAX_COMPACTIONS),
            |value| {
                KeyspaceOptions::builder()
                    .max_compactions(narrow(value))
                    .build()
            },
        ),
        (
            "keyspace.hard_pending_compaction_bytes",
            limits::MIN_PENDING_COMPACTION_BYTES,
            limits::MAX_PENDING_COMPACTION_BYTES,
            |value| {
                KeyspaceOptions::builder()
                    .soft_pending_compaction_bytes(limits::MIN_PENDING_COMPACTION_BYTES)
                    .hard_pending_compaction_bytes(value)
                    .build()
            },
        ),
        (
            "keyspace.soft_pending_compaction_bytes",
            limits::MIN_PENDING_COMPACTION_BYTES,
            limits::MAX_PENDING_COMPACTION_BYTES,
            |value| {
                KeyspaceOptions::builder()
                    .hard_pending_compaction_bytes(limits::MAX_PENDING_COMPACTION_BYTES)
                    .soft_pending_compaction_bytes(value)
                    .build()
            },
        ),
        (
            "keyspace.max_subcompactions",
            1,
            u64::from(limits::MAX_SUBCOMPACTIONS),
            |value| {
                KeyspaceOptions::builder()
                    .max_subcompactions(narrow(value))
                    .build()
            },
        ),
        (
            "keyspace.level_base_bytes",
            limits::MIN_LEVEL_BASE_BYTES,
            limits::MAX_LEVEL_BASE_BYTES,
            |value| KeyspaceOptions::builder().level_base_bytes(value).build(),
        ),
        (
            "keyspace.level_size_multiplier",
            u64::from(limits::MIN_LEVEL_SIZE_MULTIPLIER),
            u64::from(limits::MAX_LEVEL_SIZE_MULTIPLIER),
            |value| {
                KeyspaceOptions::builder()
                    .level_size_multiplier(narrow(value))
                    .build()
            },
        ),
        (
            "keyspace.target_file_bytes",
            limits::MIN_TARGET_FILE_BYTES,
            limits::MAX_TARGET_FILE_BYTES,
            |value| KeyspaceOptions::builder().target_file_bytes(value).build(),
        ),
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

#[test]
fn level_zero_triggers_must_rise_from_compaction_to_slowdown_to_stop() {
    let triggers = |compaction, slowdown, stop| {
        KeyspaceOptions::builder()
            .level0_compaction_trigger(compaction)
            .level0_slowdown_trigger(slowdown)
            .level0_stop_trigger(stop)
            .build()
    };
    assert!(triggers(4, 4, 4).is_ok(), "equal triggers are allowed");
    assert!(triggers(4, 20, 36).is_ok());
    assert_refused(triggers(4, 3, 36), "keyspace.level0_slowdown_trigger");
    assert_refused(triggers(4, 20, 19), "keyspace.level0_stop_trigger");
    let most = limits::MAX_LEVEL0_STALL_TRIGGER;
    assert!(triggers(4, most, most).is_ok());
    assert_refused(triggers(4, 20, most + 1), "keyspace.level0_stop_trigger");
    let options = KeyspaceOptions::default();
    assert_eq!(
        options.level0_slowdown_trigger(),
        limits::DEFAULT_LEVEL0_SLOWDOWN_TRIGGER
    );
    assert_eq!(
        options.level0_stop_trigger(),
        limits::DEFAULT_LEVEL0_STOP_TRIGGER
    );
}

#[test]
fn compaction_may_not_owe_more_before_writes_slow_than_before_they_stop() {
    let refused = KeyspaceOptions::builder()
        .soft_pending_compaction_bytes(2 << 30)
        .hard_pending_compaction_bytes(1 << 30)
        .build();
    assert_refused(refused, "keyspace.soft_pending_compaction_bytes");
    assert!(
        KeyspaceOptions::builder()
            .soft_pending_compaction_bytes(1 << 30)
            .hard_pending_compaction_bytes(1 << 30)
            .build()
            .is_ok(),
        "equal limits"
    );
}

#[test]
fn tables_compress_with_lz4_and_the_bottom_level_like_the_others_by_default() {
    let options = KeyspaceOptions::default();
    assert_eq!(options.compression(), Compression::Lz4);
    assert_eq!(options.bottommost_compression(), None);
    for level in 0..LEVELS {
        assert_eq!(
            options.compression_at(level),
            Compression::Lz4,
            "level {level}"
        );
    }
}

#[test]
fn the_bottom_level_takes_its_own_codec_and_every_other_level_the_keyspace_codec() {
    let options = KeyspaceOptions::builder()
        .compression(Compression::Snappy)
        .bottommost_compression(Some(Compression::Zstd))
        .build()
        .expect("valid");
    let chosen: Vec<_> = (0..LEVELS)
        .map(|level| options.compression_at(level))
        .collect();
    let mut expected = vec![Compression::Snappy; LEVELS - 1];
    expected.push(Compression::Zstd);
    assert_eq!(chosen, expected);
}

#[test]
fn a_deflate_level_above_ten_is_refused_for_either_codec_option() {
    let deflate = |level| Compression::Deflate { level };
    let top = KeyspaceOptions::builder().compression(deflate(limits::MAX_DEFLATE_LEVEL));
    let bottom = KeyspaceOptions::builder().bottommost_compression(Some(deflate(0)));
    assert!(top.build().is_ok() && bottom.build().is_ok());
    let past = limits::MAX_DEFLATE_LEVEL + 1;
    assert_refused(
        KeyspaceOptions::builder()
            .compression(deflate(past))
            .build(),
        "keyspace.compression.level",
    );
    assert_refused(
        KeyspaceOptions::builder()
            .bottommost_compression(Some(deflate(past)))
            .build(),
        "keyspace.bottommost_compression.level",
    );
}
