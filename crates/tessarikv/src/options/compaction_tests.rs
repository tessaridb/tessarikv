//! Compaction styles: first in, first out keeps a bounded number of bytes, and universal bounds
//! its ratio, its run count and its amplification.

use tessarikv_constants as limits;

use super::KeyspaceOptions;
use super::tests::assert_refused;

#[test]
fn a_first_in_first_out_keyspace_keeps_a_bounded_number_of_bytes() {
    assert_eq!(
        KeyspaceOptions::default().compaction(),
        crate::CompactionStyle::Leveled
    );
    let fifo = |max_table_bytes| {
        KeyspaceOptions::builder()
            .compaction(crate::CompactionStyle::Fifo { max_table_bytes })
            .build()
    };
    for edge in [
        limits::MIN_FIFO_MAX_TABLE_BYTES,
        limits::MAX_FIFO_MAX_TABLE_BYTES,
        limits::DEFAULT_FIFO_MAX_TABLE_BYTES,
    ] {
        assert!(fifo(edge).is_ok(), "{edge}");
    }
    assert_refused(
        fifo(limits::MIN_FIFO_MAX_TABLE_BYTES - 1),
        "keyspace.compaction.max_table_bytes",
    );
    assert_refused(
        fifo(limits::MAX_FIFO_MAX_TABLE_BYTES + 1),
        "keyspace.compaction.max_table_bytes",
    );
    assert_eq!(
        crate::CompactionStyle::fifo(),
        crate::CompactionStyle::Fifo {
            max_table_bytes: limits::DEFAULT_FIFO_MAX_TABLE_BYTES
        }
    );
}

#[test]
fn universal_compaction_bounds_its_ratio_its_run_count_and_its_amplification() {
    let universal = |size_ratio_percent, min_merge, max_size_amplification_percent| {
        KeyspaceOptions::builder()
            .compaction(crate::CompactionStyle::Universal {
                size_ratio_percent,
                min_merge,
                max_size_amplification_percent,
            })
            .build()
    };
    assert!(
        KeyspaceOptions::builder()
            .compaction(crate::CompactionStyle::universal())
            .build()
            .is_ok()
    );
    assert!(universal(0, 2, limits::MIN_UNIVERSAL_MAX_SIZE_AMPLIFICATION_PERCENT).is_ok());
    assert!(
        universal(
            limits::MAX_UNIVERSAL_SIZE_RATIO_PERCENT,
            limits::MAX_UNIVERSAL_MIN_MERGE,
            limits::MAX_UNIVERSAL_MAX_SIZE_AMPLIFICATION_PERCENT
        )
        .is_ok()
    );
    assert_refused(
        universal(limits::MAX_UNIVERSAL_SIZE_RATIO_PERCENT + 1, 2, 200),
        "keyspace.compaction.size_ratio_percent",
    );
    assert_refused(universal(1, 1, 200), "keyspace.compaction.min_merge");
    assert_refused(
        universal(1, limits::MAX_UNIVERSAL_MIN_MERGE + 1, 200),
        "keyspace.compaction.min_merge",
    );
    assert_refused(
        universal(
            1,
            2,
            limits::MIN_UNIVERSAL_MAX_SIZE_AMPLIFICATION_PERCENT - 1,
        ),
        "keyspace.compaction.max_size_amplification_percent",
    );
    assert_refused(
        universal(
            1,
            2,
            limits::MAX_UNIVERSAL_MAX_SIZE_AMPLIFICATION_PERCENT + 1,
        ),
        "keyspace.compaction.max_size_amplification_percent",
    );
}
