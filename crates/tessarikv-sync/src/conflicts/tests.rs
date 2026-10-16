use super::ConflictTable;

#[test]
fn an_unwritten_key_reads_zero() {
    let table = ConflictTable::new(64);
    assert_eq!(table.newest(42), 0);
    assert_eq!(table.newest_range(), 0);
}

#[test]
fn a_bucket_keeps_the_highest_sequence_recorded() {
    let table = ConflictTable::new(64);
    table.record(42, 7);
    table.record(42, 3);
    assert_eq!(table.newest(42), 7);
    table.record(42, 9);
    assert_eq!(table.newest(42), 9);
    table.record_range(5);
    table.record_range(4);
    assert_eq!(table.newest_range(), 5);
}

/// A bucket is an upper bound for every hash in it: never below what was recorded for the hash.
#[test]
fn a_hash_never_reads_below_its_own_record() {
    let table = ConflictTable::new(4);
    for (sequence, hash) in (1_u64..=200).zip((0_u64..).map(|n| n.wrapping_mul(0x2545_f491))) {
        table.record(hash, sequence);
        assert!(table.newest(hash) >= sequence);
    }
}

/// Hashes spread over the buckets: a thousand distinct hashes into 1 024 buckets leave most
/// buckets empty, so a key rarely reads another key's commit.
#[test]
fn distinct_hashes_spread_over_the_buckets() {
    let table = ConflictTable::new(1024);
    for n in 1_u64..=1000 {
        table.record(n.wrapping_mul(0x0123_4567_89ab_cdef), 1);
    }
    let occupied = (0_u64..100_000)
        .map(|n| n.wrapping_mul(0xfedc_ba98_7654_3211))
        .filter(|&hash| table.newest(hash) != 0)
        .count();
    // Share of occupied buckets: 1 - (1 - 1/1024)^1000 ≈ 0.62 of the probes.
    assert!((50_000..75_000).contains(&occupied), "{occupied}");
}

/// Concurrent recorders: each bucket ends at the highest sequence any thread recorded for it.
#[test]
fn concurrent_records_keep_the_maximum() {
    let table = ConflictTable::new(16);
    // OS threads the scheduler can preempt at any instruction, joined (and their panics
    // propagated) when the scope ends.
    std::thread::scope(|scope| {
        for thread in 0_u64..8 {
            let table = &table;
            scope.spawn(move || {
                for step in 0_u64..10_000 {
                    table.record(step % 32, step * 8 + thread + 1);
                }
            });
        }
    });
    // The last step recording hash h is the largest step ≡ h (mod 32); thread 7 adds 8.
    for hash in 0_u64..32 {
        let last_step = 9_999 - (9_999 - hash) % 32;
        assert!(table.newest(hash) >= last_step * 8 + 8, "{hash}");
    }
}
