//! What the levels ask of writers: every keyspace weighed by its own triggers and limits, the
//! worst one winning.

use std::sync::Arc;

use tessarikv_format::TableMeta;
use tessarikv_types::{SequenceNumber, ValueKind};

use super::block_cache::BlockCache;
use super::pressure::{Pressure, of};
use super::read_tests::{file, user_key};
use super::view::{TableFile, View};
use crate::memtable::Memtable;
use crate::{CompactionStyle, KeyspaceOptions};

const MIB: u64 = 1 << 20;

/// `count` level-0 tables of `keyspace`, newest first, each claiming `size` bytes, numbered from
/// `first`.
fn level0(keyspace: u32, count: u64, first: u64, size: u64) -> Vec<TableFile> {
    let cache = Arc::new(BlockCache::new(0, 256));
    (first..first.saturating_add(count))
        .rev()
        .map(|id| {
            let table = Memtable::new();
            table
                .insert(
                    SequenceNumber::new(id).expect("a sequence"),
                    ValueKind::Value,
                    &user_key(1),
                    b"v",
                )
                .expect("inserted");
            let written = file(&table, id, &cache);
            TableFile {
                meta: Arc::new(TableMeta {
                    keyspace,
                    size,
                    ..(*written.meta).clone()
                }),
                ..written
            }
        })
        .collect()
}

/// A view of the default keyspace and keyspace 1 holding `files`, keyspace by keyspace.
fn view(files: Vec<TableFile>) -> View {
    let names = Arc::from(vec![(1, "users".to_owned())]);
    View::recovered(
        vec![(0, Memtable::new()), (1, Memtable::new())],
        names,
        files,
    )
}

fn triggers(slowdown: u32, stop: u32) -> KeyspaceOptions {
    KeyspaceOptions::builder()
        .level0_compaction_trigger(2)
        .level0_slowdown_trigger(slowdown)
        .level0_stop_trigger(stop)
        .build()
        .expect("valid")
}

/// The same level 0 weighs differently by each keyspace's own triggers, and the worst keyspace
/// decides.
#[test]
fn each_keyspace_is_weighed_by_its_own_triggers_and_the_worst_wins() {
    let (loose, tight) = (triggers(8, 9), triggers(3, 4));
    let levels = view([level0(0, 3, 1, 1), level0(1, 3, 10, 1)].concat());
    let weighed = |default, users| of(&levels, [(0, default), (1, users)]).pressure;
    assert_eq!(weighed(&loose, &loose), Pressure::None);
    assert_eq!(weighed(&loose, &tight), Pressure::SlowLevel0);
    assert_eq!(weighed(&tight, &loose), Pressure::SlowLevel0);
    let stopped = view([level0(0, 1, 1, 1), level0(1, 4, 10, 1)].concat());
    assert_eq!(
        of(&stopped, [(0, &tight), (1, &tight)]).pressure,
        Pressure::StopLevel0,
        "keyspace 1 at its stop trigger holds every write"
    );
}

/// A first-in-first-out keyspace keeps every table at level 0, so its level 0 holds nothing.
#[test]
fn a_first_in_first_out_keyspace_puts_no_level_zero_pressure() {
    let fifo = KeyspaceOptions::builder()
        .compaction(CompactionStyle::Fifo {
            max_table_bytes: 1 << 30,
        })
        .level0_slowdown_trigger(4)
        .level0_stop_trigger(5)
        .build()
        .expect("valid");
    let levels = view(level0(1, 6, 1, 1));
    let load = of(&levels, [(0, &triggers(3, 4)), (1, &fifo)]);
    assert_eq!(load.pressure, Pressure::None);
}

/// The bytes compaction owes a keyspace are weighed against its own soft and hard limits.
#[test]
fn owed_bytes_slow_past_a_keyspace_soft_limit_and_stop_past_its_hard_one() {
    let limited = KeyspaceOptions::builder()
        .level0_compaction_trigger(2)
        .soft_pending_compaction_bytes(MIB)
        .hard_pending_compaction_bytes(4 * MIB)
        .build()
        .expect("valid");
    let roomy = triggers(20, 36);
    let slowed = view(level0(1, 2, 1, MIB));
    let load = of(&slowed, [(0, &roomy), (1, &limited)]);
    assert_eq!(load.pressure, Pressure::SlowOwed);
    assert_eq!(load.owed, 2 * MIB, "level 0 at its trigger owes its bytes");
    assert_eq!(
        of(&slowed, [(0, &roomy), (1, &roomy)]).pressure,
        Pressure::None,
        "the same bytes are under the default limits"
    );
    let stopped = view(level0(1, 4, 1, MIB));
    assert_eq!(
        of(&stopped, [(0, &roomy), (1, &limited)]).pressure,
        Pressure::StopOwed
    );
}

/// Every pressure reads back from the code it is published as.
#[test]
fn every_pressure_reads_back_from_its_code() {
    for pressure in [
        Pressure::None,
        Pressure::SlowOwed,
        Pressure::SlowLevel0,
        Pressure::StopOwed,
        Pressure::StopLevel0,
    ] {
        assert_eq!(Pressure::from_code(pressure.code()), pressure);
    }
}
