//! The event listener: flushes and compactions reported once installed, stall changes reported
//! once each, and the failure that stops the database reported exactly once.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::log::fault::FaultStorage;
use crate::{
    CompactionCompleted, Db, DbOptions, Durability, Error, EventListener, FlushCompleted,
    KeyspaceId, KeyspaceOptions, StallCondition, WriteBatch, WriteOptions,
};

const PATH: &str = "/events";

/// What the listener was told, in order.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Heard {
    Flush(FlushCompleted),
    Compaction(CompactionCompleted),
    Stall(StallCondition),
    Failure(&'static str),
}

#[derive(Debug, Default)]
struct Recorder {
    heard: Mutex<Vec<Heard>>,
}

impl Recorder {
    fn heard(&self) -> Vec<Heard> {
        self.heard.lock().expect("unpoisoned").clone()
    }

    fn record(&self, heard: Heard) {
        self.heard.lock().expect("unpoisoned").push(heard);
    }
}

impl EventListener for Recorder {
    fn name(&self) -> &str {
        "recorder"
    }

    fn flush_completed(&self, event: &FlushCompleted) {
        self.record(Heard::Flush(*event));
    }

    fn compaction_completed(&self, event: &CompactionCompleted) {
        self.record(Heard::Compaction(*event));
    }

    fn stall_changed(&self, condition: StallCondition) {
        self.record(Heard::Stall(condition));
    }

    fn background_error(&self, error: &Error) {
        self.record(Heard::Failure(error.code()));
    }
}

/// Level 0 slows writes at two tables and stops them at four; nothing compacts on its own.
async fn open(storage: &FaultStorage, recorder: &Arc<Recorder>) -> Db {
    let keyspace = KeyspaceOptions::builder()
        .memtable_bytes(64 * 1024)
        .level0_compaction_trigger(2)
        .level0_slowdown_trigger(2)
        .level0_stop_trigger(4)
        .disable_auto_compactions(true)
        .build()
        .expect("valid");
    let listener: Arc<dyn EventListener> = recorder.clone();
    let options = DbOptions::builder()
        .default_keyspace(keyspace)
        .event_listener(listener)
        .build()
        .expect("valid");
    Db::open_with_storage(PATH, options, Arc::new(storage.clone()))
        .await
        .expect("opened")
}

async fn put(db: &Db, key: &[u8], durability: Durability) -> crate::Result<()> {
    let mut batch = WriteBatch::new();
    batch
        .put(KeyspaceId::DEFAULT, key, b"value")
        .expect("entry");
    let options = WriteOptions {
        durability,
        ..WriteOptions::default()
    };
    db.write(&batch, &options).await.map(|_| ())
}

async fn until(what: &str, done: impl Fn() -> bool) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while !done() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{what}"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flushes_compactions_and_stall_changes_are_reported_in_order() {
    let storage = FaultStorage::default();
    let recorder = Arc::new(Recorder::default());
    let db = open(&storage, &recorder).await;
    for key in [b"a", b"b"] {
        put(&db, key, Durability::Sync).await.expect("written");
        db.flush().await.expect("flushed");
    }
    until("level 0 at its slowdown trigger is reported", || {
        recorder
            .heard()
            .iter()
            .any(|heard| matches!(heard, Heard::Stall(_)))
    })
    .await;
    db.compact_range(KeyspaceId::DEFAULT, b"", None)
        .await
        .expect("compacted");
    until("the stall's end is reported", || {
        recorder.heard().last() == Some(&Heard::Stall(StallCondition::Normal))
    })
    .await;
    let heard = recorder.heard();
    let flushes: Vec<&FlushCompleted> = heard
        .iter()
        .filter_map(|heard| match heard {
            Heard::Flush(flush) => Some(flush),
            _ => None,
        })
        .collect();
    assert_eq!(flushes.len(), 2, "{heard:?}");
    assert!(
        flushes
            .iter()
            .all(|flush| flush.tables == 1 && flush.bytes > 0)
    );
    assert!(flushes[0].flushed_sequence < flushes[1].flushed_sequence);
    let compactions: Vec<&CompactionCompleted> = heard
        .iter()
        .filter_map(|heard| match heard {
            Heard::Compaction(compaction) => Some(compaction),
            _ => None,
        })
        .collect();
    assert_eq!(compactions.len(), 1, "{heard:?}");
    assert_eq!(
        (compactions[0].keyspace, compactions[0].input_tables),
        (KeyspaceId::DEFAULT, 2)
    );
    assert!(compactions[0].output_tables >= 1 && compactions[0].output_bytes > 0);
    let stalls: Vec<&Heard> = heard
        .iter()
        .filter(|heard| matches!(heard, Heard::Stall(_)))
        .collect();
    assert!(
        matches!(
            stalls.as_slice(),
            [
                Heard::Stall(StallCondition::Delayed { .. }),
                Heard::Stall(StallCondition::Normal)
            ]
        ),
        "each change reported once: {stalls:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_failure_that_stops_the_database_is_reported_once() {
    let storage = FaultStorage::default();
    let recorder = Arc::new(Recorder::default());
    let db = open(&storage, &recorder).await;
    put(&db, b"a", Durability::Buffered).await.expect("written");
    storage.fail_syncs();
    let failed = put(&db, b"b", Durability::Sync).await;
    assert!(failed.is_err(), "{failed:?}");
    until("the log's failure is reported", || {
        recorder
            .heard()
            .iter()
            .any(|heard| matches!(heard, Heard::Failure(_)))
    })
    .await;
    // The flush fails too; the database had stopped already, so nothing more is reported.
    let flushed = db.flush().await;
    assert!(flushed.is_err(), "{flushed:?}");
    let failures = recorder
        .heard()
        .iter()
        .filter(|heard| matches!(heard, Heard::Failure(_)))
        .count();
    assert_eq!(failures, 1, "{:?}", recorder.heard());
    assert!(db.background_error().is_some());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_options_record_names_the_listener() {
    let storage = FaultStorage::default();
    let recorder = Arc::new(Recorder::default());
    open(&storage, &recorder)
        .await
        .close()
        .await
        .expect("closed");
    let dir = std::path::Path::new(PATH);
    let record = crate::Storage::list(&storage, dir)
        .expect("listable")
        .into_iter()
        .filter(|name| name.starts_with("OPTIONS-"))
        .max()
        .expect("an options record");
    let text = String::from_utf8(storage.bytes(&dir.join(record))).expect("utf-8");
    assert!(text.contains("recorder"), "{text}");
}
