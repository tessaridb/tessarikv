//! A keyspace's properties: the files and bytes of each level as the current view holds them,
//! what compaction owes as of the installer's last step, and the bytes of its in-memory tables.

use std::sync::Arc;

use tessarikv_constants::LEVELS;

use crate::db::Db;
use crate::db::compaction::owed;
use crate::db::flush_tests::{PATH, key, until, value};
use crate::log::fault::FaultStorage;
use crate::{DbOptions, Error, KeyspaceId, KeyspaceOptions, WriteBatch, WriteOptions};

/// No automatic compaction, so the test decides where tables are.
fn keyspace_options() -> KeyspaceOptions {
    KeyspaceOptions::builder()
        .memtable_bytes(64 * 1024)
        .level0_compaction_trigger(2)
        .level_base_bytes(64 * 1024)
        .disable_auto_compactions(true)
        .build()
        .expect("valid")
}

async fn open() -> Db {
    let options = DbOptions::builder()
        .default_keyspace(keyspace_options())
        .build()
        .expect("valid");
    Db::open_with_storage(PATH, options, Arc::new(FaultStorage::default()))
        .await
        .expect("opened")
}

async fn put_round(db: &Db, round: u32) {
    let mut batch = WriteBatch::new();
    for index in 0..100 {
        batch
            .put(KeyspaceId::DEFAULT, &key(index), &value(index + round))
            .expect("entry");
    }
    db.write(&batch, &WriteOptions::default())
        .await
        .expect("written");
}

/// Files and bytes per level, counted from the view's files.
fn counted(db: &Db) -> ([u32; LEVELS], [u64; LEVELS]) {
    let mut files = [0_u32; LEVELS];
    let mut bytes = [0_u64; LEVELS];
    for file in db.view.load().files(0) {
        let level = usize::from(file.meta.level);
        files[level] += 1;
        bytes[level] += file.meta.size;
    }
    (files, bytes)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_levels_read_as_the_view_holds_them_after_flushes_and_a_compaction() {
    let db = open().await;
    let empty = db.properties(KeyspaceId::DEFAULT).expect("properties");
    assert_eq!(empty.files_per_level, [0; LEVELS]);
    for round in 0..3 {
        put_round(&db, round).await;
        db.flush().await.expect("flushed");
    }
    let flushed = db.properties(KeyspaceId::DEFAULT).expect("properties");
    let (files, bytes) = counted(&db);
    assert_eq!(files[0], 3, "three level-0 tables");
    assert_eq!(
        (flushed.files_per_level, flushed.bytes_per_level),
        (files, bytes)
    );
    db.compact_range(KeyspaceId::DEFAULT, b"", None)
        .await
        .expect("compacted");
    let compacted = db.properties(KeyspaceId::DEFAULT).expect("properties");
    let (files, bytes) = counted(&db);
    assert_eq!(files[0], 0, "level 0 compacted away");
    assert_eq!(
        (compacted.files_per_level, compacted.bytes_per_level),
        (files, bytes)
    );
    // A newer table above the bottom level: the estimate stays the deepest level's bytes.
    put_round(&db, 9).await;
    db.flush().await.expect("flushed");
    let layered = db.properties(KeyspaceId::DEFAULT).expect("properties");
    let (_, bytes) = counted(&db);
    assert!(bytes[0] > 0 && bytes[LEVELS - 1] > 0, "{bytes:?}");
    assert_eq!(layered.estimated_live_bytes, bytes[LEVELS - 1]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pending_bytes_follow_the_installer_and_memtable_bytes_follow_the_writes() {
    let db = open().await;
    let before = db.properties(KeyspaceId::DEFAULT).expect("properties");
    put_round(&db, 0).await;
    let written = db.properties(KeyspaceId::DEFAULT).expect("properties");
    assert!(
        written.memtable_bytes > before.memtable_bytes,
        "{written:?}"
    );
    for round in 1..4 {
        put_round(&db, round).await;
        db.flush().await.expect("flushed");
    }
    let flushed = db.properties(KeyspaceId::DEFAULT).expect("properties");
    assert!(
        flushed.memtable_bytes < written.memtable_bytes,
        "{flushed:?}"
    );
    let owes = owed(&db.view.load(), 0, &keyspace_options());
    assert!(
        owes > 0,
        "level 0 past its trigger with auto compaction off owes bytes"
    );
    until("the installer publishes what compaction owes", || {
        db.properties(KeyspaceId::DEFAULT)
            .is_ok_and(|properties| properties.pending_compaction_bytes == owes)
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn properties_of_an_unknown_keyspace_are_refused() {
    let db = open().await;
    let refused = db.properties(KeyspaceId::new(77));
    assert!(
        matches!(refused, Err(Error::UnknownKeyspace { .. })),
        "{refused:?}"
    );
}
