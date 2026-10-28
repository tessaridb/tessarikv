use std::sync::Arc;

use crate::log::fault::FaultStorage;
use crate::{
    CompactionStyle, Db, DbOptions, Error, KeyspaceId, KeyspaceOptions, WriteBatch, WriteOptions,
};

const PATH: &str = "/db";

fn manual() -> KeyspaceOptions {
    KeyspaceOptions::builder()
        .disable_auto_compactions(true)
        .build()
        .expect("valid")
}

async fn open(storage: &FaultStorage, keyspace: KeyspaceOptions) -> Db {
    let options = DbOptions::builder()
        .default_keyspace(keyspace)
        .build()
        .expect("valid");
    Db::open_with_storage(PATH, options, Arc::new(storage.clone()))
        .await
        .expect("opened")
}

/// Writes `tables` level-0 tables, one key each.
async fn fill_level0(db: &Db, tables: u32) {
    for index in 0..tables {
        let mut batch = WriteBatch::new();
        batch
            .put(KeyspaceId::DEFAULT, &index.to_be_bytes(), b"value")
            .expect("small entry");
        db.write(&batch, &WriteOptions::default())
            .await
            .expect("written");
        db.flush().await.expect("flushed");
    }
}

fn level0(db: &Db) -> u32 {
    db.properties(KeyspaceId::DEFAULT)
        .expect("held")
        .files_per_level[0]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn compactions_follow_options_changed_while_open() {
    let storage = FaultStorage::default();
    let db = open(&storage, manual()).await;
    fill_level0(&db, 6).await;
    db.wait_for_compact().await.expect("idle");
    assert_eq!(level0(&db), 6, "compaction ran while turned off");

    db.set_keyspace_options(KeyspaceId::DEFAULT, KeyspaceOptions::default())
        .await
        .expect("changed");
    db.wait_for_compact().await.expect("idle");
    assert_eq!(level0(&db), 0, "the changed trigger was not applied");

    db.set_keyspace_options(KeyspaceId::DEFAULT, manual())
        .await
        .expect("changed back");
    fill_level0(&db, 5).await;
    db.wait_for_compact().await.expect("idle");
    assert_eq!(
        level0(&db),
        5,
        "compaction ran after being turned off again"
    );
    db.close().await.expect("closed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn options_the_open_database_cannot_take_are_refused_by_name() {
    let storage = FaultStorage::default();
    let db = open(&storage, KeyspaceOptions::default()).await;
    let refused = |result: crate::Result<()>| match result {
        Err(Error::InvalidArgument { reason, .. }) => reason,
        other => panic!("expected a refusal, got {other:?}"),
    };
    let changed = |build: fn(crate::KeyspaceOptionsBuilder) -> crate::KeyspaceOptionsBuilder| {
        build(KeyspaceOptions::builder()).build().expect("valid")
    };
    let style = db
        .set_keyspace_options(
            KeyspaceId::DEFAULT,
            changed(|builder| builder.compaction(CompactionStyle::universal())),
        )
        .await;
    assert_eq!(
        refused(style),
        "the compaction style of an open keyspace cannot change"
    );
    let memtable = db
        .set_keyspace_options(
            KeyspaceId::DEFAULT,
            changed(|builder| builder.memtable_bytes(1 << 20)),
        )
        .await;
    assert_eq!(
        refused(memtable),
        "the in-memory table sizes of an open keyspace cannot change"
    );
    let immutable = db
        .set_keyspace_options(
            KeyspaceId::DEFAULT,
            changed(|builder| builder.max_immutable_memtables(3)),
        )
        .await;
    assert_eq!(
        refused(immutable),
        "the in-memory table sizes of an open keyspace cannot change"
    );
    assert!(matches!(
        db.set_keyspace_options(KeyspaceId::new(7), KeyspaceOptions::default())
            .await,
        Err(Error::UnknownKeyspace { keyspace: 7 })
    ));
    db.close().await.expect("closed");

    let reader = Db::open_read_only_with_storage(PATH, DbOptions::default(), Arc::new(storage))
        .await
        .expect("opened read-only");
    assert!(matches!(
        reader
            .set_keyspace_options(KeyspaceId::DEFAULT, KeyspaceOptions::default())
            .await,
        Err(Error::ReadOnly {
            what: "changing a keyspace's options"
        })
    ));
}

#[tokio::test]
async fn a_database_in_memory_checks_the_keyspace_and_nothing_else() {
    let db = Db::open("memory", DbOptions::in_memory())
        .await
        .expect("opened");
    db.set_keyspace_options(KeyspaceId::DEFAULT, KeyspaceOptions::point_lookup())
        .await
        .expect("taken");
    assert!(matches!(
        db.set_keyspace_options(KeyspaceId::new(3), KeyspaceOptions::default())
            .await,
        Err(Error::UnknownKeyspace { keyspace: 3 })
    ));
}

#[derive(Debug)]
struct Keep;

impl crate::MergeOperator for Keep {
    fn name(&self) -> &str {
        "keep"
    }

    fn full_merge(&self, _key: &[u8], base: Option<&[u8]>, _operands: &[&[u8]]) -> Option<Vec<u8>> {
        base.map(<[u8]>::to_vec)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_merge_operator_of_an_open_keyspace_is_fixed() {
    let storage = FaultStorage::default();
    let db = open(&storage, KeyspaceOptions::default()).await;
    let operated = KeyspaceOptions::builder()
        .merge_operator(Arc::new(Keep))
        .build()
        .expect("valid");
    assert!(matches!(
        db.set_keyspace_options(KeyspaceId::DEFAULT, operated).await,
        Err(Error::InvalidArgument { reason, .. })
            if reason == "the merge operator of an open keyspace cannot change"
    ));
    db.close().await.expect("closed");
}
