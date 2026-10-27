use std::sync::Arc;

use crate::log::fault::FaultStorage;
use crate::{
    Db, DbOptions, Error, KeyspaceId, KeyspaceOptions, MergeOperator, ReadOptions, WriteBatch,
    WriteOptions,
};

const PATH: &str = "/db";
const KEY: &[u8] = b"counter";

/// Adds decimal operands to a decimal base; refuses an operand that is not a number.
#[derive(Debug)]
struct Sum;

impl MergeOperator for Sum {
    fn name(&self) -> &str {
        "sum"
    }

    fn full_merge(&self, _key: &[u8], base: Option<&[u8]>, operands: &[&[u8]]) -> Option<Vec<u8>> {
        let number = |bytes: &[u8]| std::str::from_utf8(bytes).ok()?.parse::<i64>().ok();
        let mut total = base.map_or(Some(0), number)?;
        for operand in operands {
            total = total.checked_add(number(operand)?)?;
        }
        Some(total.to_string().into_bytes())
    }
}

/// Joins the base and the operands with commas, oldest first: order shows.
#[derive(Debug)]
struct Append;

impl MergeOperator for Append {
    fn name(&self) -> &str {
        "append"
    }

    fn full_merge(&self, _key: &[u8], base: Option<&[u8]>, operands: &[&[u8]]) -> Option<Vec<u8>> {
        let parts: Vec<&[u8]> = base.into_iter().chain(operands.iter().copied()).collect();
        Some(parts.join(&b","[..]))
    }
}

fn options(memory: bool) -> DbOptions {
    with_operator(memory, Arc::new(Sum))
}

fn with_operator(memory: bool, operator: Arc<dyn MergeOperator>) -> DbOptions {
    let builder = if memory {
        DbOptions::in_memory().to_builder()
    } else {
        DbOptions::builder()
    };
    let keyspace = KeyspaceOptions::builder()
        .merge_operator(operator)
        .build()
        .expect("valid");
    builder.default_keyspace(keyspace).build().expect("valid")
}

async fn apply(db: &Db, edit: impl FnOnce(&mut WriteBatch)) -> crate::Result<()> {
    let mut batch = WriteBatch::new();
    edit(&mut batch);
    db.write(&batch, &WriteOptions::default()).await.map(drop)
}

async fn merge(db: &Db, operand: &str) {
    apply(db, |batch| {
        batch
            .merge(KeyspaceId::DEFAULT, KEY, operand.as_bytes())
            .expect("small entry");
    })
    .await
    .expect("merged");
}

async fn value(db: &Db) -> crate::Result<Option<String>> {
    let found = db
        .get(KeyspaceId::DEFAULT, KEY, &ReadOptions::default())
        .await?;
    Ok(found.map(|bytes| String::from_utf8(bytes).expect("utf-8")))
}

#[tokio::test]
async fn operands_combine_with_their_base_in_memory() {
    let db = Db::open("memory", options(true)).await.expect("opened");
    for operand in ["1", "2", "3"] {
        merge(&db, operand).await;
    }
    assert_eq!(value(&db).await.expect("read").as_deref(), Some("6"));
    let before = db.snapshot().expect("snapshot");
    apply(&db, |batch| {
        batch.put(KeyspaceId::DEFAULT, KEY, b"10").expect("entry");
    })
    .await
    .expect("put");
    merge(&db, "5").await;
    assert_eq!(value(&db).await.expect("read").as_deref(), Some("15"));
    let at_before = before
        .get(KeyspaceId::DEFAULT, KEY, &ReadOptions::default())
        .await
        .expect("read");
    assert_eq!(at_before.as_deref(), Some(&b"6"[..]));
    drop(before);
    apply(&db, |batch| {
        batch.delete(KeyspaceId::DEFAULT, KEY).expect("entry");
    })
    .await
    .expect("deleted");
    merge(&db, "2").await;
    assert_eq!(value(&db).await.expect("read").as_deref(), Some("2"));
    apply(&db, |batch| {
        batch
            .delete_range(KeyspaceId::DEFAULT, b"a", b"z")
            .expect("entry");
    })
    .await
    .expect("range deleted");
    merge(&db, "7").await;
    assert_eq!(value(&db).await.expect("read").as_deref(), Some("7"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn operands_combine_across_tables_and_files_and_survive_a_reopen() {
    let storage = FaultStorage::default();
    let open = || Db::open_with_storage(PATH, options(false), Arc::new(storage.clone()));
    let db = open().await.expect("opened");
    apply(&db, |batch| {
        batch.put(KeyspaceId::DEFAULT, KEY, b"100").expect("entry");
    })
    .await
    .expect("put");
    db.flush().await.expect("flushed");
    merge(&db, "20").await;
    db.flush().await.expect("flushed");
    db.compact_range(KeyspaceId::DEFAULT, b"", None)
        .await
        .expect("compacted");
    merge(&db, "3").await;
    db.flush().await.expect("flushed");
    merge(&db, "-1").await;
    assert_eq!(value(&db).await.expect("read").as_deref(), Some("122"));
    db.close().await.expect("closed");
    let reopened = open().await.expect("reopened");
    assert_eq!(
        value(&reopened).await.expect("read").as_deref(),
        Some("122")
    );
}

#[tokio::test]
async fn a_merge_without_an_operator_is_refused_and_an_unreadable_one_fails_by_name() {
    let plain = Db::open("memory", DbOptions::in_memory())
        .await
        .expect("opened");
    let refused = apply(&plain, |batch| {
        batch.merge(KeyspaceId::DEFAULT, KEY, b"1").expect("entry");
    })
    .await;
    assert!(matches!(
        refused,
        Err(Error::InvalidArgument { reason, .. })
            if reason == "the keyspace has no merge operator"
    ));
    assert_eq!(value(&plain).await.expect("read"), None);

    let db = Db::open("memory", options(true)).await.expect("opened");
    merge(&db, "not a number").await;
    assert!(matches!(
        value(&db).await,
        Err(Error::MergeFailed { keyspace: 0 })
    ));
}

/// Every read — point reads, a scan, and iterators both ways — of keys written with puts,
/// merges and deletes spread over in-memory tables and table files, against a model.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_read_combines_operands_as_the_model_does() {
    let storage = FaultStorage::default();
    let db = Db::open_with_storage(
        PATH,
        with_operator(false, Arc::new(Append)),
        Arc::new(storage),
    )
    .await
    .expect("opened");
    let mut model: Vec<Option<String>> = vec![None; 12];
    for round in 0_u32..40 {
        let index = usize::try_from(round.wrapping_mul(7) % 12).expect("small");
        let key = format!("k{index:02}");
        let step = round.to_string();
        apply(&db, |batch| {
            match round % 5 {
                0 => batch.put(
                    KeyspaceId::DEFAULT,
                    key.as_bytes(),
                    step.to_string().as_bytes(),
                ),
                4 => batch.delete(KeyspaceId::DEFAULT, key.as_bytes()),
                _ => batch.merge(
                    KeyspaceId::DEFAULT,
                    key.as_bytes(),
                    step.to_string().as_bytes(),
                ),
            }
            .expect("entry")
        })
        .await
        .expect("written");
        model[index] = match round % 5 {
            0 => Some(step),
            4 => None,
            _ => Some(
                model[index]
                    .take()
                    .map_or(step.clone(), |value| format!("{value},{step}")),
            ),
        };
        if round % 9 == 8 {
            db.flush().await.expect("flushed");
        }
        if round == 26 {
            db.compact_range(KeyspaceId::DEFAULT, b"", None)
                .await
                .expect("compacted");
        }
    }
    let expected: Vec<(Vec<u8>, Vec<u8>)> = model
        .iter()
        .enumerate()
        .filter_map(|(index, value)| {
            value.as_ref().map(|value| {
                (
                    format!("k{index:02}").into_bytes(),
                    value.clone().into_bytes(),
                )
            })
        })
        .collect();
    for (index, value) in model.iter().enumerate() {
        let key = format!("k{index:02}");
        let read = db
            .get(KeyspaceId::DEFAULT, key.as_bytes(), &ReadOptions::default())
            .await
            .expect("read");
        assert_eq!(read, value.clone().map(String::into_bytes), "{key}");
    }
    let page = db
        .scan(KeyspaceId::DEFAULT, b"", None, 100, &ReadOptions::default())
        .await
        .expect("scanned");
    assert_eq!(page.entries, expected, "scan");
    let mut iterator = db
        .iter(KeyspaceId::DEFAULT, &ReadOptions::default())
        .expect("iterator");
    let mut forward = Vec::new();
    iterator.seek_to_first().await.expect("moved");
    while iterator.valid() {
        forward.push((iterator.key().to_vec(), iterator.value().to_vec()));
        iterator.next().await.expect("moved");
    }
    assert_eq!(forward, expected, "forward");
    let mut backward = Vec::new();
    iterator.seek_to_last().await.expect("moved");
    while iterator.valid() {
        backward.push((iterator.key().to_vec(), iterator.value().to_vec()));
        iterator.prev().await.expect("moved");
    }
    backward.reverse();
    assert_eq!(backward, expected, "backward");
}
