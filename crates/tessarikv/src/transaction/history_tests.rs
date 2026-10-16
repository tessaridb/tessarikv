//! Serializability against generated histories: concurrent serializable transactions read and
//! write a few keys; replaying the committed ones one at a time in commit order on a model must
//! reproduce every value each of them read.

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::{
    Db, DbOptions, Isolation, KeyspaceId, ReadOptions, Result, TransactionOptions, WriteBatch,
    WriteOptions,
};

const KS: KeyspaceId = KeyspaceId::DEFAULT;
const KEYS: u64 = 6;
const TASKS: u64 = 4;
const ATTEMPTS: u64 = 40;

/// What one committed transaction saw and did.
#[derive(Debug)]
struct Committed {
    sequence: u64,
    reads: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    scanned: Scanned,
    writes: Vec<(Vec<u8>, Vec<u8>)>,
}

/// A range a transaction scanned and the entries it saw there.
#[derive(Debug)]
struct Scanned {
    start: Vec<u8>,
    end: Vec<u8>,
    entries: Vec<(Vec<u8>, Vec<u8>)>,
}

fn key(index: u64) -> Vec<u8> {
    format!("k{index}").into_bytes()
}

/// A small deterministic generator per task.
fn next(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

/// One transaction: reads two keys, scans a span, then writes one or two keys. `None` when
/// the commit was refused.
async fn attempt(db: &Db, state: &mut u64, tag: u64) -> Result<Option<Committed>> {
    let mut txn = db.begin_with(&TransactionOptions {
        isolation: Isolation::Serializable,
    })?;
    let mut reads = Vec::new();
    for _ in 0..2 {
        let read = key(next(state) % KEYS);
        let value = txn.get(KS, &read, &ReadOptions::default()).await?;
        reads.push((read, value));
    }
    let (low, high) = (next(state) % KEYS, next(state) % KEYS);
    let (start, end) = (key(low.min(high)), key(low.max(high) + 1));
    let page = txn
        .scan(KS, &start, Some(&end), 100, &ReadOptions::default())
        .await?;
    let mut writes = Vec::new();
    for index in 0..=next(state) % 2 {
        let written = (
            key(next(state) % KEYS),
            format!("{tag}.{index}").into_bytes(),
        );
        txn.put(KS, &written.0, &written.1)?;
        writes.push(written);
    }
    match txn.commit(&WriteOptions::default()).await {
        Ok(sequence) => Ok(Some(Committed {
            sequence: sequence.get(),
            reads,
            scanned: Scanned {
                start,
                end,
                entries: page.entries,
            },
            writes,
        })),
        Err(error) if error.code() == "conflict" => Ok(None),
        Err(error) => Err(error),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn committed_serializable_transactions_replay_serially_in_commit_order() {
    for round in 0..10_u64 {
        let options = DbOptions::in_memory()
            .to_builder()
            .serializable_transactions(true)
            .build()
            .expect("valid");
        let db = Arc::new(Db::open("history", options).await.expect("opened"));
        let mut model = BTreeMap::new();
        let mut batch = WriteBatch::new();
        for index in 0..KEYS {
            batch.put(KS, &key(index), b"initial").expect("entry");
            model.insert(key(index), b"initial".to_vec());
        }
        db.write(&batch, &WriteOptions::default())
            .await
            .expect("written");
        let tasks: Vec<_> = (0..TASKS)
            .map(|task| {
                let db = Arc::clone(&db);
                tokio::spawn(async move {
                    let mut state = (round * TASKS + task + 1).wrapping_mul(0x9e37_79b9_7f4a_7c15);
                    let mut committed = Vec::new();
                    for attempt_number in 0..ATTEMPTS {
                        let tag = (task << 32) | attempt_number;
                        if let Some(done) = attempt(&db, &mut state, tag).await.expect("no error") {
                            committed.push(done);
                        }
                        tokio::task::yield_now().await;
                    }
                    committed
                })
            })
            .collect();
        let mut history = Vec::new();
        for task in tasks {
            history.extend(task.await.expect("task finished"));
        }
        history.sort_by_key(|done| done.sequence);
        assert!(!history.is_empty());
        for done in &history {
            for (read, value) in &done.reads {
                assert_eq!(
                    value.as_ref(),
                    model.get(read),
                    "round {round}: read of {read:?} by {done:?}"
                );
            }
            let Scanned {
                start,
                end,
                entries,
            } = &done.scanned;
            let expected: Vec<_> = model
                .range(start.clone()..end.clone())
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            assert_eq!(entries, &expected, "round {round}: scan by {done:?}");
            for (written, value) in &done.writes {
                model.insert(written.clone(), value.clone());
            }
        }
    }
}
