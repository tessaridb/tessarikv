//! A database process killed mid-write keeps every write it acknowledged.
//!
//! The test runs this test binary again as a child that writes and reports each acknowledged
//! write on its standard output, kills it without warning once it has acknowledged enough, and
//! reopens the database the child left behind.

use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};

use crate::{
    Db, DbOptions, Durability, KeyspaceId, ReadOptions, StorageMode, WriteBatch, WriteOptions,
};

/// Names the directory the child writes to; unset, the child test does nothing.
const CHILD_DIR: &str = "TESSARIKV_KILL_CHILD_DIR";

/// Acknowledged writes the parent waits for before it kills the child.
const ACKNOWLEDGED: u64 = 200;

fn options() -> DbOptions {
    DbOptions::builder()
        .storage(StorageMode::Os)
        .build()
        .expect("valid options")
}

fn key(index: u64) -> Vec<u8> {
    format!("key-{index}").into_bytes()
}

/// The child: writes until it is killed, alternating synced and buffered writes, and prints
/// `ack <index>` after each one returns.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn child_writes_until_killed() {
    let Some(dir) = std::env::var_os(CHILD_DIR) else {
        return;
    };
    let db = Db::open(&dir, options()).await.expect("child opens");
    // Far more than the parent waits for; the child is killed long before the end.
    for index in 0..ACKNOWLEDGED * 1000 {
        let durability = if index % 2 == 0 {
            Durability::Sync
        } else {
            Durability::Buffered
        };
        let mut batch = WriteBatch::new();
        batch
            .put(KeyspaceId::DEFAULT, &key(index), b"value")
            .expect("small entry");
        let write = WriteOptions {
            durability,
            ..WriteOptions::default()
        };
        db.write(&batch, &write).await.expect("child writes");
        println!("ack {index}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn writes_acknowledged_before_a_kill_are_recovered() {
    let dir = tempfile::tempdir().expect("temporary directory");
    let mut child = Command::new(std::env::current_exe().expect("test binary"))
        .args([
            "--exact",
            "db::kill_tests::child_writes_until_killed",
            "--nocapture",
        ])
        .env(CHILD_DIR, dir.path())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("child starts");
    let stdout = child.stdout.take().expect("piped");
    let mut acknowledged = None;
    for line in BufReader::new(stdout).lines() {
        let line = line.expect("child output");
        if let Some(index) = line.strip_prefix("ack ") {
            let index: u64 = index.parse().expect("an index");
            acknowledged = Some(index);
            if index >= ACKNOWLEDGED {
                break;
            }
        }
    }
    child.kill().expect("killed");
    child.wait().expect("reaped");
    let last = acknowledged.expect("the child acknowledged writes");

    let db = Db::open(dir.path(), options()).await.expect("recovered");
    for index in 0..=last {
        let value = db
            .get(KeyspaceId::DEFAULT, &key(index), &ReadOptions::default())
            .await
            .expect("readable");
        assert_eq!(
            value.as_deref(),
            Some(&b"value"[..]),
            "write {index} was acknowledged"
        );
    }
    db.close().await.expect("closed");
}
