//! Every open on storage records the options it runs with, beside the data.

use std::path::Path;
use std::sync::Arc;

use super::Db;
use super::flush_tests::PATH;
use crate::log::fault::FaultStorage;
use crate::storage::Storage as _;
use crate::{DbOptions, KeyspaceOptions};

fn options(cache_bytes: u64) -> DbOptions {
    DbOptions::builder()
        .block_cache_bytes(cache_bytes)
        .keyspace("users", KeyspaceOptions::default())
        .create_missing_keyspaces(true)
        .build()
        .expect("valid")
}

/// The options files in the database's directory, oldest first.
fn options_files(storage: &FaultStorage) -> Vec<String> {
    let mut names: Vec<String> = storage
        .list(Path::new(PATH))
        .expect("listable")
        .into_iter()
        .filter(|name| name.starts_with("OPTIONS-"))
        .collect();
    names.sort();
    names
}

fn text(storage: &FaultStorage, name: &str) -> String {
    String::from_utf8(storage.bytes(&Path::new(PATH).join(name))).expect("utf-8")
}

/// Each open writes a file naming every option it runs with, the latest open's last; the two
/// newest are kept.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_open_records_its_options_and_the_two_newest_are_kept() {
    let storage = FaultStorage::default();
    // What an open that stopped while writing its record leaves behind.
    let stale = Path::new(PATH).join("OPTIONS-00000000000000000000.writing");
    storage.create_dir_all(Path::new(PATH)).expect("created");
    drop(storage.create(&stale).expect("created"));
    for cache_bytes in [1_234_567_u64, 2_345_678, 3_456_789] {
        let db = Db::open_with_storage(PATH, options(cache_bytes), Arc::new(storage.clone()))
            .await
            .expect("opened");
        let names = options_files(&storage);
        let newest = text(&storage, names.last().expect("a file"));
        assert!(newest.contains(&cache_bytes.to_string()), "{newest}");
        assert!(newest.contains("users"), "every keyspace is named");
        db.close().await.expect("closed");
    }
    let names = options_files(&storage);
    assert_eq!(names.len(), 2, "{names:?}");
    assert!(
        !storage.exists(&stale).expect("exists"),
        "a half-written record is removed"
    );
    assert!(text(&storage, &names[0]).contains("2345678"));
    assert!(text(&storage, &names[1]).contains("3456789"));
}
