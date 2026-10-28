//! Every open on storage records the options it runs with, beside the data.

use std::path::Path;
use std::sync::Arc;

use super::Db;
use super::flush_tests::PATH;
use crate::log::fault::FaultStorage;
use crate::storage::Storage as _;
use crate::{DbOptions, KeyspaceOptions, OptionChange};

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

fn sized(cache_bytes: u64, users_memtable_bytes: u64) -> crate::DbOptionsBuilder {
    let users = KeyspaceOptions::builder()
        .memtable_bytes(users_memtable_bytes)
        .build()
        .expect("valid");
    DbOptions::builder()
        .block_cache_bytes(cache_bytes)
        .keyspace("users", users)
        .create_missing_keyspaces(true)
}

async fn changes_of(storage: &FaultStorage, options: DbOptions) -> Vec<OptionChange> {
    let db = Db::open_with_storage(PATH, options, Arc::new(storage.clone()))
        .await
        .expect("opened");
    let changes = db.changed_options().to_vec();
    db.close().await.expect("closed");
    changes
}

fn change(option: &str, previous: Option<&str>, current: Option<&str>) -> OptionChange {
    OptionChange {
        option: option.to_owned(),
        previous: previous.map(str::to_owned),
        current: current.map(str::to_owned),
    }
}

/// An open reports each option whose recorded value differs from the previous open's, by its
/// place in the record, and nothing when the options are the same.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_open_reports_the_options_changed_since_the_previous_open() {
    let storage = FaultStorage::default();
    let first = sized(1_000_000, 1_048_576).build().expect("valid");
    assert_eq!(
        changes_of(&storage, first.clone()).await,
        [],
        "a first open"
    );
    assert_eq!(changes_of(&storage, first).await, [], "the same options");
    let changed = sized(2_000_000, 2_097_152).build().expect("valid");
    assert_eq!(
        changes_of(&storage, changed).await,
        [
            change("block_cache_bytes", Some("1000000"), Some("2000000")),
            change(
                "keyspaces[0][1].memtable_bytes",
                Some("1048576"),
                Some("2097152")
            ),
        ]
    );
}

/// A keyspace added since the previous open is reported line by line, with no previous value.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_keyspace_added_since_the_previous_open_is_reported() {
    let storage = FaultStorage::default();
    changes_of(
        &storage,
        sized(1_000_000, 1_048_576).build().expect("valid"),
    )
    .await;
    let added = sized(1_000_000, 1_048_576)
        .keyspace("orders", KeyspaceOptions::default())
        .build()
        .expect("valid");
    let changes = changes_of(&storage, added).await;
    assert!(
        changes.contains(&change("keyspaces[1][0]", None, Some("\"orders\""))),
        "{changes:?}"
    );
    assert!(
        changes
            .iter()
            .all(|change| change.option.starts_with("keyspaces[1]") && change.previous.is_none()),
        "{changes:?}"
    );
}

#[derive(Debug)]
struct Key;

impl crate::KeyProvider for Key {
    fn root_key(&self, key_id: u32) -> Option<crate::RootKey> {
        (key_id == 1).then(|| crate::RootKey::new([7; 32]))
    }
}

/// In an encrypted database the previous record is unsealed to be compared.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_encrypted_database_reports_its_changed_options() {
    let storage = FaultStorage::default();
    let encrypted = |cache_bytes| {
        sized(cache_bytes, 1_048_576)
            .encryption(Arc::new(Key), 1)
            .build()
            .expect("valid")
    };
    changes_of(&storage, encrypted(1_000_000)).await;
    assert_eq!(
        changes_of(&storage, encrypted(2_000_000)).await,
        [change(
            "block_cache_bytes",
            Some("1000000"),
            Some("2000000")
        )]
    );
}

/// A value the previous record held and this one does not is reported with no current value.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_option_no_longer_held_is_reported() {
    let storage = FaultStorage::default();
    let prefixed = KeyspaceOptions::builder()
        .prefix_length(4)
        .build()
        .expect("valid");
    let with = DbOptions::builder()
        .default_keyspace(prefixed)
        .build()
        .expect("valid");
    changes_of(&storage, with).await;
    assert_eq!(
        changes_of(&storage, DbOptions::default()).await,
        [
            change("default_keyspace.prefix_length", None, Some("None")),
            change("default_keyspace.prefix_length[0]", Some("4"), None),
        ]
    );
}
