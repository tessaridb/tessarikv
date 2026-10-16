//! Opening a database names the keyspaces it holds: those left out are refused, and those it
//! does not hold are added only when the options ask.

use std::sync::Arc;

use super::Db;
use super::flush_tests::PATH;
use crate::log::fault::FaultStorage;
use crate::version::manifest::ManifestStore;
use crate::{DbOptions, KeyspaceOptions};

fn options(names: &[&str], create_missing: bool) -> DbOptions {
    names
        .iter()
        .fold(DbOptions::builder(), |builder, name| {
            builder.keyspace(*name, KeyspaceOptions::default())
        })
        .create_missing_keyspaces(create_missing)
        .build()
        .expect("valid")
}

async fn open(storage: &FaultStorage, names: &[&str], create_missing: bool) -> crate::Result<Db> {
    Db::open_with_storage(
        PATH,
        options(names, create_missing),
        Arc::new(storage.clone()),
    )
    .await
}

fn held(storage: &FaultStorage) -> Vec<(u32, String)> {
    let (version, _) = ManifestStore::recover(storage, std::path::Path::new(PATH))
        .expect("recovered")
        .expect("a manifest");
    version
        .keyspaces()
        .map(|(id, name)| (id, name.to_owned()))
        .collect()
}

#[tokio::test]
async fn opening_adds_missing_keyspaces_only_when_asked_and_requires_naming_the_rest() {
    let storage = FaultStorage::default();
    let refused = open(&storage, &["users"], false).await.map(|_| ());
    assert_eq!(refused.expect_err("not held").code(), "invalid_argument");
    open(&storage, &["users"], true)
        .await
        .expect("created")
        .close()
        .await
        .expect("closed");
    assert_eq!(held(&storage), [(1, "users".to_owned())]);
    let unnamed = open(&storage, &[], false).await.map(|_| ());
    assert_eq!(
        unnamed.expect_err("users left out").code(),
        "invalid_argument"
    );
    open(&storage, &["users"], false)
        .await
        .expect("reopened")
        .close()
        .await
        .expect("closed");
    open(&storage, &["orders", "users"], true)
        .await
        .expect("orders added")
        .close()
        .await
        .expect("closed");
    assert_eq!(
        held(&storage),
        [(1, "users".to_owned()), (2, "orders".to_owned())]
    );
}

#[test]
fn keyspace_names_are_checked() {
    let refused = |names: &[&str]| {
        let built = names
            .iter()
            .fold(DbOptions::builder(), |builder, name| {
                builder.keyspace(*name, KeyspaceOptions::default())
            })
            .build();
        assert_eq!(
            built.expect_err("refused").code(),
            "invalid_options",
            "{names:?}"
        );
    };
    refused(&[""]);
    refused(&["default"]);
    refused(&["users", "users"]);
    refused(&[&"x".repeat(256)]);
}
