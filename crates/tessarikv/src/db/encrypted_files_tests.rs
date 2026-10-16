//! Every file of an encrypted database: no key, value or keyspace name anywhere on storage, and
//! a manifest changed under a valid checksum refused.

use std::path::Path;
use std::sync::Arc;

use tessarikv_format::{FRAME_HEADER_BYTES, SEAL_HEADER_BYTES, frame_header, payload_checksum};

use super::Db;
use super::flush_tests::PATH;
use crate::log::fault::FaultStorage;
use crate::storage::Storage as _;
use crate::{
    DbOptions, Error, KeyProvider, KeyspaceOptions, ReadOptions, RootKey, WriteBatch, WriteOptions,
};

#[derive(Debug)]
struct Key;

impl KeyProvider for Key {
    fn root_key(&self, key_id: u32) -> Option<RootKey> {
        (key_id == 1).then(|| RootKey::new([7; 32]))
    }
}

fn options(encrypted: bool) -> DbOptions {
    let builder = DbOptions::builder()
        .keyspace("secret-keyspace", KeyspaceOptions::default())
        .create_missing_keyspaces(true);
    let builder = if encrypted {
        builder.encryption(Arc::new(Key), 1)
    } else {
        builder
    };
    builder.build().expect("valid")
}

/// A crash image of a database holding flushed and unflushed writes in a named keyspace.
async fn image(encrypted: bool) -> FaultStorage {
    let storage = FaultStorage::default();
    let db = Db::open_with_storage(PATH, options(encrypted), Arc::new(storage.clone()))
        .await
        .expect("opened");
    let keyspace = db.keyspace("secret-keyspace").expect("named");
    for index in 0..2 {
        let mut batch = WriteBatch::new();
        batch
            .put(
                keyspace,
                format!("secret-key-{index}").as_bytes(),
                b"secret-value",
            )
            .expect("entry");
        db.write(&batch, &WriteOptions::default())
            .await
            .expect("written");
        if index == 0 {
            db.flush().await.expect("flushed");
        }
    }
    let image = storage.crash(Path::new(PATH), true);
    db.close().await.expect("closed");
    image
}

/// Every file and what in it shows `secret-`.
fn shown(storage: &FaultStorage) -> Vec<String> {
    storage
        .list(Path::new(PATH))
        .expect("listable")
        .into_iter()
        .filter(|name| {
            storage
                .bytes(&Path::new(PATH).join(name))
                .windows(7)
                .any(|window| window == b"secret-")
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn no_file_of_an_encrypted_database_holds_plaintext() {
    let plain = image(false).await;
    assert!(
        shown(&plain)
            .iter()
            .any(|name| name.starts_with("MANIFEST-")),
        "control: a plain manifest names its keyspaces: {:?}",
        shown(&plain)
    );
    let sealed = image(true).await;
    assert_eq!(shown(&sealed), Vec::<String>::new());
    let db = Db::open_with_storage(PATH, options(true), Arc::new(sealed))
        .await
        .expect("recovered");
    let keyspace = db.keyspace("secret-keyspace").expect("still named");
    for index in 0..2 {
        let found = db
            .get(
                keyspace,
                format!("secret-key-{index}").as_bytes(),
                &ReadOptions::default(),
            )
            .await
            .expect("read");
        assert_eq!(found.as_deref(), Some(&b"secret-value"[..]), "key {index}");
    }
    db.close().await.expect("closed");
}

/// The first edit of the manifest changed, with the checksum that matches it written too.
fn alter_manifest(storage: &FaultStorage) {
    let name = storage
        .list(Path::new(PATH))
        .expect("listable")
        .into_iter()
        .find(|name| name.starts_with("MANIFEST-"))
        .expect("a manifest");
    let number: u64 = name["MANIFEST-".len()..].parse().expect("a number");
    let path = Path::new(PATH).join(name);
    let mut bytes = storage.bytes(&path);
    let header_at = SEAL_HEADER_BYTES;
    let payload_at = header_at + FRAME_HEADER_BYTES;
    let len = u32::from_le_bytes(
        bytes[header_at + 4..header_at + 8]
            .try_into()
            .expect("four"),
    );
    let payload_end = payload_at + usize::try_from(len).expect("fits");
    bytes[payload_at] ^= 1;
    let checksum = payload_checksum(&bytes[payload_at..payload_end]);
    let header = frame_header(number, payload_end - payload_at, checksum).expect("a header");
    bytes[header_at..payload_at].copy_from_slice(&header);
    let mut file = storage.create(&path).expect("replaced");
    file.append(&bytes).expect("written");
    file.sync().expect("synced");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_manifest_altered_under_a_valid_checksum_fails_the_open() {
    let sealed = image(true).await;
    alter_manifest(&sealed);
    let opened = Db::open_with_storage(PATH, options(true), Arc::new(sealed)).await;
    assert!(
        matches!(&opened, Err(Error::Corruption { detail, .. }) if detail.contains("authentication")),
        "{:?}",
        opened.as_ref().err()
    );
}

/// The manifest's first two edit frames swapped: each frame keeps its own valid checksum, but
/// an edit is bound to its place, so recovery refuses rather than applying them out of order.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn swapped_manifest_edits_fail_the_open() {
    let sealed = image(true).await;
    let name = sealed
        .list(Path::new(PATH))
        .expect("listable")
        .into_iter()
        .find(|name| name.starts_with("MANIFEST-"))
        .expect("a manifest");
    let path = Path::new(PATH).join(name);
    let bytes = sealed.bytes(&path);
    let frame_end = |at: usize| {
        let len = u32::from_le_bytes(bytes[at + 4..at + 8].try_into().expect("four"));
        at + FRAME_HEADER_BYTES + usize::try_from(len).expect("fits")
    };
    let first = SEAL_HEADER_BYTES;
    let second = frame_end(first);
    let third = frame_end(second);
    let mut swapped = bytes[..first].to_vec();
    swapped.extend_from_slice(&bytes[second..third]);
    swapped.extend_from_slice(&bytes[first..second]);
    swapped.extend_from_slice(&bytes[third..]);
    let mut file = sealed.create(&path).expect("replaced");
    file.append(&swapped).expect("written");
    file.sync().expect("synced");
    let opened = Db::open_with_storage(PATH, options(true), Arc::new(sealed)).await;
    assert!(
        matches!(&opened, Err(Error::Corruption { detail, .. }) if detail.contains("authentication")),
        "{:?}",
        opened.as_ref().err()
    );
}
