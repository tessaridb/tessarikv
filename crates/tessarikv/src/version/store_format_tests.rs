//! The store format a manifest records, and the gate that refuses a store this build cannot read.

use std::path::Path;
use std::sync::Arc;

use tessarikv_constants::STORE_FORMAT;
use tessarikv_format::{
    FrameReader, LogRead, ManifestEdit, encode_frame, frame_bytes, peek_store_format,
};

use super::Version;
use super::manifest::{CURRENT, ManifestStore};
use crate::storage::{Storage, read_all};
use crate::{Db, DbOptions, Error, MemoryStorage};

const MANIFEST: &str = "MANIFEST-00000000000000000001";

fn dir() -> &'static Path {
    Path::new("db")
}

/// Writes a database whose manifest 1 holds `payloads`, one frame each, and is current.
fn write_manifest(storage: &dyn Storage, payloads: &[Vec<u8>]) {
    storage
        .create(&dir().join("IDENTITY"))
        .unwrap()
        .sync()
        .unwrap();
    let mut file = storage.create(&dir().join(MANIFEST)).unwrap();
    for payload in payloads {
        let mut frame = vec![0; frame_bytes(payload.len()).unwrap()];
        encode_frame(1, payload, &mut frame).unwrap();
        file.append(&frame).unwrap();
    }
    file.sync().unwrap();
    let mut current = storage.create(&dir().join(CURRENT)).unwrap();
    current.append(format!("{MANIFEST}\n").as_bytes()).unwrap();
    current.sync().unwrap();
}

fn encoded(edit: &ManifestEdit) -> Vec<u8> {
    let mut bytes = Vec::new();
    edit.encode(&mut bytes);
    bytes
}

#[test]
fn a_new_manifest_begins_with_the_store_format() {
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new());
    drop(ManifestStore::create(Arc::clone(&storage), dir(), &Version::empty(), 1, None).unwrap());
    let bytes = read_all(storage.as_ref(), &dir().join(MANIFEST), "manifest").unwrap();
    let mut frames = FrameReader::new(&bytes, 1);
    let LogRead::Frame(first) = frames.next_frame() else {
        panic!("the manifest holds no edit");
    };
    assert_eq!(peek_store_format(first).unwrap(), Some(STORE_FORMAT));
}

#[test]
fn a_store_of_a_later_format_is_refused_before_its_edits_are_read() {
    let storage = MemoryStorage::new();
    let later = STORE_FORMAT + 1;
    // The second edit is one this build cannot decode: the gate must refuse before reaching it.
    write_manifest(
        &storage,
        &[
            encoded(&ManifestEdit {
                store_format: Some(later),
                next_file: Some(2),
                ..ManifestEdit::default()
            }),
            vec![2, 0x09],
        ],
    );
    let refused = ManifestStore::recover(&storage, dir(), None);
    assert!(
        matches!(
            &refused,
            Err(Error::UnsupportedFormat { found, newest, .. })
                if *found == later && *newest == STORE_FORMAT
        ),
        "{refused:?}"
    );
}

#[test]
fn a_store_written_before_the_format_was_recorded_opens() {
    let storage = MemoryStorage::new();
    // A version 1 edit: flushed sequence 5, next file 9.
    write_manifest(&storage, &[vec![1, 1, 5, 2, 9]]);
    let (version, number) = ManifestStore::recover(&storage, dir(), None)
        .unwrap()
        .unwrap();
    assert_eq!(
        (version.flushed_sequence(), version.next_file(), number),
        (5, 9, 1)
    );
}

#[test]
fn a_store_of_every_earlier_format_opens() {
    for earlier in 1..STORE_FORMAT {
        let storage = MemoryStorage::new();
        write_manifest(
            &storage,
            &[encoded(&ManifestEdit {
                store_format: Some(earlier),
                next_file: Some(7),
                ..ManifestEdit::default()
            })],
        );
        let (version, _) = ManifestStore::recover(&storage, dir(), None)
            .unwrap()
            .unwrap();
        assert_eq!(version.next_file(), 7, "format {earlier}");
    }
}

#[test]
fn a_manifest_holding_no_edit_is_corruption_not_an_empty_store() {
    let storage = MemoryStorage::new();
    write_manifest(&storage, &[]);
    assert!(matches!(
        ManifestStore::recover(&storage, dir(), None),
        Err(Error::Corruption {
            what: "manifest",
            ..
        })
    ));
}

#[test]
fn a_first_edit_without_the_store_format_is_corruption() {
    let storage = MemoryStorage::new();
    write_manifest(&storage, &[vec![2, 1, 5]]);
    assert!(matches!(
        ManifestStore::recover(&storage, dir(), None),
        Err(Error::Corruption {
            what: "manifest",
            ..
        })
    ));
}

/// Opening refuses the store and leaves every file as it was.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn opening_a_store_of_a_later_format_changes_nothing() {
    let storage = MemoryStorage::new();
    write_manifest(
        &storage,
        &[encoded(&ManifestEdit {
            store_format: Some(STORE_FORMAT + 1),
            next_file: Some(2),
            ..ManifestEdit::default()
        })],
    );
    let before = storage.list(dir()).unwrap();
    let refused =
        Db::open_with_storage(dir(), DbOptions::default(), Arc::new(storage.clone())).await;
    assert!(
        matches!(&refused, Err(Error::UnsupportedFormat { .. })),
        "{:?}",
        refused.err()
    );
    assert_eq!(storage.list(dir()).unwrap(), before);
}
