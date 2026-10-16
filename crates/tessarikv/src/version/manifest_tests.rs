use std::path::Path;
use std::sync::Arc;

use tessarikv_format::{ManifestEdit, TableMeta};

use super::Version;
use super::manifest::{CURRENT, ManifestStore};
use crate::log::fault::FaultStorage;
use crate::storage::Storage;
use crate::{Error, MemoryStorage};

fn dir() -> &'static Path {
    Path::new("db")
}

fn table(id: u64, max_sequence: u64) -> TableMeta {
    TableMeta {
        keyspace: 0,
        level: 0,
        id,
        size: 10,
        smallest: vec![b'a'; 9],
        largest: vec![b'z'; 9],
        min_sequence: 1,
        max_sequence,
    }
}

fn flush(id: u64, sequence: u64) -> ManifestEdit {
    ManifestEdit {
        flushed_sequence: Some(sequence),
        next_file: Some(id + 1),
        added: vec![table(id, sequence)],
        removed: vec![],
        ..ManifestEdit::default()
    }
}

fn ids(version: &Version) -> Vec<u64> {
    version.tables(0, 0).iter().map(|table| table.id).collect()
}

#[test]
fn a_new_store_has_no_manifest_and_edits_survive_a_reopen() {
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new());
    assert!(
        ManifestStore::recover(storage.as_ref(), dir(), None)
            .unwrap()
            .is_none()
    );
    let mut store =
        ManifestStore::create(Arc::clone(&storage), dir(), &Version::empty(), 1, None).unwrap();
    store.append(&flush(1, 10)).unwrap();
    store.append(&flush(2, 20)).unwrap();
    let (version, number) = ManifestStore::recover(storage.as_ref(), dir(), None)
        .unwrap()
        .unwrap();
    assert_eq!(number, 1);
    assert_eq!(ids(&version), vec![2, 1]);
    assert_eq!((version.flushed_sequence(), version.next_file()), (20, 3));
    assert_eq!(&*store.version(), &version);
}

#[test]
fn rotating_writes_the_whole_version_to_a_new_manifest_and_removes_the_old() {
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new());
    let mut store =
        ManifestStore::create(Arc::clone(&storage), dir(), &Version::empty(), 1, None).unwrap();
    store.append(&flush(1, 10)).unwrap();
    let (version, number) = ManifestStore::recover(storage.as_ref(), dir(), None)
        .unwrap()
        .unwrap();
    let rotated =
        ManifestStore::create(Arc::clone(&storage), dir(), &version, number + 1, None).unwrap();
    drop(rotated);
    let names = storage.list(dir()).unwrap();
    assert!(
        names.contains(&"MANIFEST-00000000000000000002".to_owned()),
        "{names:?}"
    );
    assert!(
        !names.contains(&"MANIFEST-00000000000000000001".to_owned()),
        "{names:?}"
    );
    let (reopened, number) = ManifestStore::recover(storage.as_ref(), dir(), None)
        .unwrap()
        .unwrap();
    assert_eq!((ids(&reopened), number), (vec![1], 2));
}

#[test]
fn a_torn_last_edit_is_dropped_and_earlier_ones_kept() {
    let storage = FaultStorage::default();
    let shared: Arc<dyn Storage> = Arc::new(storage.clone());
    let mut store =
        ManifestStore::create(Arc::clone(&shared), dir(), &Version::empty(), 1, None).unwrap();
    store.append(&flush(1, 10)).unwrap();
    let path = dir().join("MANIFEST-00000000000000000001");
    storage.append_raw(&path, &[0x55; 7]);
    let (version, _) = ManifestStore::recover(&storage, dir(), None)
        .unwrap()
        .unwrap();
    assert_eq!(ids(&version), vec![1]);
}

#[test]
fn a_damaged_current_or_a_missing_manifest_is_refused() {
    let storage = MemoryStorage::new();
    let mut current = storage.create(&dir().join(CURRENT)).unwrap();
    current.append(b"MANIFEST-x\n").unwrap();
    current.sync().unwrap();
    assert!(matches!(
        ManifestStore::recover(&storage, dir(), None),
        Err(Error::Corruption {
            what: "manifest",
            ..
        })
    ));
    let storage = MemoryStorage::new();
    let mut current = storage.create(&dir().join(CURRENT)).unwrap();
    current.append(b"MANIFEST-00000000000000000004\n").unwrap();
    current.sync().unwrap();
    assert!(matches!(
        ManifestStore::recover(&storage, dir(), None),
        Err(Error::NotFound { .. })
    ));
}

#[test]
fn a_contradicting_edit_is_refused_and_not_written() {
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new());
    let mut store =
        ManifestStore::create(Arc::clone(&storage), dir(), &Version::empty(), 1, None).unwrap();
    store.append(&flush(1, 10)).unwrap();
    let mut duplicate = flush(1, 11);
    duplicate.next_file = None;
    assert!(store.append(&duplicate).is_err());
    let (version, _) = ManifestStore::recover(storage.as_ref(), dir(), None)
        .unwrap()
        .unwrap();
    assert_eq!((ids(&version), version.flushed_sequence()), (vec![1], 10));
}

#[test]
fn an_appended_edit_and_the_current_name_survive_a_power_loss() {
    let storage = FaultStorage::default();
    let shared: Arc<dyn Storage> = Arc::new(storage.clone());
    let mut store =
        ManifestStore::create(Arc::clone(&shared), dir(), &Version::empty(), 1, None).unwrap();
    store.append(&flush(1, 10)).unwrap();
    let after = storage.crash(dir(), true);
    let (version, number) = ManifestStore::recover(&after, dir(), None)
        .unwrap()
        .unwrap();
    assert_eq!((ids(&version), number), (vec![1], 1));
}

/// The names of the manifests in the directory.
fn manifests(storage: &dyn Storage) -> Vec<String> {
    storage
        .list(dir())
        .unwrap()
        .into_iter()
        .filter(|name| name.starts_with("MANIFEST-"))
        .collect()
}

/// Past its limit a manifest is replaced by a new one holding the version as one edit: the
/// directory keeps one manifest, it stays near the limit, and every edit survives a reopen.
#[test]
fn a_manifest_past_its_limit_rolls_into_a_new_one() {
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new());
    let mut store = ManifestStore::create(Arc::clone(&storage), dir(), &Version::empty(), 1, None)
        .unwrap()
        .rolling_at(2048, None);
    for id in 1..=200 {
        store.append(&flush(id, id * 10)).unwrap();
        assert_eq!(manifests(storage.as_ref()).len(), 1, "after edit {id}");
    }
    let (version, number) = ManifestStore::recover(storage.as_ref(), dir(), None)
        .unwrap()
        .unwrap();
    assert!(number > 1, "rolled: manifest {number}");
    assert_eq!(ids(&version), ids(&store.version()));
    assert_eq!(version.tables(0, 0).len(), 200);
    assert_eq!(version.flushed_sequence(), 2000);
    let path = dir().join(&manifests(storage.as_ref())[0]);
    let len = storage.open(&path).unwrap().len().unwrap();
    // One edit holding every table can itself pass the limit; the next append rolls again.
    assert!(len < 2048 + 200 * 64, "{len} bytes");
}

/// Without a limit nothing rolls.
#[test]
fn a_manifest_without_a_limit_never_rolls() {
    let storage: Arc<dyn Storage> = Arc::new(MemoryStorage::new());
    let mut store =
        ManifestStore::create(Arc::clone(&storage), dir(), &Version::empty(), 1, None).unwrap();
    for id in 1..=200 {
        store.append(&flush(id, id * 10)).unwrap();
    }
    let (_, number) = ManifestStore::recover(storage.as_ref(), dir(), None)
        .unwrap()
        .unwrap();
    assert_eq!(number, 1);
}
