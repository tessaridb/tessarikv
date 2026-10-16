//! One contract, run against every backend: what the engine relies on from a file system.

use std::path::{Path, PathBuf};

use super::*;
use crate::{Error, ErrorCategory};

fn backends() -> Vec<(Box<dyn Storage>, PathBuf, Option<tempfile::TempDir>)> {
    let dir = tempfile::tempdir().unwrap();
    let os_root = dir.path().join("db");
    vec![
        (Box::new(MemoryStorage::new()), PathBuf::from("/db"), None),
        (Box::new(OsStorage), os_root, Some(dir)),
    ]
}

fn write(storage: &dyn Storage, path: &Path, bytes: &[u8]) {
    let mut file = storage.create(path).unwrap();
    file.append(bytes).unwrap();
    file.sync().unwrap();
}

#[test]
fn appended_bytes_read_back_at_any_offset() {
    for (storage, root, _guard) in backends() {
        storage.create_dir_all(&root).unwrap();
        let path = root.join("000001.log");
        let mut file = storage.create(&path).unwrap();
        file.append(b"hello ").unwrap();
        file.append(b"world").unwrap();
        file.sync().unwrap();
        assert_eq!(file.len(), 11);
        let reader = storage.open(&path).unwrap();
        let mut buf = [0; 5];
        reader.read_at(6, &mut buf).unwrap();
        assert_eq!(&buf, b"world", "{storage:?}");
        assert_eq!(reader.len().unwrap(), 11);
    }
}

#[test]
fn a_read_past_the_end_is_a_short_read() {
    for (storage, root, _guard) in backends() {
        storage.create_dir_all(&root).unwrap();
        let path = root.join("a");
        write(storage.as_ref(), &path, b"abc");
        let mut buf = [0; 4];
        let error = storage
            .open(&path)
            .unwrap()
            .read_at(0, &mut buf)
            .unwrap_err();
        assert_eq!(error.code(), "short_read", "{storage:?}");
        assert_eq!(error.category(), ErrorCategory::Corruption);
    }
}

#[test]
fn rename_replaces_the_target_and_removes_the_source() {
    for (storage, root, _guard) in backends() {
        storage.create_dir_all(&root).unwrap();
        let (from, to) = (root.join("CURRENT.tmp"), root.join("CURRENT"));
        write(storage.as_ref(), &to, b"old");
        write(storage.as_ref(), &from, b"new");
        storage.rename(&from, &to).unwrap();
        storage.sync_dir(&root).unwrap();
        let mut buf = [0; 3];
        storage.open(&to).unwrap().read_at(0, &mut buf).unwrap();
        assert_eq!(&buf, b"new", "{storage:?}");
        assert!(!storage.exists(&from).unwrap());
    }
}

#[test]
fn missing_files_are_not_found() {
    for (storage, root, _guard) in backends() {
        storage.create_dir_all(&root).unwrap();
        let missing = root.join("nope");
        assert!(matches!(
            storage.open(&missing),
            Err(Error::NotFound { .. })
        ));
        assert!(matches!(
            storage.remove(&missing),
            Err(Error::NotFound { .. })
        ));
        assert!(matches!(
            storage.rename(&missing, &root.join("x")),
            Err(Error::NotFound { .. })
        ));
    }
}

#[test]
fn list_names_the_files_of_one_directory() {
    for (storage, root, _guard) in backends() {
        storage.create_dir_all(&root).unwrap();
        write(storage.as_ref(), &root.join("a"), b"1");
        write(storage.as_ref(), &root.join("b"), b"2");
        storage.remove(&root.join("a")).unwrap();
        assert_eq!(storage.list(&root).unwrap(), ["b"], "{storage:?}");
    }
}

#[test]
fn the_lock_is_exclusive_until_released() {
    for (storage, root, _guard) in backends() {
        storage.create_dir_all(&root).unwrap();
        let path = root.join("LOCK");
        let held = storage.lock(&path).unwrap();
        let refused = storage.lock(&path).unwrap_err();
        assert_eq!(
            refused.category(),
            ErrorCategory::Unavailable,
            "{storage:?}"
        );
        drop(held);
        assert!(storage.lock(&path).is_ok(), "{storage:?}");
    }
}

/// A reader of a replaced file keeps the bytes it opened (how an unlinked file behaves).
#[test]
fn a_reader_of_a_replaced_file_keeps_its_bytes() {
    let storage = MemoryStorage::new();
    let path = Path::new("/db/x");
    write(&storage, path, b"first");
    let old = storage.open(path).unwrap();
    write(&storage, path, b"second");
    let mut buf = [0; 5];
    old.read_at(0, &mut buf).unwrap();
    assert_eq!(&buf, b"first");
}

#[test]
fn a_read_that_must_not_block_fills_the_buffer_only_where_the_backend_promises_it() {
    for (storage, root, guard) in backends() {
        storage.create_dir_all(&root).unwrap();
        let path = root.join("000001.sst");
        write(&*storage, &path, b"hello world");
        let file = storage.open(&path).unwrap();
        let mut buf = [0; 5];
        let filled = file.try_read_at(6, &mut buf).unwrap();
        // Memory never waits; on Linux a just-written file is in the page cache. Overlay file
        // systems refuse such reads, so a Linux run keeps TMPDIR on ext4, XFS or Btrfs.
        let promised = guard.is_none() || cfg!(any(target_os = "linux", target_os = "android"));
        assert_eq!(
            filled, promised,
            "{storage:?} (on Linux: is TMPDIR on overlayfs?)"
        );
        if filled {
            assert_eq!(&buf, b"world");
        }
    }
}

#[test]
fn a_read_that_must_not_block_past_the_end_never_reports_a_full_buffer() {
    for (storage, root, guard) in backends() {
        storage.create_dir_all(&root).unwrap();
        let path = root.join("000001.sst");
        write(&*storage, &path, b"hello");
        let file = storage.open(&path).unwrap();
        let mut buf = [0; 8];
        let result = file.try_read_at(2, &mut buf);
        if guard.is_none() {
            assert!(matches!(result, Err(Error::ShortRead { .. })), "{result:?}");
        } else {
            assert!(matches!(result, Ok(false)), "{result:?}");
        }
    }
}

#[test]
fn a_file_that_cannot_promise_a_read_will_not_block_says_so() {
    struct Plain;
    impl ReadableFile for Plain {
        fn read_at(&self, _offset: u64, _buf: &mut [u8]) -> crate::Result<()> {
            Ok(())
        }
        fn len(&self) -> crate::Result<u64> {
            Ok(0)
        }
    }
    assert!(!Plain.try_read_at(0, &mut [0; 4]).unwrap());
}
