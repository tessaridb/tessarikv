//! An encrypted database's log: frames sealed by their writers, replayed after a crash, and a
//! frame changed under a valid checksum refused rather than replayed.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use tessarikv_format::{FRAME_HEADER_BYTES, SEAL_HEADER_BYTES, frame_header, payload_checksum};

use super::Db;
use super::flush_tests::PATH;
use crate::log::fault::FaultStorage;
use crate::storage::Storage as _;
use crate::{
    DbOptions, Error, KeyProvider, KeyspaceId, ReadOptions, RootKey, WalRecovery, WriteBatch,
    WriteOptions,
};

#[derive(Debug)]
struct Key;

impl KeyProvider for Key {
    fn root_key(&self, key_id: u32) -> Option<RootKey> {
        (key_id == 1).then(|| RootKey::new([7; 32]))
    }
}

fn options(encrypted: bool, recovery: WalRecovery) -> DbOptions {
    let builder = DbOptions::builder().wal_recovery(recovery);
    let builder = if encrypted {
        builder.encryption(Arc::new(Key), 1)
    } else {
        builder
    };
    builder.build().expect("valid")
}

/// One small batch, then one large enough that its checksum is taken in parts.
async fn write(db: &Db) {
    for (index, size) in [(0_u32, 1), (1, 400)] {
        let mut batch = WriteBatch::new();
        for entry in 0..size {
            batch
                .put(
                    KeyspaceId::DEFAULT,
                    format!("secret-key-{index}-{entry:04}").as_bytes(),
                    format!("secret-value-{index}-{entry:04}").as_bytes(),
                )
                .expect("entry");
        }
        // The default write is synced: it survives the power loss the image takes.
        db.write(&batch, &WriteOptions::default())
            .await
            .expect("written");
    }
}

async fn found(db: &Db, key: &str) -> bool {
    db.get(KeyspaceId::DEFAULT, key.as_bytes(), &ReadOptions::default())
        .await
        .expect("read")
        .is_some()
}

fn segments(storage: &FaultStorage) -> Vec<PathBuf> {
    let mut names: Vec<String> = storage
        .list(Path::new(PATH))
        .expect("listable")
        .into_iter()
        .filter(|name| name.ends_with(".log"))
        .collect();
    names.sort();
    names
        .iter()
        .map(|name| Path::new(PATH).join(name))
        .collect()
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}

/// A crash image of a database that wrote [`write`]'s batches and stayed open.
async fn crashed(encrypted: bool) -> FaultStorage {
    let storage = FaultStorage::default();
    let db = Db::open_with_storage(
        PATH,
        options(encrypted, WalRecovery::PointInTime),
        Arc::new(storage.clone()),
    )
    .await
    .expect("opened");
    write(&db).await;
    let image = storage.crash(Path::new(PATH), true);
    db.close().await.expect("closed");
    image
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_sealed_log_holds_no_plaintext_and_replays_after_a_crash() {
    let image = crashed(true).await;
    let logs = segments(&image);
    assert!(!logs.is_empty());
    for path in &logs {
        let bytes = image.bytes(path);
        assert_eq!(bytes.get(..4), Some(&b"TKVS"[..]), "{path:?} starts sealed");
        assert!(!contains(&bytes, b"secret-"), "{path:?} shows plaintext");
    }
    let db = Db::open_with_storage(
        PATH,
        options(true, WalRecovery::PointInTime),
        Arc::new(image),
    )
    .await
    .expect("recovered");
    assert!(found(&db, "secret-key-0-0000").await);
    assert!(found(&db, "secret-key-1-0399").await);
    db.close().await.expect("closed");
    let plain = crashed(false).await;
    let shown = segments(&plain)
        .iter()
        .any(|path| contains(&plain.bytes(path), b"secret-"));
    assert!(shown, "control: a plain log shows its keys");
}

/// Changes the first byte of the first frame's sealed batch and, when `fix_checksum`, writes the
/// frame checksum that matches, so the frame is no longer torn but altered.
fn alter_first_frame(storage: &FaultStorage, fix_checksum: bool) {
    let path = segments(storage).remove(0);
    let log_number: u64 = path
        .file_stem()
        .and_then(|stem| stem.to_str())
        .and_then(|stem| stem.parse().ok())
        .expect("a log number");
    let mut bytes = storage.bytes(&path);
    let header_at = SEAL_HEADER_BYTES;
    let payload_at = header_at + FRAME_HEADER_BYTES;
    let payload_len = u32::from_le_bytes(bytes[header_at + 4..header_at + 8].try_into().unwrap());
    let payload_end = payload_at + usize::try_from(payload_len).unwrap();
    bytes[payload_at + 8] ^= 1;
    if fix_checksum {
        let checksum = payload_checksum(&bytes[payload_at..payload_end]);
        let header = frame_header(log_number, payload_end - payload_at, checksum).unwrap();
        bytes[header_at..payload_at].copy_from_slice(&header);
    }
    let mut file = storage.create(&path).expect("replaced");
    file.append(&bytes).expect("written");
    file.sync().expect("synced");
}

/// A frame altered under a checksum that holds is not a torn write: no recovery mode replays
/// around it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_frame_altered_under_a_valid_checksum_fails_the_open() {
    for recovery in [WalRecovery::PointInTime, WalRecovery::Absolute] {
        let image = crashed(true).await;
        alter_first_frame(&image, true);
        let opened =
            Db::open_with_storage(PATH, options(true, recovery), Arc::new(image.clone())).await;
        assert!(
            matches!(&opened, Err(Error::Corruption { detail, .. }) if detail.contains("authentication")),
            "{recovery:?}: {:?}",
            opened.as_ref().err()
        );
    }
}

/// A frame torn in a sealed log is still a torn tail: point-in-time recovery cuts it away and
/// opens without its batches.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_torn_sealed_frame_is_cut_like_any_torn_tail() {
    let image = crashed(true).await;
    alter_first_frame(&image, false);
    let db = Db::open_with_storage(
        PATH,
        options(true, WalRecovery::PointInTime),
        Arc::new(image),
    )
    .await
    .expect("recovered to the valid prefix");
    assert!(!found(&db, "secret-key-0-0000").await);
    assert!(!found(&db, "secret-key-1-0000").await);
    db.close().await.expect("closed");
}

/// The sealed batches of the frames in the segment at `path`, tag included.
fn sealed_batches(storage: &FaultStorage, path: &Path) -> Vec<Vec<u8>> {
    let bytes = storage.bytes(path);
    let mut at = SEAL_HEADER_BYTES;
    let mut batches = Vec::new();
    while at + FRAME_HEADER_BYTES <= bytes.len() {
        let len = u32::from_le_bytes(bytes[at + 4..at + 8].try_into().expect("four bytes"));
        let payload = at + FRAME_HEADER_BYTES;
        let end = payload + usize::try_from(len).expect("fits");
        // The first eight bytes of a payload are its sequence number, in the clear.
        batches.push(bytes[payload + 8..end].to_vec());
        at = end;
    }
    batches
}

/// Two identical batches seal to different bytes: each frame's nonce is its own sequence.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn identical_batches_seal_differently() {
    let storage = FaultStorage::default();
    let db = Db::open_with_storage(
        PATH,
        options(true, WalRecovery::PointInTime),
        Arc::new(storage.clone()),
    )
    .await
    .expect("opened");
    let mut batch = WriteBatch::new();
    batch
        .put(KeyspaceId::DEFAULT, b"same", b"bytes")
        .expect("entry");
    for _ in 0..2 {
        db.write(&batch, &WriteOptions::default())
            .await
            .expect("written");
    }
    let batches = sealed_batches(&storage, &segments(&storage)[0]);
    db.close().await.expect("closed");
    assert_eq!(batches.len(), 2);
    assert_eq!(batches[0].len(), batches[1].len());
    assert_ne!(batches[0], batches[1]);
}

/// A segment cut inside its seal header is a torn write: refused under absolute recovery.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_segment_cut_inside_its_header_is_torn() {
    let image = crashed(true).await;
    let path = segments(&image).remove(0);
    let bytes = image.bytes(&path);
    let mut file = image.create(&path).expect("replaced");
    file.append(&bytes[..20]).expect("written");
    file.sync().expect("synced");
    let absolute = Db::open_with_storage(
        PATH,
        options(true, WalRecovery::Absolute),
        Arc::new(image.clone()),
    )
    .await;
    assert!(
        matches!(absolute, Err(Error::Corruption { .. })),
        "{:?}",
        absolute.as_ref().err()
    );
    let db = Db::open_with_storage(
        PATH,
        options(true, WalRecovery::PointInTime),
        Arc::new(image),
    )
    .await
    .expect("cut back to nothing");
    assert!(!found(&db, "secret-key-0-0000").await);
    db.close().await.expect("closed");
}
