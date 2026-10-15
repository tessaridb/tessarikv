use tessarikv_types::{KeyspaceId, ValueKind};

use super::{BatchEntry, WriteBatch};
use crate::Error;

/// An entry as owned data: keyspace, kind, key, value.
type Owned = (u32, ValueKind, Vec<u8>, Option<Vec<u8>>);

fn entries(batch: &WriteBatch) -> Vec<Owned> {
    batch
        .iter()
        .map(|entry: BatchEntry<'_>| {
            (
                entry.keyspace.get(),
                entry.kind,
                entry.key.to_vec(),
                entry.value.map(<[u8]>::to_vec),
            )
        })
        .collect()
}

fn sample() -> WriteBatch {
    let mut batch = WriteBatch::new();
    batch.put(KeyspaceId::new(3), b"apple", b"red").unwrap();
    batch.delete(KeyspaceId::DEFAULT, b"pear").unwrap();
    batch.put(KeyspaceId::DEFAULT, b"", b"").unwrap();
    batch
}

#[test]
fn entries_come_back_in_the_order_they_were_added() {
    let batch = sample();
    assert_eq!(batch.len(), 3);
    assert!(!batch.is_empty());
    assert_eq!(
        entries(&batch),
        vec![
            (
                3,
                ValueKind::Value,
                b"apple".to_vec(),
                Some(b"red".to_vec())
            ),
            (0, ValueKind::Delete, b"pear".to_vec(), None),
            (0, ValueKind::Value, Vec::new(), Some(Vec::new())),
        ]
    );
}

#[test]
fn encoded_bytes_decode_to_the_same_batch() {
    let batch = sample();
    let decoded = WriteBatch::from_bytes(batch.as_bytes().to_vec()).unwrap();
    assert_eq!(decoded.len(), batch.len());
    assert_eq!(entries(&decoded), entries(&batch));
    assert_eq!(decoded.size_bytes(), batch.size_bytes());
}

#[test]
fn a_cleared_batch_is_empty_and_reusable() {
    let mut batch = sample();
    batch.clear();
    assert!(batch.is_empty());
    assert_eq!(entries(&batch), Vec::new());
    batch.put(KeyspaceId::DEFAULT, b"k", b"v").unwrap();
    assert_eq!(entries(&batch).len(), 1);
}

#[test]
fn an_oversized_key_is_refused_and_leaves_the_batch_unchanged() {
    let mut batch = sample();
    let before = batch.as_bytes().to_vec();
    let key = vec![0; tessarikv_constants::MAX_KEY_BYTES + 1];
    assert!(matches!(
        batch.put(KeyspaceId::DEFAULT, &key, b"v"),
        Err(Error::TooLarge { what: "key", .. })
    ));
    assert_eq!(batch.as_bytes(), &before[..]);
    assert_eq!(batch.len(), 3);
}

#[test]
fn every_truncation_of_a_valid_batch_is_refused() {
    let bytes = sample().as_bytes().to_vec();
    for end in 0..bytes.len() {
        assert!(
            WriteBatch::from_bytes(bytes[..end].to_vec()).is_err(),
            "accepted a batch cut at {end} of {}",
            bytes.len()
        );
    }
}

#[test]
fn malformed_batches_are_refused_with_their_reason() {
    let good = sample().as_bytes().to_vec();

    let mut wrong_version = good.clone();
    wrong_version[0] = 99;
    assert!(matches!(
        WriteBatch::from_bytes(wrong_version),
        Err(Error::UnsupportedVersion {
            what: "write batch",
            version: 99
        })
    ));

    let mut trailing = good.clone();
    trailing.push(0);
    assert!(matches!(
        WriteBatch::from_bytes(trailing),
        Err(Error::Malformed {
            what: "write batch",
            ..
        })
    ));

    let mut wrong_count = good.clone();
    wrong_count[1..5].copy_from_slice(&4_u32.to_le_bytes());
    assert!(WriteBatch::from_bytes(wrong_count).is_err());

    let mut unknown_kind = good.clone();
    unknown_kind[5] = 7;
    assert!(matches!(
        WriteBatch::from_bytes(unknown_kind),
        Err(Error::Malformed {
            what: "write batch entry kind",
            ..
        })
    ));
}

#[test]
fn a_huge_length_field_is_refused_without_allocating_it() {
    let mut bytes = vec![1];
    bytes.extend_from_slice(&1_u32.to_le_bytes());
    bytes.push(ValueKind::Value.to_byte());
    bytes.extend_from_slice(&0_u32.to_le_bytes());
    bytes.extend_from_slice(&u32::MAX.to_le_bytes());
    assert!(matches!(
        WriteBatch::from_bytes(bytes),
        Err(Error::TooLarge { what: "key", .. })
    ));
}
