//! Sealed tables: every block and the footer encrypted and bound to their place in the file.

use std::collections::BTreeMap;
use std::sync::Arc;

use super::block::Block;
use super::*;
use crate::error::{Error, Result};
use crate::seal::{Cipher, FileKind, FileSealer, HEADER_BYTES, RootKey, SealHeader};

const CIPHERS: [Cipher; 2] = [Cipher::Aes256Gcm, Cipher::ChaCha20Poly1305];

fn bytewise(left: &[u8], right: &[u8]) -> std::cmp::Ordering {
    left.cmp(right)
}

fn whole(key: &[u8]) -> &[u8] {
    key
}

fn header(cipher: Cipher) -> SealHeader {
    SealHeader {
        cipher,
        key_id: 1,
        salt: [5; 32],
    }
}

fn sealer(cipher: Cipher, root: u8) -> Arc<FileSealer> {
    Arc::new(FileSealer::new(
        &RootKey::new([root; 32]),
        &header(cipher),
        FileKind::Table,
    ))
}

fn model() -> BTreeMap<Vec<u8>, Vec<u8>> {
    (0..200_u32)
        .map(|index| {
            let key = format!("secret-key-{index:04}").into_bytes();
            let value =
                format!("secret-value-{index:04} of a block worth compressing").into_bytes();
            (key, value)
        })
        .collect()
}

fn options(compression: Compression) -> TableOptions {
    TableOptions {
        block_bytes: 256,
        restart_interval: 4,
        bloom_bits_per_key: 10,
        filter: crate::table::FilterPolicy::Bloom,
        compression,
        prefix_length: None,
    }
}

const RANGE: (&[u8], &[u8]) = (b"secret-key-0010", b"secret-key-0020");

/// A sealed table of [`model`], taken from the builder in pieces as a streaming writer would.
fn build_sealed(cipher: Cipher, compression: Compression) -> Vec<u8> {
    let mut builder = TableBuilder::sealed(
        options(compression),
        bytewise,
        whole,
        sealer(cipher, 7),
        &header(cipher),
    );
    let mut file = Vec::new();
    for (index, (key, value)) in model().iter().enumerate() {
        builder.add(key, value).unwrap();
        if index % 50 == 49 {
            assert_eq!(builder.drained(), u64::try_from(file.len()).unwrap());
            file.extend(builder.take_finished());
        }
    }
    builder.add_range_delete(RANGE.0, RANGE.1).unwrap();
    file.extend(builder.finish().unwrap());
    file
}

/// Key-value pairs read from a table.
type Entries = Vec<(Vec<u8>, Vec<u8>)>;

fn entries(table: &Table) -> (Entries, Result<()>) {
    let mut iter = table.iter();
    iter.seek_to_first();
    let mut entries = Vec::new();
    while iter.valid() {
        entries.push((iter.key().to_vec(), iter.value().to_vec()));
        iter.next();
    }
    (entries, iter.status())
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}

#[test]
fn a_sealed_table_reads_back_what_was_written() {
    for cipher in CIPHERS {
        for compression in [Compression::None, Compression::Lz4] {
            let file = build_sealed(cipher, compression);
            assert_eq!(SealHeader::decode(&file).unwrap(), header(cipher));
            let table = Table::open_sealed(file, sealer(cipher, 7), bytewise, whole).unwrap();
            let (read, status) = entries(&table);
            status.unwrap();
            assert_eq!(read, model().into_iter().collect::<Vec<_>>(), "{cipher:?}");
            assert_eq!(table.entries(), 200);
            assert_eq!(
                table.range_deletes().unwrap(),
                vec![(RANGE.0.to_vec(), RANGE.1.to_vec())]
            );
            assert!(table.may_contain(b"secret-key-0042"));
        }
    }
}

/// No key, value or range delete appears in a sealed file; the same table unsealed shows them.
#[test]
fn a_sealed_table_holds_no_plaintext() {
    let mut plain = TableBuilder::new(options(Compression::None), bytewise, whole);
    for (key, value) in model() {
        plain.add(&key, &value).unwrap();
    }
    let plain = plain.finish().unwrap();
    assert!(contains(&plain, b"secret-key-0000"), "control: plain shows");
    for cipher in CIPHERS {
        let file = build_sealed(cipher, Compression::None);
        assert!(!contains(&file, b"secret-"), "{cipher:?}");
    }
}

#[test]
fn a_sealed_table_opens_only_with_its_key_and_whole() {
    for cipher in CIPHERS {
        let file = build_sealed(cipher, Compression::Lz4);
        let wrong_key = Table::open_sealed(file.clone(), sealer(cipher, 8), bytewise, whole);
        assert!(matches!(wrong_key, Err(Error::Unauthenticated { .. })));
        assert!(
            Table::open(file.clone(), bytewise, whole).is_err(),
            "not plain"
        );
        let short = file[..file.len() - 1].to_vec();
        let short = Table::open_sealed(short, sealer(cipher, 7), bytewise, whole);
        assert!(matches!(short, Err(Error::Unauthenticated { .. })));
        let mut long = file.clone();
        long.push(0);
        let long = Table::open_sealed(long, sealer(cipher, 7), bytewise, whole);
        assert!(matches!(long, Err(Error::Unauthenticated { .. })));
        let mut changed = file;
        changed[HEADER_BYTES] ^= 1;
        let table = Table::open_sealed(changed, sealer(cipher, 7), bytewise, whole).unwrap();
        let (_, status) = entries(&table);
        assert!(matches!(status, Err(Error::Unauthenticated { .. })));
    }
}

/// A source with no block at hand, so a walk names the block it needs.
#[derive(Debug)]
struct Nothing;

impl BlockSource for Nothing {
    fn block(&self, _: BlockHandle) -> Result<Option<Block>> {
        Ok(None)
    }
}

/// A data block read by its handle opens at its own offset with the file's key, and nowhere else.
#[test]
fn a_data_block_opens_only_at_its_offset_with_its_key() {
    let cipher = Cipher::ChaCha20Poly1305;
    let file = build_sealed(cipher, Compression::None);
    let len = u64::try_from(file.len()).unwrap();
    let read = |offset: u64, buf: &mut [u8]| {
        let start = usize::try_from(offset).unwrap();
        buf.copy_from_slice(&file[start..start + buf.len()]);
        Ok::<(), ()>(())
    };
    let key = sealer(cipher, 7);
    let table =
        Table::open_with(len, read, Arc::new(Nothing), Some(&key), bytewise, whole).unwrap();
    let mut iter = table.iter();
    iter.seek_to_first();
    let handle = iter.pending().unwrap();
    assert_eq!(handle.offset(), u64::try_from(HEADER_BYTES).unwrap());
    let start = usize::try_from(handle.offset()).unwrap();
    let bytes = &file[start..start + usize::try_from(handle.size()).unwrap()];
    let block = unseal_block(bytes, handle.offset(), Some(&key), bytewise).unwrap();
    iter.resume(block);
    assert_eq!(iter.key(), b"secret-key-0000");
    let moved = unseal_block(bytes, handle.offset() + 1, Some(&key), bytewise);
    assert!(matches!(moved, Err(Error::Unauthenticated { .. })));
    let other = unseal_block(bytes, handle.offset(), Some(&sealer(cipher, 8)), bytewise);
    assert!(matches!(other, Err(Error::Unauthenticated { .. })));
    assert!(unseal_block(bytes, handle.offset(), None, bytewise).is_err());
}
