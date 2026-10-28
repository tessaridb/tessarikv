//! Blob files: values written as sealed blocks come back from the handle their index names, and
//! damaged bytes or a damaged index are refused by name.

use super::Compression;
use super::blob::{BlobBuilder, BlobIndex, open_blob};
use crate::error::Error;

fn values() -> Vec<Vec<u8>> {
    (0..20_u8)
        .map(|n| vec![n; 100 + usize::from(n) * 37])
        .collect()
}

/// A blob file numbered 9 holding [`values`], built in pieces as a writer takes them.
fn written(compression: Compression) -> (Vec<u8>, Vec<BlobIndex>) {
    let mut builder = BlobBuilder::new(9, compression);
    let mut file = Vec::new();
    let mut indexes = Vec::new();
    for value in values() {
        indexes.push(builder.add(&value).expect("added"));
        file.extend(builder.take_finished());
    }
    file.extend(builder.finish());
    (file, indexes)
}

fn read(file: &[u8], index: &BlobIndex) -> crate::Result<Vec<u8>> {
    let start = usize::try_from(index.handle.offset()).expect("small");
    let end = start + usize::try_from(index.handle.size()).expect("small");
    open_blob(file[start..end].to_vec(), index.handle, None)
}

#[test]
fn every_value_reads_back_from_its_index() {
    for compression in [Compression::None, Compression::Lz4] {
        let (file, indexes) = written(compression);
        for (value, index) in values().iter().zip(&indexes) {
            assert_eq!(index.file, 9);
            let decoded = BlobIndex::decode(&index.encode()).expect("decoded");
            assert_eq!(&decoded, index);
            assert_eq!(&read(&file, index).expect("read"), value);
        }
    }
}

#[test]
fn a_damaged_value_or_index_is_refused() {
    let (mut file, indexes) = written(Compression::None);
    let at = usize::try_from(indexes[3].handle.offset()).expect("small") + 10;
    file[at] ^= 1;
    assert!(matches!(
        read(&file, &indexes[3]),
        Err(Error::Malformed {
            what: "table block",
            reason: "checksum mismatch"
        })
    ));
    assert!(read(&file, &indexes[4]).is_ok(), "a neighbour is untouched");
    let short = BlobIndex::decode(&indexes[0].encode()[..23]);
    assert!(matches!(
        short,
        Err(Error::Malformed {
            what: "blob index",
            reason: "not 24 bytes"
        })
    ));
}

#[test]
fn a_table_names_the_blob_files_its_entries_point_into() {
    use super::TableBuilder;
    use super::tests::{bytewise, open, options, whole};
    let mut builder = TableBuilder::new(options(Compression::None), bytewise, whole);
    builder.add(b"key", b"value").expect("added");
    for file in [12, 7, 12, 30] {
        builder.references_blob(file);
    }
    let table = open(builder.finish().expect("finished")).expect("opens");
    assert_eq!(table.blob_files(), &[7, 12, 30]);
    let mut plain = TableBuilder::new(options(Compression::None), bytewise, whole);
    plain.add(b"key", b"value").expect("added");
    assert!(
        open(plain.finish().expect("finished"))
            .expect("opens")
            .blob_files()
            .is_empty()
    );
}

#[test]
fn a_damaged_blob_file_list_is_refused_by_name() {
    let listed = |files: &[u8]| {
        let mut properties = super::block::BlockBuilder::new(1, |left, right| left.cmp(right));
        properties.add(b"blob_files", files).expect("added");
        super::blob::blob_files_of(properties.finish())
    };
    let ids = |list: &[u64]| -> Vec<u8> { list.iter().flat_map(|id| id.to_le_bytes()).collect() };
    assert_eq!(&*listed(&ids(&[3, 9])).expect("whole"), &[3, 9]);
    for (files, reason) in [
        (
            ids(&[3, 9])[..12].to_vec(),
            "blob file list is not whole numbers",
        ),
        (ids(&[9, 3]), "blob file list is not increasing"),
        (ids(&[3, 3]), "blob file list is not increasing"),
    ] {
        let refused = listed(&files);
        assert!(
            matches!(&refused, Err(Error::Malformed { what: "table properties", reason: why }) if *why == reason),
            "{refused:?}"
        );
    }
}
