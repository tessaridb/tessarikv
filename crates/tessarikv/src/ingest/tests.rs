use tessarikv_format::{TableBuilder, TableOptions};
use tessarikv_types::{SequenceNumber, Trailer, ValueKind, compare_internal, encode_internal};

use super::*;

fn refusal<T: std::fmt::Debug>(result: Result<T>) -> &'static str {
    match result {
        Err(Error::InvalidArgument { reason, .. }) => reason,
        other => panic!("expected a refusal, got {other:?}"),
    }
}

#[test]
fn a_written_table_decodes_to_its_entries() {
    let mut table = TableWriter::new();
    table.put(b"a", b"1").expect("entry");
    table.delete(b"b").expect("entry");
    table.put(b"c", b"").expect("entry");
    let entries = decode(table.finish().expect("finished")).expect("decoded");
    assert_eq!(
        entries,
        vec![
            (b"a".to_vec(), ValueKind::Value, b"1".to_vec()),
            (b"b".to_vec(), ValueKind::Delete, Vec::new()),
            (b"c".to_vec(), ValueKind::Value, Vec::new()),
        ]
    );
}

#[test]
fn a_writer_refuses_keys_out_of_order_and_an_empty_table() {
    let mut table = TableWriter::new();
    assert_eq!(refusal(table.put(b"", b"x")), "an ingested key is empty");
    table.put(b"b", b"x").expect("entry");
    assert_eq!(
        refusal(table.put(b"b", b"y")),
        "ingested keys are not strictly increasing"
    );
    assert_eq!(
        refusal(table.delete(b"a")),
        "ingested keys are not strictly increasing"
    );
    assert_eq!(
        refusal(TableWriter::new().finish()),
        "an ingested table holds no entry"
    );
}

/// A table built directly, as a crafted file could be.
fn crafted(entries: &[(&[u8], u64, ValueKind)]) -> Vec<u8> {
    let options = TableOptions {
        block_bytes: 4096,
        restart_interval: 16,
        bloom_bits_per_key: 1,
        filter: tessarikv_format::FilterPolicy::Bloom,
        compression: tessarikv_format::Compression::None,
        prefix_length: None,
    };
    let mut builder = TableBuilder::new(options, compare_internal, user_key);
    for (key, sequence, kind) in entries {
        let mut internal = Vec::new();
        let sequence = SequenceNumber::new(*sequence).expect("in range");
        encode_internal(key, Trailer::new(sequence, *kind), &mut internal);
        builder.add(&internal, b"v").expect("added");
    }
    builder.finish().expect("finished")
}

#[test]
fn a_table_a_writer_did_not_write_is_refused_by_name() {
    assert_eq!(
        refusal(decode(b"no table here".to_vec())),
        "an ingested table does not decode"
    );
    assert_eq!(
        refusal(decode(crafted(&[(b"a", 7, ValueKind::Value)]))),
        "an ingested table was not written by a table writer"
    );
    assert_eq!(
        refusal(decode(crafted(&[(b"a", 0, ValueKind::Merge)]))),
        "an ingested table holds more than puts and deletes"
    );
    assert_eq!(
        refusal(decode(crafted(&[(b"", 0, ValueKind::Value)]))),
        "ingested keys are not strictly increasing"
    );
    // Two versions of one key: increasing as internal keys, not as user keys.
    assert_eq!(
        refusal(decode(crafted(&[
            (b"a", 0, ValueKind::Value),
            (b"a", 0, ValueKind::Delete)
        ]))),
        "ingested keys are not strictly increasing"
    );
}
