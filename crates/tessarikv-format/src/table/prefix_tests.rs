//! The prefix filter: a table written with a prefix length rules out a range that lies inside
//! one absent prefix, and answers "may hold" for every other range and for every older table.

use super::tests::{bytewise, open, options, whole};
use super::{Compression, TableBuilder};
use crate::error::Error;

/// Keys `aaaa-0` … over four-byte prefixes `aaaa`, `aaab`, `aaad`, plus a key shorter than the
/// prefix.
fn keys() -> Vec<Vec<u8>> {
    let mut keys: Vec<Vec<u8>> = ["aaaa", "aaab", "aaad"]
        .iter()
        .flat_map(|prefix| (0..20).map(move |n| format!("{prefix}-{n:02}").into_bytes()))
        .collect();
    keys.push(b"ab".to_vec());
    keys.sort();
    keys
}

fn build(prefix_length: Option<u32>) -> Vec<u8> {
    let mut table_options = options(Compression::None);
    table_options.prefix_length = prefix_length;
    let mut builder = TableBuilder::new(table_options, bytewise, whole);
    for key in keys() {
        builder.add(&key, b"value").expect("in order");
    }
    builder.finish().expect("finished")
}

#[test]
fn a_range_inside_one_absent_prefix_is_ruled_out() {
    let table = open(build(Some(4))).expect("opens");
    assert!(!table.may_hold_prefix_range(b"aaac", Some(b"aaad")));
    assert!(!table.may_hold_prefix_range(b"aaac-05", Some(b"aaac-09")));
    assert!(!table.may_hold_prefix_range(b"zzzz", Some(b"zzz{")));
}

#[test]
fn a_range_that_may_hold_keys_is_kept() {
    let table = open(build(Some(4))).expect("opens");
    // Present prefixes.
    assert!(table.may_hold_prefix_range(b"aaab", Some(b"aaac")));
    assert!(table.may_hold_prefix_range(b"aaad-03", Some(b"aaad-04")));
    // Ranges no single prefix covers: past the prefix's end, no upper end, a short lower end.
    assert!(table.may_hold_prefix_range(b"aaac", Some(b"aaad-00")));
    assert!(table.may_hold_prefix_range(b"aaac", None));
    assert!(table.may_hold_prefix_range(b"aa", Some(b"ab")));
}

#[test]
fn a_table_without_a_prefix_filter_may_hold_any_range() {
    let table = open(build(None)).expect("opens");
    assert!(table.may_hold_prefix_range(b"aaac", Some(b"aaad")));
    assert!(table.may_hold_prefix_range(b"zzzz", Some(b"zzz{")));
}

#[test]
fn every_key_reads_back_from_a_table_with_a_prefix_filter() {
    let table = open(build(Some(4))).expect("opens");
    let mut walk = table.iter();
    walk.seek_to_first();
    let (read, status) = super::tests::drain(&mut walk);
    status.expect("whole");
    let read: Vec<Vec<u8>> = read.into_iter().map(|(key, _)| key).collect();
    assert_eq!(read, keys());
}

/// A table whose properties block carries `value` as its prefix-filter entry.
fn with_prefix_entry(value: &[u8]) -> Vec<u8> {
    let mut builder = TableBuilder::new(options(Compression::None), bytewise, whole);
    builder.add(b"key", b"value").expect("added");
    builder.prefix_entry_for_tests(value);
    builder.finish().expect("finished")
}

#[test]
fn a_damaged_prefix_filter_entry_is_refused_by_name() {
    for (value, reason) in [
        (
            &[0_u8; 3][..],
            "prefix filter entry is not a handle and a length",
        ),
        (&[0_u8; 20][..], "prefix length out of range"),
    ] {
        let refused = open(with_prefix_entry(value));
        assert!(
            matches!(
                &refused,
                Err(Error::Malformed { what: "table properties", reason: why }) if *why == reason
            ),
            "{:?}",
            refused.err()
        );
    }
}

#[test]
fn the_upper_end_check_agrees_with_the_successor_it_does_not_build() {
    // The successor written out: last byte below 0xff incremented, the rest dropped.
    fn successor(prefix: &[u8]) -> Option<Vec<u8>> {
        let last = prefix.iter().rposition(|&byte| byte != u8::MAX)?;
        let mut end = prefix.get(..=last)?.to_vec();
        *end.last_mut()? += 1;
        Some(end)
    }
    let bytes = [0_u8, 1, 0xfe, 0xff];
    let mut strings = vec![Vec::new()];
    for length in 1..=3 {
        let previous: Vec<Vec<u8>> = strings
            .iter()
            .filter(|s| s.len() == length - 1)
            .cloned()
            .collect();
        for prefix in previous {
            for byte in bytes {
                let mut next = prefix.clone();
                next.push(byte);
                strings.push(next);
            }
        }
    }
    for prefix in strings.iter().filter(|s| !s.is_empty()) {
        for high in &strings {
            let expected = successor(prefix).is_some_and(|end| high.as_slice() <= end.as_slice());
            assert_eq!(
                super::prefix::at_most_successor_for_tests(high, prefix),
                expected,
                "high {high:?} prefix {prefix:?}"
            );
        }
    }
}
