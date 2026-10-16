//! A table file of arbitrary bytes: opening, walking both ways and reading the range deletes
//! end in an error or entries, never a panic.
#![no_main]

use std::cmp::Ordering;

use libfuzzer_sys::fuzz_target;
use tessarikv_format::Table;

fn bytewise(left: &[u8], right: &[u8]) -> Ordering {
    left.cmp(right)
}

fn whole(key: &[u8]) -> &[u8] {
    key
}

fuzz_target!(|bytes: &[u8]| {
    let Ok(table) = Table::open(bytes.to_vec(), bytewise, whole) else {
        return;
    };
    let mut walk = table.iter();
    walk.seek_to_first();
    while walk.valid() {
        walk.next();
    }
    let _ = walk.status();
    walk.seek_to_last();
    let _ = table.range_deletes();
});
