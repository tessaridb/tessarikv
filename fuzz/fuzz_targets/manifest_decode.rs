//! A manifest edit of arbitrary bytes decodes to an edit or an error, and so does its store-format prefix.
#![no_main]

use libfuzzer_sys::fuzz_target;
use tessarikv_format::{ManifestEdit, peek_store_format};

fuzz_target!(|bytes: &[u8]| {
    let _ = ManifestEdit::decode(bytes);
    let _ = peek_store_format(bytes);
});
